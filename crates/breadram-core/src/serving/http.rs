//! Minimal HTTP/1.x request-head parsing for the gateway, with hard bounds.
//!
//! The gateway passes a body through and never inspects it, so only the request
//! line and headers are read here, which is all the routing metadata needs. The
//! parser is pure and bounded: an oversized or malformed head is refused rather
//! than buffered indefinitely.

/// The largest head the parser will accept, in bytes.
pub const MAX_HEADER_BYTES: usize = 64 * 1024;
/// The most headers the parser will accept.
pub const MAX_HEADERS: usize = 128;

/// A parsed request line and headers, with no body.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestHead {
    /// The method, upper-cased for comparison.
    pub method: String,
    /// The request target, as sent.
    pub target: String,
    /// Header name/value pairs, names lower-cased, in order.
    pub headers: Vec<(String, String)>,
    /// Whether the body uses chunked transfer encoding.
    pub chunked: bool,
    /// The declared body length, when there is one.
    pub content_length: Option<u64>,
    /// How many bytes the head occupies; the body begins here.
    pub body_offset: usize,
}

impl RequestHead {
    /// The first value for a header name, which is compared case-insensitively.
    pub fn header(&self, name: &str) -> Option<&str> {
        let name = name.to_ascii_lowercase();
        self.headers
            .iter()
            .find(|(key, _)| *key == name)
            .map(|(_, value)| value.as_str())
    }

    /// Whether the client asked for a server-sent-events stream.
    pub fn wants_stream(&self) -> bool {
        self.header("accept").is_some_and(|value| {
            value
                .to_ascii_lowercase()
                .contains(super::gateway::EVENT_STREAM)
        })
    }
}

/// Why a head could not be parsed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ParseError {
    /// The head is not complete yet; read more.
    Incomplete,
    /// The head exceeded the bound.
    TooLarge,
    /// The head was complete but not valid HTTP/1.x.
    Malformed,
}

/// Parse a request head from a buffer.
///
/// Returns [`ParseError::Incomplete`] when the terminating blank line is not yet
/// present, so the caller can read more without guessing.
pub fn parse_head(buf: &[u8]) -> Result<RequestHead, ParseError> {
    let end = find_head_end(buf);
    let head_end = match end {
        Some(index) => index,
        None if buf.len() > MAX_HEADER_BYTES => return Err(ParseError::TooLarge),
        None => return Err(ParseError::Incomplete),
    };
    if head_end > MAX_HEADER_BYTES {
        return Err(ParseError::TooLarge);
    }
    let text = std::str::from_utf8(&buf[..head_end]).map_err(|_| ParseError::Malformed)?;
    let mut lines = text.split("\r\n");
    let request_line = lines.next().ok_or(ParseError::Malformed)?;
    let mut parts = request_line.split(' ');
    let method = parts.next().filter(|s| !s.is_empty());
    let target = parts.next().filter(|s| !s.is_empty());
    let version = parts.next().filter(|s| !s.is_empty());
    let (Some(method), Some(target), Some(version)) = (method, target, version) else {
        return Err(ParseError::Malformed);
    };
    if parts.next().is_some() || !version.starts_with("HTTP/1.") {
        return Err(ParseError::Malformed);
    }

    let mut headers = Vec::new();
    let mut chunked = false;
    let mut content_length = None;
    for line in lines {
        if line.is_empty() {
            continue;
        }
        let (name, value) = line.split_once(':').ok_or(ParseError::Malformed)?;
        let name = name.trim().to_ascii_lowercase();
        if name.is_empty() {
            return Err(ParseError::Malformed);
        }
        let value = value.trim().to_string();
        if name == "transfer-encoding" && value.to_ascii_lowercase().contains("chunked") {
            chunked = true;
        }
        if name == "content-length" {
            content_length = Some(value.parse::<u64>().map_err(|_| ParseError::Malformed)?);
        }
        headers.push((name, value));
        if headers.len() > MAX_HEADERS {
            return Err(ParseError::TooLarge);
        }
    }

    Ok(RequestHead {
        method: method.to_ascii_uppercase(),
        target: target.to_string(),
        headers,
        chunked,
        content_length,
        // The blank line's CRLF is part of the head; the body follows it.
        body_offset: head_end + 4,
    })
}

/// The index of the blank line that ends the head.
fn find_head_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|window| window == b"\r\n\r\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_complete_head_parses_with_method_target_and_case_insensitive_headers() {
        let raw = b"POST /v1/chat/completions HTTP/1.1\r\nHost: x\r\nAccept: text/event-stream\r\nIdempotency-Key: k1\r\n\r\n";
        let head = parse_head(raw).unwrap();
        assert_eq!(head.method, "POST");
        assert_eq!(head.target, "/v1/chat/completions");
        assert_eq!(head.header("accept"), Some("text/event-stream"));
        assert_eq!(head.header("idempotency-key"), Some("k1"));
        assert!(head.wants_stream());
        assert!(!head.chunked);
        assert_eq!(head.body_offset, raw.len());
    }

    #[test]
    fn an_incomplete_head_asks_for_more_and_an_oversized_one_is_refused() {
        assert_eq!(
            parse_head(b"GET / HTTP/1.1\r\nHost: x\r\n"),
            Err(ParseError::Incomplete)
        );
        let big = vec![b'a'; MAX_HEADER_BYTES + 1];
        assert_eq!(parse_head(&big), Err(ParseError::TooLarge));
    }

    #[test]
    fn malformed_and_bounded_heads_are_refused() {
        assert_eq!(parse_head(b"GET\r\n\r\n"), Err(ParseError::Malformed));
        assert_eq!(
            parse_head(b"GET / HTTP/2.0\r\n\r\n"),
            Err(ParseError::Malformed)
        );
        assert_eq!(
            parse_head(b"GET / HTTP/1.1\r\nContent-Length: nope\r\n\r\n"),
            Err(ParseError::Malformed)
        );
        // Chunked and content length are read, not guessed.
        let head = parse_head(b"POST / HTTP/1.1\r\nTransfer-Encoding: chunked\r\n\r\n").unwrap();
        assert!(head.chunked);
        assert_eq!(head.content_length, None);
        let head = parse_head(b"POST / HTTP/1.1\r\nContent-Length: 12\r\n\r\n").unwrap();
        assert_eq!(head.content_length, Some(12));
    }
}
