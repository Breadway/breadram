//! The real transport: bounded HTTP to a loopback endpoint.
//!
//! Adapters describe probes and operations; this is the thing that puts them on
//! the wire. It is deliberately narrow:
//!
//! - **Loopback `http` only.** A remote endpoint needs transport security, and
//!   this build does not ship a TLS stack, so `https` and non-loopback hosts are
//!   refused with a reason rather than silently sent in the clear. A `unix:`
//!   endpoint is refused too: the hook protocol is a process protocol, not an
//!   HTTP one.
//! - **Bounded.** One connection per request, a connect timeout, a read and write
//!   timeout, `Connection: close`, and a cap on how much of a response is read. A
//!   provider that never answers cannot hold the daemon open.
//! - **Quiet about credentials.** A request's `Debug` is already redacted, and
//!   this module never puts a header value into an error message, so a token
//!   cannot reach a log line through a failure.
//!
//! Everything here is blocking and the caller owns the thread, which is why the
//! daemon runs its monitor passes on a blocking worker.

use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream, ToSocketAddrs};
use std::time::Duration;

use super::probe::{Auth, OperationTransport, ProbeRequest, Response, Transport, TransportError};
use super::OperationRequest;

/// The default cap on a response body, matching the probe client's limit.
pub const MAX_RESPONSE_BYTES: usize = 1024 * 1024;
/// The default time allowed to establish a connection.
pub const DEFAULT_CONNECT_TIMEOUT_MS: u64 = 1_000;

/// A bounded, loopback-only HTTP transport.
#[derive(Debug, Clone, Copy)]
pub struct HttpTransport {
    connect_timeout_ms: u64,
    max_response_bytes: usize,
}

impl Default for HttpTransport {
    fn default() -> Self {
        Self::new()
    }
}

impl HttpTransport {
    /// A transport with the default bounds.
    pub fn new() -> Self {
        Self {
            connect_timeout_ms: DEFAULT_CONNECT_TIMEOUT_MS,
            max_response_bytes: MAX_RESPONSE_BYTES,
        }
    }

    /// Change the time allowed to establish a connection.
    pub fn with_connect_timeout(mut self, connect_timeout_ms: u64) -> Self {
        self.connect_timeout_ms = connect_timeout_ms.max(1);
        self
    }

    /// Change the cap on a response body.
    pub fn with_max_response_bytes(mut self, max_response_bytes: usize) -> Self {
        self.max_response_bytes = max_response_bytes.max(1);
        self
    }

    /// Perform one request, read-only or not.
    ///
    /// The method and body come from the caller: a probe can only be a `GET`, and
    /// only an adapter operation can be a write.
    fn request(
        &self,
        method: &str,
        url: &str,
        auth: &Auth,
        body: Option<&str>,
        timeout_ms: u64,
    ) -> Result<Response, TransportError> {
        let target = Target::parse(url)?;
        let timeout = Duration::from_millis(timeout_ms.max(1));
        let addresses: Vec<SocketAddr> = (target.host.as_str(), target.port)
            .to_socket_addrs()
            .map_err(|err| TransportError::Failure(format!("{url} does not resolve: {err}")))?
            .collect();
        if addresses.is_empty() {
            return Err(TransportError::Failure(format!(
                "{url} did not resolve to an address"
            )));
        }

        // Every resolved address is tried in turn. `localhost` commonly resolves
        // to both `::1` and `127.0.0.1`, and a provider listens on one of them, so
        // taking only the first would fail against a working server.
        let connect_timeout = Duration::from_millis(self.connect_timeout_ms.min(timeout_ms.max(1)));
        let mut stream = None;
        let mut failure = None;
        for address in &addresses {
            match TcpStream::connect_timeout(address, connect_timeout) {
                Ok(connected) => {
                    stream = Some(connected);
                    break;
                }
                Err(err) => failure = Some(format!("cannot connect to {address}: {err}")),
            }
        }
        let mut stream = stream.ok_or_else(|| {
            TransportError::Failure(failure.unwrap_or_else(|| format!("cannot connect to {url}")))
        })?;
        stream
            .set_read_timeout(Some(timeout))
            .map_err(|err| TransportError::Failure(err.to_string()))?;
        stream
            .set_write_timeout(Some(timeout))
            .map_err(|err| TransportError::Failure(err.to_string()))?;

        let body = body.unwrap_or("");
        let mut head = format!(
            "{method} {} HTTP/1.1\r\nHost: {}:{}\r\nAccept: application/json\r\nConnection: close\r\n",
            target.path, target.host, target.port
        );
        match auth {
            Auth::None => {}
            Auth::Bearer(secret) => {
                head.push_str("Authorization: Bearer ");
                head.push_str(secret.expose());
                head.push_str("\r\n");
            }
            Auth::Header { name, value } => {
                head.push_str(name);
                head.push_str(": ");
                head.push_str(value.expose());
                head.push_str("\r\n");
            }
        }
        if !body.is_empty() {
            head.push_str("Content-Type: application/json\r\n");
        }
        head.push_str(&format!("Content-Length: {}\r\n\r\n", body.len()));

        stream
            .write_all(head.as_bytes())
            .and_then(|()| stream.write_all(body.as_bytes()))
            .and_then(|()| stream.flush())
            .map_err(|err| map_io(&err, url, timeout_ms))?;

        let raw = read_bounded(&mut stream, self.max_response_bytes, url, timeout_ms)?;
        parse_response(&raw)
    }
}

impl Transport for HttpTransport {
    fn send(&self, request: &ProbeRequest, timeout_ms: u64) -> Result<Response, TransportError> {
        self.request(
            request.method.as_str(),
            &request.url,
            &request.auth,
            None,
            timeout_ms,
        )
    }
}

impl OperationTransport for HttpTransport {
    /// Send one operation. This is the only path that can write.
    fn send_operation(
        &self,
        request: &OperationRequest,
        timeout_ms: u64,
    ) -> Result<Response, TransportError> {
        self.request(
            request.method.as_str(),
            &request.url,
            &request.auth,
            request.body.as_deref(),
            timeout_ms,
        )
    }
}

/// Why an endpoint cannot be used by this transport.
fn unsupported(url: &str) -> TransportError {
    if url.starts_with("unix:") {
        return TransportError::Failure(format!(
            "{url} is a unix socket endpoint, which this transport does not speak"
        ));
    }
    if url.starts_with("https://") {
        return TransportError::Failure(format!(
            "{url} needs TLS, and this build ships no TLS stack: configure the provider over loopback http"
        ));
    }
    TransportError::Failure(format!(
        "{url} is not a loopback http endpoint, and this build will not send a request to a remote host in the clear"
    ))
}

/// A parsed loopback endpoint.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Target {
    host: String,
    port: u16,
    path: String,
}

impl Target {
    /// Parse a URL, refusing anything this transport cannot send safely.
    ///
    /// A host that is not loopback is refused, including a public name that
    /// happens to resolve to a loopback address: the refusal is about what was
    /// configured, not about where it points today.
    fn parse(url: &str) -> Result<Self, TransportError> {
        let Some(rest) = url.strip_prefix("http://") else {
            return Err(unsupported(url));
        };
        let (authority, path) = match rest.find(['/', '?']) {
            Some(index) => (&rest[..index], &rest[index..]),
            None => (rest, "/"),
        };
        if authority.contains('@') {
            return Err(TransportError::Failure(format!(
                "{url} embeds credentials in the URL, which is not allowed"
            )));
        }
        let (host, port) = match authority.rsplit_once(':') {
            Some((host, port)) => {
                let port: u16 = port.parse().map_err(|_| {
                    TransportError::Failure(format!("{url} has an invalid port {port:?}"))
                })?;
                (host.to_string(), port)
            }
            None => (authority.to_string(), 80),
        };
        let host = host
            .trim_start_matches('[')
            .trim_end_matches(']')
            .to_string();
        if !is_loopback_host(&host) {
            return Err(unsupported(url));
        }
        Ok(Self {
            host,
            port,
            path: path.to_string(),
        })
    }
}

/// Whether a host name is loopback, by name or by address.
fn is_loopback_host(host: &str) -> bool {
    if host.eq_ignore_ascii_case("localhost") {
        return true;
    }
    match host.parse::<std::net::IpAddr>() {
        Ok(address) => address.is_loopback(),
        Err(_) => false,
    }
}

/// Read at most `limit` bytes, reporting a timeout as such.
fn read_bounded(
    stream: &mut TcpStream,
    limit: usize,
    url: &str,
    timeout_ms: u64,
) -> Result<Vec<u8>, TransportError> {
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 8 * 1024];
    loop {
        match stream.read(&mut chunk) {
            Ok(0) => return Ok(buffer),
            Ok(read) => {
                if buffer.len() + read > limit {
                    return Err(TransportError::Failure(format!(
                        "the response from {url} exceeded {limit} bytes"
                    )));
                }
                buffer.extend_from_slice(&chunk[..read]);
            }
            Err(err) => return Err(map_io(&err, url, timeout_ms)),
        }
    }
}

/// Turn an I/O error into a transport error, saying nothing about the request.
fn map_io(err: &std::io::Error, url: &str, timeout_ms: u64) -> TransportError {
    match err.kind() {
        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut => {
            TransportError::Timeout(timeout_ms)
        }
        _ => TransportError::Failure(format!("{url}: {err}")),
    }
}

/// Parse an HTTP/1.1 response, honouring `Content-Length` and chunked encoding.
fn parse_response(raw: &[u8]) -> Result<Response, TransportError> {
    let split = raw
        .windows(4)
        .position(|window| window == b"\r\n\r\n")
        .ok_or_else(|| {
            TransportError::Failure("the response ended before its headers were complete".into())
        })?;
    let head = std::str::from_utf8(&raw[..split])
        .map_err(|_| TransportError::Failure("the response headers are not valid UTF-8".into()))?;
    let body = &raw[split + 4..];

    let mut lines = head.split("\r\n");
    let status_line = lines
        .next()
        .ok_or_else(|| TransportError::Failure("the response has no status line".into()))?;
    let status = status_line
        .split_whitespace()
        .nth(1)
        .and_then(|code| code.parse::<u16>().ok())
        .ok_or_else(|| {
            TransportError::Failure(format!("cannot read a status code from {status_line:?}"))
        })?;

    let mut content_length = None;
    let mut chunked = false;
    for line in lines {
        let Some((name, value)) = line.split_once(':') else {
            continue;
        };
        let name = name.trim().to_ascii_lowercase();
        let value = value.trim();
        match name.as_str() {
            "content-length" => content_length = value.parse::<usize>().ok(),
            "transfer-encoding" if value.to_ascii_lowercase().contains("chunked") => chunked = true,
            _ => {}
        }
    }

    let decoded = if chunked {
        decode_chunked(body)?
    } else if let Some(length) = content_length {
        body.get(..length)
            .ok_or_else(|| {
                TransportError::Failure(format!(
                    "the response promised {length} bytes but sent {}",
                    body.len()
                ))
            })?
            .to_vec()
    } else {
        body.to_vec()
    };

    Ok(Response::new(
        status,
        String::from_utf8_lossy(&decoded).into_owned(),
    ))
}

/// Decode a chunked body, which is what a streaming server sends.
fn decode_chunked(body: &[u8]) -> Result<Vec<u8>, TransportError> {
    let mut decoded = Vec::new();
    let mut rest = body;
    loop {
        let Some(end) = rest.windows(2).position(|window| window == b"\r\n") else {
            return Err(TransportError::Failure(
                "a chunked response ended mid chunk header".into(),
            ));
        };
        let size_line = std::str::from_utf8(&rest[..end])
            .map_err(|_| TransportError::Failure("a chunk size is not valid UTF-8".into()))?;
        let size = usize::from_str_radix(size_line.trim().split(';').next().unwrap_or(""), 16)
            .map_err(|_| {
                TransportError::Failure(format!("cannot read a chunk size from {size_line:?}"))
            })?;
        let start = end + 2;
        if size == 0 {
            return Ok(decoded);
        }
        let end_of_chunk = start + size;
        if end_of_chunk > rest.len() {
            return Err(TransportError::Failure(
                "a chunked response ended inside a chunk".into(),
            ));
        }
        decoded.extend_from_slice(&rest[start..end_of_chunk]);
        rest = &rest[(end_of_chunk + 2).min(rest.len())..];
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{BufRead, BufReader};
    use std::net::TcpListener;
    use std::sync::mpsc;
    use std::sync::{Arc, Mutex};

    /// One recorded request, for an assertion.
    #[derive(Clone, Debug, Default)]
    struct Recorded {
        request_line: String,
        headers: Vec<String>,
        body: String,
    }

    impl Recorded {
        /// Whether a header with this name is present, whatever its value.
        fn has_header(&self, name: &str) -> bool {
            self.headers.iter().any(|header| {
                header
                    .split_once(':')
                    .is_some_and(|(head, _)| head.eq_ignore_ascii_case(name))
            })
        }
    }

    /// How a fake server answers.
    enum Answer {
        /// A complete response, written verbatim.
        Raw(Vec<u8>),
        /// Accept the connection, then never answer.
        Silent,
        /// Accept the connection and close it without a response.
        Close,
    }

    /// Start a fake provider. Returns its address and everything it received.
    fn serve(answers: Vec<Answer>) -> (SocketAddr, Arc<Mutex<Vec<Recorded>>>, mpsc::Receiver<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("binds a loopback port");
        let address = listener.local_addr().expect("has an address");
        let recorded = Arc::new(Mutex::new(Vec::new()));
        let (done, finished) = mpsc::channel();
        let seen = Arc::clone(&recorded);
        std::thread::spawn(move || {
            for answer in answers {
                let Ok((stream, _)) = listener.accept() else {
                    break;
                };
                let mut reader = BufReader::new(stream.try_clone().expect("clones the stream"));
                let request = read_request(&mut reader);
                seen.lock().expect("not poisoned").push(request);
                let mut stream = stream;
                match answer {
                    Answer::Raw(bytes) => {
                        let _ = stream.write_all(&bytes);
                    }
                    Answer::Close => {}
                    Answer::Silent => {
                        // Held open well past any timeout a test uses, so the
                        // client sees a stall rather than an end of stream.
                        std::thread::sleep(Duration::from_secs(10));
                    }
                }
                let _ = stream.shutdown(std::net::Shutdown::Both);
                let _ = done.send(());
            }
        });
        (address, recorded, finished)
    }

    fn read_request(reader: &mut BufReader<TcpStream>) -> Recorded {
        let mut recorded = Recorded::default();
        let mut line = String::new();
        if reader.read_line(&mut line).unwrap_or(0) == 0 {
            return recorded;
        }
        recorded.request_line = line.trim_end().to_string();
        let mut length = 0usize;
        loop {
            line.clear();
            if reader.read_line(&mut line).unwrap_or(0) == 0 {
                break;
            }
            let trimmed = line.trim_end().to_string();
            if trimmed.is_empty() {
                break;
            }
            if let Some((name, value)) = trimmed.split_once(':') {
                if name.eq_ignore_ascii_case("content-length") {
                    length = value.trim().parse().unwrap_or(0);
                }
            }
            recorded.headers.push(trimmed);
        }
        if length > 0 {
            let mut body = vec![0u8; length];
            if reader.read_exact(&mut body).is_ok() {
                recorded.body = String::from_utf8_lossy(&body).into_owned();
            }
        }
        recorded
    }

    fn http_response(body: &str) -> Vec<u8> {
        format!(
            "HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{body}",
            body.len()
        )
        .into_bytes()
    }

    fn probe(url: &str) -> Result<Response, TransportError> {
        HttpTransport::new().send(&ProbeRequest::get(url), 2_000)
    }

    #[test]
    fn a_probe_reads_the_status_and_the_body() {
        let (address, recorded, finished) =
            serve(vec![Answer::Raw(http_response(r#"{"status":"ok"}"#))]);
        let url = format!("http://{address}/api/ps");
        let response = probe(&url).expect("the fake server answers");
        assert_eq!(response.status, 200);
        assert_eq!(response.body, r#"{"status":"ok"}"#);
        finished
            .recv_timeout(Duration::from_secs(2))
            .expect("served");

        let request = recorded.lock().expect("not poisoned")[0].clone();
        assert_eq!(request.request_line, "GET /api/ps HTTP/1.1");
        assert!(request.has_header("Host"), "{:?}", request.headers);
        assert!(
            request.headers.iter().any(|h| h == "Connection: close"),
            "{:?}",
            request.headers
        );
        assert!(request.body.is_empty());
    }

    #[test]
    fn an_operation_sends_its_method_body_and_length() {
        let (address, recorded, finished) =
            serve(vec![Answer::Raw(http_response(r#"{"done":true}"#))]);
        let url = format!("http://{address}/api/generate");
        let operation =
            OperationRequest::post(url.clone(), r#"{"model":"llama3:8b","keep_alive":0}"#);
        let response = HttpTransport::new()
            .send_operation(&operation, 2_000)
            .expect("the fake server answers");
        assert_eq!(response.status, 200);
        finished
            .recv_timeout(Duration::from_secs(2))
            .expect("served");

        let request = recorded.lock().expect("not poisoned")[0].clone();
        assert_eq!(request.request_line, "POST /api/generate HTTP/1.1");
        assert_eq!(request.body, r#"{"model":"llama3:8b","keep_alive":0}"#);
        assert!(
            request.headers.iter().any(|header| {
                header.split_once(':').is_some_and(|(name, value)| {
                    name.eq_ignore_ascii_case("content-length")
                        && value.trim() == request.body.len().to_string()
                })
            }),
            "the length must match the body: {:?}",
            request.headers
        );
        assert!(
            request.headers.iter().any(|header| {
                header.split_once(':').is_some_and(|(name, value)| {
                    name.eq_ignore_ascii_case("content-type") && value.trim() == "application/json"
                })
            }),
            "{:?}",
            request.headers
        );
    }

    #[test]
    fn a_credential_is_sent_and_never_rendered() {
        let (address, recorded, finished) = serve(vec![Answer::Raw(http_response("{}"))]);
        let secret = "s3cret-token-value";
        let url = format!("http://{address}/api/ps");
        let request = ProbeRequest::get(&url).with_bearer(secret);

        // The request, and every part of it that renders, stays quiet.
        assert!(!format!("{request:?}").contains(secret));
        assert!(!format!("{:?}", request.auth).contains(secret));

        let response = HttpTransport::new()
            .send(&request, 2_000)
            .expect("the fake server answers");
        assert_eq!(response.status, 200);
        finished
            .recv_timeout(Duration::from_secs(2))
            .expect("served");

        let recorded = recorded.lock().expect("not poisoned")[0].clone();
        assert_eq!(
            recorded
                .headers
                .iter()
                .find(|header| header.to_ascii_lowercase().starts_with("authorization"))
                .map(String::as_str),
            Some(format!("Authorization: Bearer {secret}").as_str()),
            "the secret goes on the wire and nowhere else"
        );
    }

    #[test]
    fn a_failure_message_never_contains_the_token() {
        // Nothing is listening on this port, so the connection is refused.
        let listener = TcpListener::bind("127.0.0.1:0").expect("binds");
        let address = listener.local_addr().expect("has an address");
        drop(listener);

        let secret = "s3cret-token-value";
        let request = ProbeRequest::get(format!("http://{address}/api/ps")).with_bearer(secret);
        let error = HttpTransport::new()
            .send(&request, 500)
            .expect_err("nothing is listening");
        let rendered = format!("{error} {error:?}");
        assert!(!rendered.contains(secret), "{rendered}");
    }

    #[test]
    fn only_a_loopback_http_endpoint_is_accepted() {
        for url in [
            "https://gpu.example.net:8000/api/ps",
            "http://gpu.example.net:8000/api/ps",
            "http://10.0.0.5:8000/api/ps",
            "unix:/run/breadram/hook.sock",
            "http://user:pass@127.0.0.1:8000/api/ps",
        ] {
            let error = probe(url).expect_err("refused before anything is sent");
            assert!(
                matches!(error, TransportError::Failure(_)),
                "{url} gave {error:?}"
            );
            assert!(
                !error.to_string().is_empty(),
                "a refusal has to say why: {url}"
            );
        }

        // And a loopback endpoint is accepted, by name and by address.
        let (address, _recorded, _finished) = serve(vec![Answer::Raw(http_response("{}"))]);
        assert!(probe(&format!("http://{address}/api/ps")).is_ok());
        let (address, _recorded, _finished) = serve(vec![Answer::Raw(http_response("{}"))]);
        assert!(probe(&format!("http://localhost:{}/api/ps", address.port())).is_ok());
    }

    #[test]
    fn a_remote_host_is_refused_even_when_it_resolves_to_loopback() {
        // A name that is not `localhost` is refused whatever it resolves to, so
        // the decision is about the configuration rather than the DNS answer.
        let error = probe("http://127.0.0.1.nip.io:9/api/ps").expect_err("refused");
        assert!(error.to_string().contains("loopback"), "{error}");
    }

    #[test]
    fn content_length_bounds_the_body() {
        let mut raw = "HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\n{}this is not part of the body"
            .to_string()
            .into_bytes();
        raw.truncate(raw.len());
        let (address, _recorded, _finished) = serve(vec![Answer::Raw(raw)]);
        let response = probe(&format!("http://{address}/api/ps")).expect("answers");
        assert_eq!(response.body, "{}");
    }

    #[test]
    fn a_chunked_body_is_decoded() {
        let raw = b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n5\r\n{\"a\":\r\n3\r\n1}\n\r\n0\r\n\r\n"
            .to_vec();
        let (address, _recorded, _finished) = serve(vec![Answer::Raw(raw)]);
        let response = probe(&format!("http://{address}/api/ps")).expect("answers");
        assert_eq!(response.body, "{\"a\":1}\n");
    }

    #[test]
    fn an_oversized_response_is_refused_rather_than_buffered() {
        let (address, _recorded, _finished) =
            serve(vec![Answer::Raw(http_response(&"x".repeat(4096)))]);
        let error = HttpTransport::new()
            .with_max_response_bytes(1024)
            .send(
                &ProbeRequest::get(format!("http://{address}/api/ps")),
                2_000,
            )
            .expect_err("over the cap");
        assert!(error.to_string().contains("1024"), "{error}");
    }

    #[test]
    fn a_server_that_never_answers_times_out() {
        let (address, _recorded, _finished) = serve(vec![Answer::Silent]);
        let error = probe(&format!("http://{address}/api/ps")).expect_err("times out");
        assert_eq!(error, TransportError::Timeout(2_000));
    }

    #[test]
    fn a_server_that_closes_without_answering_is_a_failure_not_a_response() {
        let (address, _recorded, _finished) = serve(vec![Answer::Close]);
        let error = probe(&format!("http://{address}/api/ps")).expect_err("no response");
        assert!(matches!(error, TransportError::Failure(_)), "{error:?}");
    }

    #[test]
    fn a_status_line_that_cannot_be_read_is_refused() {
        let (address, _recorded, _finished) =
            serve(vec![Answer::Raw(b"not http at all\r\n\r\n".to_vec())]);
        let error = probe(&format!("http://{address}/api/ps")).expect_err("no status");
        assert!(error.to_string().contains("status"), "{error}");
    }

    #[test]
    fn a_host_name_is_not_confused_with_a_loopback_address() {
        assert!(is_loopback_host("localhost"));
        assert!(is_loopback_host("127.0.0.1"));
        assert!(is_loopback_host("::1"));
        assert!(!is_loopback_host("127.0.0.1.nip.io"));
        assert!(!is_loopback_host("example.net"));
        assert!(!is_loopback_host("10.0.0.5"));

        let target = Target::parse("http://127.0.0.1:11434/api/ps?x=1").expect("parses");
        assert_eq!(target.port, 11434);
        assert_eq!(target.path, "/api/ps?x=1");
        assert_eq!(Target::parse("http://127.0.0.1").expect("parses").path, "/");
        assert!(Target::parse("http://[::1]:11434/api/ps").is_ok());
    }
}
