//! Versioned local IPC contract.
//!
//! The daemon and every client (the CLI, the desktop UI, and optional
//! integrations) speak one newline delimited JSON protocol over an owner only
//! Unix socket. That protocol is versioned so a mismatch fails loudly with an
//! actionable error instead of silently misbehaving.
//!
//! Compatibility rule: the major version must match exactly, and a peer may
//! speak a minor version lower than or equal to this build's. A peer that
//! speaks a newer minor version is rejected, because this build cannot know
//! about fields or messages it does not ship.
//!
//! Every request carries an id and every response echoes it, so a client can
//! match replies to requests and detect a desynchronized stream.

use std::fmt;
use std::path::PathBuf;

use serde::{Deserialize, Serialize};

use crate::{CapabilityReport, ManagementMode};

/// Major protocol version. Breaking changes bump this.
pub const PROTOCOL_MAJOR: u16 = 1;

/// Minor protocol version. Backward compatible additions bump this.
pub const PROTOCOL_MINOR: u16 = 0;

/// The largest single message, in bytes. Oversized messages are rejected.
pub const MAX_MESSAGE_BYTES: usize = 1024 * 1024;

/// A rejected protocol version negotiation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum ProtocolError {
    /// The peer speaks a different major version.
    #[error(
        "incompatible IPC major version: peer speaks {peer_major}, this build speaks {local_major}"
    )]
    MajorMismatch {
        /// Major version reported by the peer.
        peer_major: u16,
        /// Major version this build speaks.
        local_major: u16,
    },
    /// The peer speaks a newer minor version than this build understands.
    #[error("peer IPC minor version {peer_minor} is newer than this build's {local_minor}")]
    PeerTooNew {
        /// Minor version reported by the peer.
        peer_minor: u16,
        /// Minor version this build speaks.
        local_minor: u16,
    },
}

/// Check whether a peer speaking `peer_major`/`peer_minor` may talk to this
/// build.
pub fn check_protocol(peer_major: u16, peer_minor: u16) -> Result<(), ProtocolError> {
    if peer_major != PROTOCOL_MAJOR {
        return Err(ProtocolError::MajorMismatch {
            peer_major,
            local_major: PROTOCOL_MAJOR,
        });
    }
    if peer_minor > PROTOCOL_MINOR {
        return Err(ProtocolError::PeerTooNew {
            peer_minor,
            local_minor: PROTOCOL_MINOR,
        });
    }
    Ok(())
}

/// A framing failure.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum IpcError {
    /// The message exceeds [`MAX_MESSAGE_BYTES`].
    #[error("message is {len} bytes, over the {MAX_MESSAGE_BYTES} byte limit")]
    TooLarge {
        /// The observed size in bytes.
        len: usize,
    },
    /// The message could not be serialized.
    #[error("cannot encode message: {0}")]
    Encode(String),
    /// The message could not be parsed.
    #[error("cannot decode message: {0}")]
    Decode(String),
}

/// The opening message from a client.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Hello {
    /// The client's major protocol version.
    pub major: u16,
    /// The client's minor protocol version.
    pub minor: u16,
    /// A short client name for diagnostics.
    pub client: String,
}

/// What a client is asking for.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum RequestBody {
    /// The mandatory first message.
    Hello(Hello),
    /// The current status snapshot.
    Status,
    /// The current capability report.
    Capabilities,
    /// Start streaming status events.
    Subscribe,
}

/// A client message with its request id.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClientMessage {
    /// The request id, echoed by the matching response.
    pub id: u64,
    /// The request.
    #[serde(flatten)]
    pub body: RequestBody,
}

/// A status snapshot.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct StatusPayload {
    /// The current management mode.
    pub mode: ManagementMode,
    /// The daemon version.
    pub version: String,
    /// Monotonic observation time in milliseconds.
    pub observed_at_ms: u64,
    /// Available memory in bytes, when known.
    pub available_bytes: Option<u64>,
    /// Total memory in bytes, when known.
    pub total_bytes: Option<u64>,
    /// How many workloads are currently known.
    pub workloads: usize,
    /// What the daemon can do on this host.
    pub capabilities: CapabilityReport,
}

/// A streamed event.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EventPayload {
    /// The event kind, currently always `status`.
    pub kind: String,
    /// Monotonic observation time in milliseconds.
    pub observed_at_ms: u64,
    /// Available memory in bytes, when known.
    pub available_bytes: Option<u64>,
}

/// A machine readable error code.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ErrorCode {
    /// The peer's protocol version is not supported.
    IncompatibleVersion,
    /// The request type is not understood.
    UnknownRequest,
    /// The request was understood but malformed.
    BadRequest,
    /// The daemon cannot serve the request right now.
    Unavailable,
    /// An internal failure.
    Internal,
}

/// An actionable error returned to a client.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiError {
    /// A stable code.
    pub code: ErrorCode,
    /// A human readable explanation.
    pub message: String,
}

impl ApiError {
    /// Build an error.
    pub fn new(code: ErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.code, self.message)
    }
}

impl std::error::Error for ApiError {}

/// What the daemon replies with.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum ResponseBody {
    /// The handshake reply.
    Hello {
        /// The daemon's major protocol version.
        major: u16,
        /// The daemon's minor protocol version.
        minor: u16,
        /// The daemon version.
        server: String,
    },
    /// A status snapshot.
    Status(Box<StatusPayload>),
    /// A capability report.
    Capabilities(CapabilityReport),
    /// The subscription was accepted.
    Subscribed {
        /// The interval between status events, in milliseconds.
        interval_ms: u64,
    },
    /// A streamed event.
    Event(EventPayload),
    /// An error.
    Error(ApiError),
}

/// A server message with the id of the request it answers.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ServerMessage {
    /// The id of the request this answers, or 0 for an unsolicited event.
    pub id: u64,
    /// The response.
    #[serde(flatten)]
    pub body: ResponseBody,
}

/// Encode a message to a single line, without the trailing newline.
pub fn encode<T: Serialize>(message: &T) -> Result<String, IpcError> {
    let text = serde_json::to_string(message).map_err(|err| IpcError::Encode(err.to_string()))?;
    if text.len() > MAX_MESSAGE_BYTES {
        return Err(IpcError::TooLarge { len: text.len() });
    }
    Ok(text)
}

/// Decode a single line into a message.
pub fn decode<T: for<'de> Deserialize<'de>>(line: &str) -> Result<T, IpcError> {
    let trimmed = line.trim_end_matches(['\n', '\r']);
    if trimmed.len() > MAX_MESSAGE_BYTES {
        return Err(IpcError::TooLarge { len: trimmed.len() });
    }
    serde_json::from_str(trimmed).map_err(|err| IpcError::Decode(err.to_string()))
}

/// The default socket path: under `XDG_RUNTIME_DIR` when set, which is owner
/// only per user, otherwise a socket in the temp directory.
pub fn default_socket_path() -> PathBuf {
    match std::env::var_os("XDG_RUNTIME_DIR") {
        Some(dir) if !dir.is_empty() => PathBuf::from(dir).join("breadram.sock"),
        _ => std::env::temp_dir().join("breadram.sock"),
    }
}

/// An async client for the daemon protocol.
pub mod client {
    use super::{
        check_protocol, decode, encode, ApiError, ClientMessage, EventPayload, Hello, IpcError,
        RequestBody, ResponseBody, ServerMessage, StatusPayload, MAX_MESSAGE_BYTES, PROTOCOL_MAJOR,
        PROTOCOL_MINOR,
    };
    use crate::CapabilityReport;
    use std::path::Path;
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio::net::unix::{OwnedReadHalf, OwnedWriteHalf};
    use tokio::net::UnixStream;

    /// Why a client call failed.
    #[derive(Debug, thiserror::Error)]
    pub enum ClientError {
        /// The daemon is not reachable.
        #[error("breadramd is not running or the socket at {path} is unavailable: {source}")]
        Unavailable {
            /// The socket path that was tried.
            path: String,
            /// The underlying connect error.
            #[source]
            source: std::io::Error,
        },
        /// The daemon returned an error.
        #[error("daemon error: {0}")]
        Api(ApiError),
        /// The daemon closed the connection.
        #[error("the daemon closed the connection")]
        Closed,
        /// The daemon speaks an incompatible protocol version.
        #[error("incompatible protocol version: {0}")]
        Version(#[from] super::ProtocolError),
        /// A framing error.
        #[error(transparent)]
        Frame(#[from] IpcError),
        /// An I/O error.
        #[error("io error: {0}")]
        Io(#[from] std::io::Error),
        /// The daemon sent a response this client did not expect.
        #[error("unexpected response from the daemon: {0}")]
        Unexpected(String),
    }

    /// A connected client.
    #[derive(Debug)]
    pub struct Client {
        reader: BufReader<OwnedReadHalf>,
        writer: OwnedWriteHalf,
        next_id: u64,
    }

    impl Client {
        /// Connect to a daemon socket.
        pub async fn connect(path: &Path) -> Result<Self, ClientError> {
            let stream =
                UnixStream::connect(path)
                    .await
                    .map_err(|source| ClientError::Unavailable {
                        path: path.display().to_string(),
                        source,
                    })?;
            let (read, write) = stream.into_split();
            Ok(Self {
                reader: BufReader::new(read),
                writer: write,
                next_id: 1,
            })
        }

        /// Perform the version handshake. Returns the daemon version.
        pub async fn handshake(&mut self, client_name: &str) -> Result<String, ClientError> {
            let body = RequestBody::Hello(Hello {
                major: PROTOCOL_MAJOR,
                minor: PROTOCOL_MINOR,
                client: client_name.to_string(),
            });
            match self.call(body).await? {
                ResponseBody::Hello { server, major, .. } => {
                    check_protocol(major, PROTOCOL_MINOR).map_err(ClientError::Version)?;
                    Ok(server)
                }
                ResponseBody::Error(err) => Err(ClientError::Api(err)),
                other => Err(ClientError::Unexpected(format!("{other:?}"))),
            }
        }

        /// Request the current status.
        pub async fn status(&mut self) -> Result<StatusPayload, ClientError> {
            match self.call(RequestBody::Status).await? {
                ResponseBody::Status(status) => Ok(*status),
                ResponseBody::Error(err) => Err(ClientError::Api(err)),
                other => Err(ClientError::Unexpected(format!("{other:?}"))),
            }
        }

        /// Request the capability report.
        pub async fn capabilities(&mut self) -> Result<CapabilityReport, ClientError> {
            match self.call(RequestBody::Capabilities).await? {
                ResponseBody::Capabilities(report) => Ok(report),
                ResponseBody::Error(err) => Err(ClientError::Api(err)),
                other => Err(ClientError::Unexpected(format!("{other:?}"))),
            }
        }

        /// Subscribe to status events.
        pub async fn subscribe(&mut self) -> Result<u64, ClientError> {
            match self.call(RequestBody::Subscribe).await? {
                ResponseBody::Subscribed { interval_ms } => Ok(interval_ms),
                ResponseBody::Error(err) => Err(ClientError::Api(err)),
                other => Err(ClientError::Unexpected(format!("{other:?}"))),
            }
        }

        /// Read the next event from a subscription.
        pub async fn next_event(&mut self) -> Result<EventPayload, ClientError> {
            match self.recv().await?.body {
                ResponseBody::Event(event) => Ok(event),
                ResponseBody::Error(err) => Err(ClientError::Api(err)),
                other => Err(ClientError::Unexpected(format!("{other:?}"))),
            }
        }

        async fn call(&mut self, body: RequestBody) -> Result<ResponseBody, ClientError> {
            let id = self.next_id;
            self.next_id += 1;
            self.send(&ClientMessage { id, body }).await?;
            let response = self.recv().await?;
            if response.id != id {
                return Err(ClientError::Unexpected(format!(
                    "response id {} does not match request id {id}",
                    response.id
                )));
            }
            Ok(response.body)
        }

        async fn send(&mut self, message: &ClientMessage) -> Result<(), ClientError> {
            let line = encode(message)?;
            self.writer.write_all(line.as_bytes()).await?;
            self.writer.write_all(b"\n").await?;
            self.writer.flush().await?;
            Ok(())
        }

        async fn recv(&mut self) -> Result<ServerMessage, ClientError> {
            let mut line = String::new();
            let read = self.reader.read_line(&mut line).await?;
            if read == 0 {
                return Err(ClientError::Closed);
            }
            if line.len() > MAX_MESSAGE_BYTES {
                return Err(ClientError::Frame(IpcError::TooLarge { len: line.len() }));
            }
            Ok(decode(&line)?)
        }
    }
}

pub use client::{Client, ClientError};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn exact_version_is_accepted() {
        assert_eq!(check_protocol(PROTOCOL_MAJOR, PROTOCOL_MINOR), Ok(()));
    }

    #[test]
    fn older_minor_is_accepted() {
        assert_eq!(check_protocol(PROTOCOL_MAJOR, 0), Ok(()));
    }

    #[test]
    fn different_major_is_rejected() {
        let err = check_protocol(PROTOCOL_MAJOR + 1, PROTOCOL_MINOR).unwrap_err();
        assert!(matches!(err, ProtocolError::MajorMismatch { .. }));
    }

    #[test]
    fn newer_minor_is_rejected() {
        let err = check_protocol(PROTOCOL_MAJOR, PROTOCOL_MINOR + 1).unwrap_err();
        assert!(matches!(err, ProtocolError::PeerTooNew { .. }));
    }

    #[test]
    fn a_wrong_major_beats_a_wrong_minor() {
        let err = check_protocol(PROTOCOL_MAJOR + 1, PROTOCOL_MINOR + 5).unwrap_err();
        assert!(matches!(err, ProtocolError::MajorMismatch { .. }));
    }

    #[test]
    fn messages_round_trip_as_json_lines() {
        let message = ClientMessage {
            id: 7,
            body: RequestBody::Hello(Hello {
                major: PROTOCOL_MAJOR,
                minor: PROTOCOL_MINOR,
                client: "cli".to_string(),
            }),
        };
        let line = encode(&message).unwrap();
        assert!(!line.contains('\n'));
        let decoded: ClientMessage = decode(&line).unwrap();
        assert_eq!(decoded, message);
    }

    #[test]
    fn a_response_round_trips() {
        let message = ServerMessage {
            id: 3,
            body: ResponseBody::Error(ApiError::new(ErrorCode::UnknownRequest, "nope")),
        };
        let line = encode(&message).unwrap();
        let decoded: ServerMessage = decode(&line).unwrap();
        assert_eq!(decoded, message);
    }

    #[test]
    fn an_oversized_message_is_rejected() {
        let big = "x".repeat(MAX_MESSAGE_BYTES + 1);
        assert!(matches!(
            decode::<ClientMessage>(&big),
            Err(IpcError::TooLarge { .. })
        ));
    }

    #[test]
    fn malformed_json_is_a_decode_error() {
        assert!(matches!(
            decode::<ClientMessage>("not json"),
            Err(IpcError::Decode(_))
        ));
    }
}
