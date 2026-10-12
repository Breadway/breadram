//! A fake Ollama service on a loopback port, shared by the core suites.
//!
//! Nothing here needs a model server or a GPU. The fake speaks the documented
//! Ollama API (`GET /api/version`, `GET /api/ps`, `POST /api/generate` with a
//! zero keep-alive as the unload), so the real probe client, transport,
//! adapter, planner, coordinator, and chained executor run against it over a
//! real socket. RAM-29 shared support.

#![allow(dead_code)]

use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

use super::GIB;

/// The model the fake reports.
pub const MODEL: &str = "llama3:8b";
/// The model's bytes: 4 GiB total, 3 GiB of it in device memory.
pub const MODEL_BYTES: u64 = 4 * GIB;
pub const MODEL_VRAM_BYTES: u64 = 3 * GIB;
pub const MODEL_HOST_BYTES: u64 = MODEL_BYTES - MODEL_VRAM_BYTES;
/// 2026-10-11T00:00:00Z, so a reported expiry is read against a fixed instant
/// rather than the clock the test happens to run on.
pub const FIXED_NOW_MS: u64 = 1_791_676_800_000;

/// The provider's state, shared with the serving thread.
#[derive(Default)]
pub struct FakeState {
    /// Whether the model is currently resident.
    pub loaded: Mutex<bool>,
    /// Whether an accepted unload nevertheless leaves the model resident, which
    /// is what a provider that ignores the request looks like.
    pub stubborn: Mutex<bool>,
    /// Every request line the fake saw, in order.
    pub requests: Mutex<Vec<String>>,
    /// Every request body the fake read, in order.
    pub bodies: Mutex<Vec<String>>,
}

/// A fake Ollama service on a loopback port.
pub struct FakeProvider {
    /// The base URL of the fake, for example `http://127.0.0.1:41237`.
    pub endpoint: String,
    pub state: Arc<FakeState>,
    stop: Arc<AtomicBool>,
    handle: Option<thread::JoinHandle<()>>,
}

impl FakeProvider {
    pub fn start(loaded: bool) -> Self {
        Self::start_with(loaded, false)
    }

    /// A provider that accepts an unload but keeps the model resident.
    pub fn stubborn() -> Self {
        Self::start_with(true, true)
    }

    pub fn start_with(loaded: bool, stubborn: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("a loopback port");
        let endpoint = format!("http://{}", listener.local_addr().expect("a bound address"));
        listener
            .set_nonblocking(true)
            .expect("non-blocking accepts");
        let state = Arc::new(FakeState {
            loaded: Mutex::new(loaded),
            stubborn: Mutex::new(stubborn),
            ..FakeState::default()
        });
        let stop = Arc::new(AtomicBool::new(false));
        let serving = Arc::clone(&state);
        let stopping = Arc::clone(&stop);
        let handle = thread::spawn(move || {
            while !stopping.load(Ordering::Relaxed) {
                match listener.accept() {
                    Ok((stream, _)) => {
                        let _ = handle_connection(stream, &serving);
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                        thread::sleep(Duration::from_millis(2));
                    }
                    Err(_) => break,
                }
            }
        });
        Self {
            endpoint,
            state,
            stop,
            handle: Some(handle),
        }
    }

    pub fn requests(&self) -> Vec<String> {
        self.state.requests.lock().expect("not poisoned").clone()
    }

    pub fn bodies(&self) -> Vec<String> {
        self.state.bodies.lock().expect("not poisoned").clone()
    }
}

impl Drop for FakeProvider {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

/// Answer one request, reading the body so the write is real rather than assumed.
fn handle_connection(mut stream: TcpStream, state: &FakeState) -> std::io::Result<()> {
    stream.set_read_timeout(Some(Duration::from_secs(2)))?;
    let mut buffer = Vec::new();
    let mut chunk = [0u8; 1024];
    // Read the head, then exactly the body the head declares.
    let head_end = loop {
        let read = stream.read(&mut chunk)?;
        if read == 0 {
            return Ok(());
        }
        buffer.extend_from_slice(&chunk[..read]);
        if let Some(position) = find(&buffer, b"\r\n\r\n") {
            break position + 4;
        }
    };
    let head = String::from_utf8_lossy(&buffer[..head_end]).to_string();
    let length = head
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())?
        })
        .unwrap_or(0);
    while buffer.len() < head_end + length {
        let read = stream.read(&mut chunk)?;
        if read == 0 {
            break;
        }
        buffer.extend_from_slice(&chunk[..read]);
    }
    let body = String::from_utf8_lossy(&buffer[head_end..]).to_string();
    let request_line = head.lines().next().unwrap_or_default().to_string();
    state
        .requests
        .lock()
        .expect("not poisoned")
        .push(request_line.clone());
    if !body.is_empty() {
        state
            .bodies
            .lock()
            .expect("not poisoned")
            .push(body.clone());
    }

    let path = request_line.split_whitespace().nth(1).unwrap_or("/");
    let response = match (request_line.split_whitespace().next(), path) {
        (Some("GET"), "/api/version") => json(200, r#"{"version":"0.12.6"}"#),
        (Some("GET"), "/api/ps") => {
            let loaded = *state.loaded.lock().expect("not poisoned");
            json(200, &ps_body(loaded))
        }
        (Some("POST"), "/api/generate") => {
            // The documented unload is a generate with a zero keep-alive.
            let unloading = body.contains("\"keep_alive\":0");
            let stubborn = *state.stubborn.lock().expect("not poisoned");
            if unloading && !stubborn {
                *state.loaded.lock().expect("not poisoned") = false;
            }
            json(
                200,
                if unloading {
                    r#"{"done":true}"#
                } else {
                    r#"{"done":false}"#
                },
            )
        }
        _ => json(404, r#"{"error":"not found"}"#),
    };
    stream.write_all(&response)?;
    stream.flush()
}

fn ps_body(loaded: bool) -> String {
    if loaded {
        format!(
            r#"{{"models":[{{"name":"{MODEL}","model":"{MODEL}","size":{MODEL_BYTES},"size_vram":{MODEL_VRAM_BYTES},"expires_at":"2026-10-11T16:00:00Z"}}]}}"#
        )
    } else {
        r#"{"models":[]}"#.to_string()
    }
}

fn json(status: u16, body: &str) -> Vec<u8> {
    format!(
        "HTTP/1.1 {status} OK\r\nContent-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
        body.len()
    )
    .into_bytes()
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack
        .windows(needle.len())
        .position(|window| window == needle)
}
