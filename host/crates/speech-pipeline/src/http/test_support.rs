//! In-process fake HTTP server shared by the STT and TTS stage tests. Binds a
//! loopback port, reads one full HTTP request (framing on `Content-Length`),
//! responds per [`Behavior`], and hands the captured request bytes back through
//! the join handle. We control both ends, so no HTTP-mock dev-dependency is
//! needed.

use std::time::Duration;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

/// What the fake server does after reading the request.
pub(crate) enum Behavior {
    /// Reply `200 OK` with the given `Content-Type` and body bytes.
    Ok {
        content_type: &'static str,
        body: Vec<u8>,
    },
    /// Reply with a non-2xx status and a plain-text body.
    Status(u16, String),
    /// Write exact response bytes verbatim — for tests that must craft their own
    /// headers (e.g. a bogus oversized `Content-Length`).
    Raw(Vec<u8>),
    /// Hold the connection open with no response so the client's total timeout
    /// fires. The task is aborted at test end.
    Stall,
}

/// Bind a one-shot fake server on a loopback port. Returns the base URL and a
/// join handle yielding the captured request bytes.
pub(crate) async fn spawn_server(behavior: Behavior) -> (String, tokio::task::JoinHandle<Vec<u8>>) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let handle = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await.unwrap();
        let req = read_http_request(&mut stream).await;
        match behavior {
            Behavior::Ok { content_type, body } => {
                let mut resp = format!(
                    "HTTP/1.1 200 OK\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                )
                .into_bytes();
                resp.extend_from_slice(&body);
                stream.write_all(&resp).await.unwrap();
                stream.flush().await.unwrap();
            }
            Behavior::Status(code, body) => {
                let resp = format!(
                    "HTTP/1.1 {code} Error\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                );
                stream.write_all(resp.as_bytes()).await.unwrap();
                stream.flush().await.unwrap();
            }
            Behavior::Raw(bytes) => {
                // A client that rejects on the size cap closes the connection
                // mid-body, so the write may fail with a broken pipe — expected,
                // not a server fault.
                let _ = stream.write_all(&bytes).await;
                let _ = stream.flush().await;
            }
            Behavior::Stall => {
                tokio::time::sleep(Duration::from_secs(30)).await;
            }
        }
        req
    });
    (format!("http://{addr}"), handle)
}

async fn read_http_request(stream: &mut TcpStream) -> Vec<u8> {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 4096];
    loop {
        let n = stream.read(&mut tmp).await.unwrap();
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&tmp[..n]);
        if let Some(hdr_end) = find_subslice(&buf, b"\r\n\r\n") {
            let content_len = parse_content_length(&buf[..hdr_end]);
            let body_start = hdr_end + 4;
            while buf.len() < body_start + content_len {
                let n = stream.read(&mut tmp).await.unwrap();
                if n == 0 {
                    break;
                }
                buf.extend_from_slice(&tmp[..n]);
            }
            break;
        }
    }
    buf
}

/// Locate `needle` in `haystack`; used by tests to assert on captured request
/// bytes (e.g. the RIFF marker of an uploaded WAV).
pub(crate) fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    haystack.windows(needle.len()).position(|w| w == needle)
}

fn parse_content_length(headers: &[u8]) -> usize {
    let text = String::from_utf8_lossy(headers);
    for line in text.lines() {
        let lower = line.to_ascii_lowercase();
        if let Some(v) = lower.strip_prefix("content-length:") {
            return v.trim().parse().unwrap_or(0);
        }
    }
    0
}
