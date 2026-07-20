//! HTTP-backed speech stages: the `Transcriber` / `Synthesizer` implementations
//! that call a speaches (OpenAI-compatible) container over plain HTTP.
//!
//! The container is a LAN/localhost service, so the shared `reqwest` client is
//! built without a TLS stack (`default-features = false`); an `https://` URL is
//! rejected upstream at config validation, never reaching a client build here.
//! Each stage owns one client for connection reuse and its own atomic counters,
//! reporting failures in-band as the stream's terminal `Err` (the caller knows
//! the pod / segment ids and emits the correlated JSONL line).
//!
//! The plumbing both stages share — client + endpoint construction
//! ([`build_stage`]), reqwest-error classification ([`classify_send`] /
//! [`classify_body`]), size-capped body reads ([`read_body_capped`]), and the
//! four-counter [`StageCounters`] — lives here so the next HTTP stage reuses it
//! rather than copying it.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;

mod stt;
#[cfg(test)]
pub(crate) mod test_support;
mod tts;

pub use stt::{HttpTranscriber, SttParams, SttStats, SttStatsSnapshot};
pub use tts::{HttpSynthesizer, TtsParams, TtsStats, TtsStatsSnapshot};

/// The URL type the stages parse their endpoints with. Re-exported so config
/// validation parse-checks endpoints with the same parser that builds them,
/// rather than a second one that could disagree.
pub use reqwest::Url;

/// Why constructing an HTTP stage failed: a base URL that will not parse, or a
/// `reqwest` client that will not build. Both are fatal startup errors — the
/// operator's config is wrong, so the daemon refuses to start rather than
/// failing every request at runtime.
#[derive(Debug, thiserror::Error)]
pub enum BuildError {
    #[error("invalid url {url:?}: {reason}")]
    Url { url: String, reason: String },
    #[error("http client build: {0}")]
    Client(String),
}

/// Compose and validate a stage's endpoint URL and build its reused client.
/// Joins `base_url` and `path`, parse-checks the composed endpoint so a bad base
/// is a fatal startup error rather than a per-request one, and builds one client
/// with the connect/total budgets for connection reuse. The returned `Url` is
/// stored and handed to `post` directly, so the endpoint is parsed once, not on
/// every request.
pub(crate) fn build_stage(
    base_url: &str,
    path: &str,
    connect_timeout: Duration,
    timeout: Duration,
) -> Result<(reqwest::Client, reqwest::Url), BuildError> {
    let endpoint = format!(
        "{}/{}",
        base_url.trim_end_matches('/'),
        path.trim_start_matches('/')
    );
    let url = reqwest::Url::parse(&endpoint).map_err(|e| BuildError::Url {
        url: endpoint.clone(),
        reason: e.to_string(),
    })?;
    let client = reqwest::Client::builder()
        .connect_timeout(connect_timeout)
        .timeout(timeout)
        .build()
        .map_err(|e| BuildError::Client(e.to_string()))?;
    Ok((client, url))
}

/// The trait-error constructors the shared reqwest-error classifiers need, so
/// both stages' send/body error mapping lives in one place instead of a copy per
/// stage.
pub(crate) trait StageError {
    fn connect(msg: String) -> Self;
    fn timeout() -> Self;
    fn decode(msg: String) -> Self;
}

/// Classify a send-phase `reqwest` error: a timeout is its own variant
/// (distinguishable in `stage_health`); everything else — connection refused,
/// DNS, reset — is a connect failure.
pub(crate) fn classify_send<E: StageError>(e: reqwest::Error) -> E {
    if e.is_timeout() {
        E::timeout()
    } else {
        E::connect(e.to_string())
    }
}

/// Classify a body-read `reqwest` error. A genuine deserialize failure (a body
/// that arrived but would not parse into the expected shape) is `decode`; a
/// transport error *during* the body transfer — a reset or drop after the
/// headers — is a transient connect failure, not the persistent server/model
/// bug `decode` denotes. Keeping them apart preserves the transient-vs-logic
/// signal the split error enum exists for.
pub(crate) fn classify_body<E: StageError>(e: reqwest::Error) -> E {
    if e.is_timeout() {
        E::timeout()
    } else if e.is_decode() {
        E::decode(e.to_string())
    } else {
        E::connect(e.to_string())
    }
}

/// Read a response body into memory under a hard byte ceiling, regardless of the
/// advertised `Content-Length`. Loops [`reqwest::Response::chunk`], mapping a
/// transport error mid-body through [`classify_body`], and returns `decode` the
/// moment the running total passes `max` — so a backend that omits or understates
/// `Content-Length` (chunked encoding, or a hostile peer) cannot drive unbounded
/// host allocation. The caller's `Content-Length` precheck stays the fast path
/// that rejects an honestly-advertised oversized reply without reading a byte.
pub(crate) async fn read_body_capped<E: StageError>(
    mut resp: reqwest::Response,
    max: u64,
) -> Result<Vec<u8>, E> {
    let hint = resp.content_length().map(|l| l.min(max)).unwrap_or(0);
    let mut body = Vec::with_capacity(hint as usize);
    let mut total: u64 = 0;
    while let Some(chunk) = resp.chunk().await.map_err(classify_body::<E>)? {
        total += chunk.len() as u64;
        if total > max {
            return Err(E::decode(format!("response body exceeds {max}-byte cap")));
        }
        body.extend_from_slice(&chunk);
    }
    Ok(body)
}

/// Four atomically-updated counters shared by both HTTP stages, read for
/// `stage_health` via [`StageCounters::snapshot`]. `requests` is incremented when
/// a request *starts* (before the round-trip await), so a snapshot taken while a
/// request is in flight shows `requests > ok + failed + timeouts` — the count of
/// requests neither completed nor yet cancelled, the signal that matters during a
/// wedged-container incident.
#[derive(Debug, Default)]
pub(crate) struct StageCounters {
    requests: AtomicU64,
    ok: AtomicU64,
    failed: AtomicU64,
    timeouts: AtomicU64,
}

impl StageCounters {
    /// Count a request as started, before its round-trip await.
    pub(crate) fn start(&self) {
        self.requests.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_ok(&self) {
        self.ok.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_timeout(&self) {
        self.timeouts.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_failed(&self) {
        self.failed.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn snapshot(&self) -> StageCountersValues {
        StageCountersValues {
            requests: self.requests.load(Ordering::Relaxed),
            ok: self.ok.load(Ordering::Relaxed),
            failed: self.failed.load(Ordering::Relaxed),
            timeouts: self.timeouts.load(Ordering::Relaxed),
        }
    }
}

/// A plain-`u64` copy of [`StageCounters`], the shape each stage's public
/// snapshot wraps.
pub(crate) struct StageCountersValues {
    pub requests: u64,
    pub ok: u64,
    pub failed: u64,
    pub timeouts: u64,
}

/// Truncate a response body to at most 256 bytes on a char boundary before it
/// rides a JSONL error line. Shared by both stages' `Status` error paths so a
/// multi-kilobyte error page cannot bloat the log.
pub(crate) fn truncate_body(body: &str) -> String {
    const MAX: usize = 256;
    if body.len() <= MAX {
        return body.to_string();
    }
    let mut end = MAX;
    while !body.is_char_boundary(end) {
        end -= 1;
    }
    body[..end].to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn short_body_is_unchanged() {
        assert_eq!(truncate_body("boom"), "boom");
        let exactly_max = "a".repeat(256);
        assert_eq!(truncate_body(&exactly_max), exactly_max);
    }

    #[test]
    fn long_ascii_body_truncates_to_256() {
        let body = "a".repeat(500);
        let out = truncate_body(&body);
        assert_eq!(out.len(), 256);
        assert!(body.starts_with(&out));
    }

    #[test]
    fn multibyte_boundary_is_not_split() {
        // "é" is two bytes (0xC3 0xA9). A body of 200 of them is 400 bytes; the
        // naive 256th byte lands inside the 128th 'é'. The walk-back must yield a
        // valid `&str` shorter than 256, never a mid-codepoint slice (which would
        // panic on `body[..end]`).
        let body = "é".repeat(200);
        let out = truncate_body(&body);
        assert!(out.len() <= 256);
        assert!(out.len() >= 254, "walk-back drops at most one 2-byte char");
        // Round-trips as valid UTF-8 (constructing `out` already proved it, but
        // assert the content is a clean prefix).
        assert!(body.starts_with(&out));
    }
}
