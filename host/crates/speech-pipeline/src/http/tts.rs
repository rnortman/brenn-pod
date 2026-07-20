//! `HttpSynthesizer`: the TTS stage backed by speaches' OpenAI-compatible
//! `/v1/audio/speech` endpoint.
//!
//! The request asks for WAV at the spine rate; the response body is decoded with
//! `hound` and its spec checked against `SPINE_FORMAT` through the shared
//! [`check_spine_format`]. A mismatch is a reported `Format` error, never a silent
//! resample — a mis-configured server is a bug to surface, not to paper over. The
//! result is batch-shaped today (one `PcmChunk` carrying the whole clip); the
//! stream signature is the seam a future chunk-streaming backend fills without a
//! trait change.

use std::io::Cursor;
use std::sync::Arc;
use std::time::Duration;

use futures::stream::{self, BoxStream, StreamExt};
use serde::Serialize;

use super::{
    build_stage, classify_send, read_body_capped, truncate_body, BuildError, StageCounters,
    StageError,
};
use crate::traits::{PcmChunk, SynthesisError, Synthesizer};
use crate::wav::check_spine_format;
use crate::SPINE_FORMAT;

/// Largest response body this stage will buffer. A readback clip at the spine
/// rate is ~32 KB/s, so this covers minutes of audio; any `Content-Length` past
/// it is a misbehaving backend, rejected before the buffer allocation rather than
/// driving host memory.
const MAX_BODY_BYTES: u64 = 16 * 1024 * 1024;

impl StageError for SynthesisError {
    fn connect(msg: String) -> Self {
        SynthesisError::Connect(msg)
    }
    fn timeout() -> Self {
        SynthesisError::Timeout
    }
    fn decode(msg: String) -> Self {
        SynthesisError::Decode(msg)
    }
}

/// Everything `HttpSynthesizer::new` needs from config: the base URL, the model
/// and voice the operator's speaches install serves, and the per-request time
/// budgets.
#[derive(Debug, Clone)]
pub struct TtsParams {
    /// Base URL of the speaches container, e.g. `http://10.0.0.5:8000`.
    pub url: String,
    /// TTS model name, as the server registers it.
    pub model: String,
    /// Voice name, as the server registers it.
    pub voice: String,
    /// Total per-request budget (connect + request + response).
    pub timeout: Duration,
    /// Connect budget, so a down container fails fast rather than at `timeout`.
    pub connect_timeout: Duration,
}

/// Shared, atomically-updated TTS counters, read for `stage_health` via
/// [`TtsStats::snapshot`]. The atomics stay private so the synchronization detail
/// never leaks to the emit site (the `WakeStats` idiom).
#[derive(Debug, Default)]
pub struct TtsStats(StageCounters);

/// A point-in-time copy of [`TtsStats`], for `stage_health` reporting: plain
/// `u64` fields, `Copy + Serialize`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct TtsStatsSnapshot {
    /// Total synthesis requests started (incremented before the round-trip, so
    /// `requests - (ok + failed + timeouts)` is the in-flight count).
    pub requests: u64,
    /// Requests that yielded a chunk.
    pub ok: u64,
    /// Requests that failed for a reason other than timeout (connect, status,
    /// decode, format).
    pub failed: u64,
    /// Requests that exceeded their time budget.
    pub timeouts: u64,
}

impl TtsStats {
    fn record_outcome(&self, result: &Result<PcmChunk, SynthesisError>) {
        match result {
            Ok(_) => self.0.record_ok(),
            Err(SynthesisError::Timeout) => self.0.record_timeout(),
            Err(_) => self.0.record_failed(),
        }
    }

    /// A `Copy` snapshot of the counters, read for `stage_health`.
    pub fn snapshot(&self) -> TtsStatsSnapshot {
        let v = self.0.snapshot();
        TtsStatsSnapshot {
            requests: v.requests,
            ok: v.ok,
            failed: v.failed,
            timeouts: v.timeouts,
        }
    }
}

/// The `/v1/audio/speech` JSON request body. `sample_rate` asks the server for
/// spine-rate output; the decoded result is validated regardless, so a server
/// that ignores the field produces a loud `Format` error rather than silent
/// wrong-rate audio.
#[derive(Debug, Serialize)]
struct SpeechRequest<'a> {
    model: &'a str,
    voice: &'a str,
    input: &'a str,
    response_format: &'a str,
    sample_rate: u32,
}

/// TTS stage over speaches' `/v1/audio/speech`. Holds one reused client.
#[derive(Debug)]
pub struct HttpSynthesizer {
    client: reqwest::Client,
    endpoint: reqwest::Url,
    model: String,
    voice: String,
    stats: Arc<TtsStats>,
}

impl HttpSynthesizer {
    /// Build the synthesizer, resolving the endpoint URL and the reused client.
    /// Fallible: a malformed base URL or a client that will not build is a fatal
    /// startup error, not a per-request failure.
    pub fn new(params: TtsParams, stats: Arc<TtsStats>) -> Result<Self, BuildError> {
        let (client, endpoint) = build_stage(
            &params.url,
            "v1/audio/speech",
            params.connect_timeout,
            params.timeout,
        )?;
        Ok(Self {
            client,
            endpoint,
            model: params.model,
            voice: params.voice,
            stats,
        })
    }
}

impl Synthesizer for HttpSynthesizer {
    fn synthesize(&self, text: &str) -> BoxStream<'static, Result<PcmChunk, SynthesisError>> {
        let client = self.client.clone();
        let endpoint = self.endpoint.clone();
        let model = self.model.clone();
        let voice = self.voice.clone();
        let input = text.to_string();
        let stats = Arc::clone(&self.stats);
        // One request → one item: exactly one chunk, or one terminal `Err`,
        // matching the stream contract on the trait.
        stream::once(async move {
            stats.0.start();
            let result = request(&client, endpoint, &model, &voice, &input).await;
            stats.record_outcome(&result);
            result
        })
        .boxed()
    }
}

/// Perform one synthesis round-trip and produce the whole clip as one chunk.
async fn request(
    client: &reqwest::Client,
    endpoint: reqwest::Url,
    model: &str,
    voice: &str,
    input: &str,
) -> Result<PcmChunk, SynthesisError> {
    let body = SpeechRequest {
        model,
        voice,
        input,
        response_format: "wav",
        sample_rate: SPINE_FORMAT.sample_rate_hz,
    };

    let resp = client
        .post(endpoint)
        .json(&body)
        .send()
        .await
        .map_err(classify_send::<SynthesisError>)?;

    let status = resp.status();
    if !status.is_success() {
        // Sentinel, not empty string, if the body read itself fails: "server sent
        // no body" and "we could not read the body" are different diagnoses.
        let body = resp
            .text()
            .await
            .unwrap_or_else(|_| "<error body unavailable>".into());
        return Err(SynthesisError::Status {
            code: status.as_u16(),
            body: truncate_body(&body),
        });
    }

    // Reject an honestly-advertised oversized body before reading a byte; the
    // capped read below holds the bound even when `Content-Length` is absent or
    // understated (chunked encoding, or a hostile peer).
    if let Some(len) = resp.content_length() {
        if len > MAX_BODY_BYTES {
            return Err(SynthesisError::Decode(format!(
                "response body {len} bytes exceeds {MAX_BODY_BYTES}-byte cap"
            )));
        }
    }

    let bytes = read_body_capped::<SynthesisError>(resp, MAX_BODY_BYTES).await?;
    decode_clip(&bytes)
}

/// Decode a WAV body into one spine-format `PcmChunk`. Rejects a non-spine spec
/// (`Format`) and an empty clip (`Decode`) — an EOA-only playback job must never
/// look like a successful synthesis.
fn decode_clip(bytes: &[u8]) -> Result<PcmChunk, SynthesisError> {
    let mut reader = hound::WavReader::new(Cursor::new(bytes))
        .map_err(|e| SynthesisError::Decode(format!("wav header: {e}")))?;
    check_spine_format(&reader.spec())
        .map_err(|v| SynthesisError::Format { got: v.to_string() })?;
    let pcm: Vec<i16> = reader
        .samples::<i16>()
        .collect::<Result<_, _>>()
        .map_err(|e| SynthesisError::Decode(format!("wav samples: {e}")))?;
    if pcm.is_empty() {
        return Err(SynthesisError::Decode("empty clip: zero samples".into()));
    }
    Ok(PcmChunk {
        pcm: Arc::from(pcm.as_slice()),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::test_support::{spawn_server, Behavior};
    use tokio::net::TcpListener;

    fn params(url: String) -> TtsParams {
        TtsParams {
            url,
            model: "Kokoro-82M".into(),
            voice: "af_heart".into(),
            timeout: Duration::from_secs(5),
            connect_timeout: Duration::from_secs(2),
        }
    }

    /// Build a WAV body at the given rate/channels for a handful of samples.
    fn wav_body(sample_rate: u32, channels: u16, samples: &[i16]) -> Vec<u8> {
        let spec = hound::WavSpec {
            channels,
            sample_rate,
            bits_per_sample: 16,
            sample_format: hound::SampleFormat::Int,
        };
        let mut cursor = Cursor::new(Vec::new());
        {
            let mut writer = hound::WavWriter::new(&mut cursor, spec).unwrap();
            for &s in samples {
                writer.write_sample(s).unwrap();
            }
            writer.finalize().unwrap();
        }
        cursor.into_inner()
    }

    fn wav_ok(body: Vec<u8>) -> Behavior {
        Behavior::Ok {
            content_type: "audio/wav",
            body,
        }
    }

    #[tokio::test]
    async fn success_yields_one_chunk_with_samples() {
        let samples = [10i16, -20, 30, -40, 500, -500];
        let wav = wav_body(16_000, 1, &samples);
        let (url, server) = spawn_server(wav_ok(wav)).await;
        let s = HttpSynthesizer::new(params(url), Arc::new(TtsStats::default())).unwrap();

        let mut stream = s.synthesize("hey jarvis");
        let chunk = stream.next().await.unwrap().unwrap();
        assert_eq!(chunk.pcm.to_vec(), samples.to_vec());
        assert!(stream.next().await.is_none(), "stream ends after the chunk");

        // The captured request is the shape speaches expects.
        let req = server.await.unwrap();
        let req_text = String::from_utf8_lossy(&req);
        assert!(req_text.starts_with("POST /v1/audio/speech"));
        assert!(req_text.contains(r#""model":"Kokoro-82M""#));
        assert!(req_text.contains(r#""voice":"af_heart""#));
        assert!(req_text.contains(r#""input":"hey jarvis""#));
        assert!(req_text.contains(r#""response_format":"wav""#));
        assert!(req_text.contains(r#""sample_rate":16000"#));
    }

    #[tokio::test]
    async fn wrong_rate_is_format_error() {
        let wav = wav_body(24_000, 1, &[1i16, 2, 3]);
        let (url, _server) = spawn_server(wav_ok(wav)).await;
        let s = HttpSynthesizer::new(params(url), Arc::new(TtsStats::default())).unwrap();

        let err = s.synthesize("hi").next().await.unwrap().unwrap_err();
        assert!(matches!(err, SynthesisError::Format { .. }), "got {err:?}");
    }

    #[tokio::test]
    async fn zero_sample_body_is_decode_error() {
        let wav = wav_body(16_000, 1, &[]);
        let (url, _server) = spawn_server(wav_ok(wav)).await;
        let s = HttpSynthesizer::new(params(url), Arc::new(TtsStats::default())).unwrap();

        let err = s.synthesize("hi").next().await.unwrap().unwrap_err();
        assert!(matches!(err, SynthesisError::Decode(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn non_2xx_is_status_error() {
        let (url, _server) = spawn_server(Behavior::Status(500, "boom".into())).await;
        let stats = Arc::new(TtsStats::default());
        let s = HttpSynthesizer::new(params(url), Arc::clone(&stats)).unwrap();

        let err = s.synthesize("hi").next().await.unwrap().unwrap_err();
        match err {
            SynthesisError::Status { code, body } => {
                assert_eq!(code, 500);
                assert_eq!(body, "boom");
            }
            other => panic!("expected Status, got {other:?}"),
        }
        // A non-timeout error lands in `failed`, not `timeouts`.
        assert_eq!(
            stats.snapshot(),
            TtsStatsSnapshot {
                requests: 1,
                ok: 0,
                failed: 1,
                timeouts: 0,
            }
        );
    }

    #[tokio::test]
    async fn garbage_body_is_decode_error() {
        let (url, _server) = spawn_server(wav_ok(b"not a wav at all".to_vec())).await;
        let s = HttpSynthesizer::new(params(url), Arc::new(TtsStats::default())).unwrap();

        let err = s.synthesize("hi").next().await.unwrap().unwrap_err();
        assert!(matches!(err, SynthesisError::Decode(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn oversized_content_length_is_rejected() {
        // 200 OK claiming a gigabyte body; the stage must reject on the header,
        // never attempt to buffer it. The tiny actual body is never read.
        let raw =
            b"HTTP/1.1 200 OK\r\nContent-Type: audio/wav\r\nContent-Length: 1073741824\r\nConnection: close\r\n\r\nxx"
                .to_vec();
        let (url, _server) = spawn_server(Behavior::Raw(raw)).await;
        let s = HttpSynthesizer::new(params(url), Arc::new(TtsStats::default())).unwrap();

        let err = s.synthesize("hi").next().await.unwrap().unwrap_err();
        assert!(matches!(err, SynthesisError::Decode(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn unbounded_body_without_content_length_is_rejected() {
        // 200 OK with no Content-Length (the precheck's blind spot): the server
        // streams past the cap and closes. The running byte ceiling must reject it
        // before it buffers unbounded.
        let mut raw =
            b"HTTP/1.1 200 OK\r\nContent-Type: audio/wav\r\nConnection: close\r\n\r\n".to_vec();
        raw.resize(raw.len() + MAX_BODY_BYTES as usize + 1024, b' ');
        let (url, _server) = spawn_server(Behavior::Raw(raw)).await;
        let s = HttpSynthesizer::new(params(url), Arc::new(TtsStats::default())).unwrap();

        let err = s.synthesize("hi").next().await.unwrap().unwrap_err();
        assert!(matches!(err, SynthesisError::Decode(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn connect_refused_is_connect_error() {
        // Bind then drop the listener so the port is dead but was never
        // accepting — the "container not up yet" case §3 names.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let s = HttpSynthesizer::new(
            params(format!("http://{addr}")),
            Arc::new(TtsStats::default()),
        )
        .unwrap();

        let err = s.synthesize("hi").next().await.unwrap().unwrap_err();
        assert!(matches!(err, SynthesisError::Connect(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn stalled_server_times_out_within_budget() {
        let (url, _server) = spawn_server(Behavior::Stall).await;
        let mut p = params(url);
        p.timeout = Duration::from_millis(200);
        let stats = Arc::new(TtsStats::default());
        let s = HttpSynthesizer::new(p, Arc::clone(&stats)).unwrap();

        let started = std::time::Instant::now();
        let err = s.synthesize("hi").next().await.unwrap().unwrap_err();
        assert!(matches!(err, SynthesisError::Timeout), "got {err:?}");
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "timed out on budget"
        );
        assert_eq!(stats.snapshot().timeouts, 1);
    }

    #[tokio::test]
    async fn stats_count_success() {
        let wav = wav_body(16_000, 1, &[1i16, 2]);
        let (url, _server) = spawn_server(wav_ok(wav)).await;
        let stats = Arc::new(TtsStats::default());
        let s = HttpSynthesizer::new(params(url), Arc::clone(&stats)).unwrap();

        let _ = s.synthesize("hi").next().await.unwrap().unwrap();
        let snap = stats.snapshot();
        assert_eq!(snap.requests, 1);
        assert_eq!(snap.ok, 1);
        assert_eq!(snap.failed, 0);
        assert_eq!(snap.timeouts, 0);
    }

    #[test]
    fn unparseable_url_fails_to_build() {
        let err = HttpSynthesizer::new(
            params("http://[not a url".into()),
            Arc::new(TtsStats::default()),
        )
        .unwrap_err();
        assert!(matches!(err, BuildError::Url { .. }), "got {err:?}");
    }
}
