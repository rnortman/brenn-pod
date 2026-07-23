//! `HttpTranscriber`: the STT stage backed by speaches' OpenAI-compatible
//! `/v1/audio/transcriptions` endpoint.
//!
//! The segment PCM (16 kHz mono S16 by construction — the spine admits nothing
//! else) is encoded to an in-memory WAV and posted as multipart `file`, the
//! OpenAI-compatible common denominator. The response is batch-shaped today —
//! one `{"text": ...}` object yielding one final `TranscriptEvent` — but the
//! stream signature is the seam a future streaming backend fills without a trait
//! change; the parrot needs only the settled text.

use std::sync::Arc;
use std::time::Duration;

use futures::stream::{self, BoxStream, StreamExt};
use serde::{Deserialize, Serialize};

use super::{
    BuildError, StageCounters, StageError, build_stage, classify_send, read_body_capped,
    truncate_body,
};
use crate::traits::{SegmentAudio, TranscribeError, Transcriber, TranscriptEvent};
use crate::types::TranscriptConfidence;

/// Largest response body this stage will buffer. A `verbose_json` reply carries a
/// per-segment array on top of the transcript text, but an endpointer-bounded
/// voice segment still yields at most a handful of short segments — kilobytes, not
/// hundreds. A `Content-Length` past this generous cap is a misbehaving backend,
/// not a legitimate transcript; reject it before the buffer allocation rather than
/// letting an oversized body drive host memory.
const MAX_BODY_BYTES: u64 = 256 * 1024;

/// Largest transcript text this stage accepts. A segment is endpointer-bounded in
/// duration, so a real spoken transcript is at most a few KB; anything past this
/// generous cap is a misbehaving backend, and the text flows onto a persistent
/// JSONL line and into a follow-on TTS request, so it must not be unbounded.
const MAX_TRANSCRIPT_BYTES: usize = 64 * 1024;

impl StageError for TranscribeError {
    fn connect(msg: String) -> Self {
        TranscribeError::Connect(msg)
    }
    fn timeout() -> Self {
        TranscribeError::Timeout
    }
    fn decode(msg: String) -> Self {
        TranscribeError::Decode(msg)
    }
}

/// Everything `HttpTranscriber::new` needs from config: the base URL, the model
/// the operator's speaches install serves, an optional language hint, and the
/// per-request time budgets.
#[derive(Debug, Clone)]
pub struct SttParams {
    /// Base URL of the speaches container, e.g. `http://10.0.0.5:8000`.
    pub url: String,
    /// Whisper model name, as the server registers it.
    pub model: String,
    /// Optional language hint; omitted from the request when absent.
    pub language: Option<String>,
    /// Total per-request budget (connect + upload + response).
    pub timeout: Duration,
    /// Connect budget, so a down container fails fast rather than at `timeout`.
    pub connect_timeout: Duration,
}

/// Shared, atomically-updated STT counters, read for `stage_health` via
/// [`SttStats::snapshot`]. The atomics stay private so the synchronization detail
/// never leaks to the emit site (the `WakeStats` idiom).
#[derive(Debug, Default)]
pub struct SttStats(StageCounters);

/// A point-in-time copy of [`SttStats`], for `stage_health` reporting: plain
/// `u64` fields, `Copy + Serialize`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub struct SttStatsSnapshot {
    /// Total transcription requests started (incremented before the round-trip,
    /// so `requests - (ok + failed + timeouts)` is the in-flight count).
    pub requests: u64,
    /// Requests that yielded a transcript.
    pub ok: u64,
    /// Requests that failed for a reason other than timeout (connect, status,
    /// decode).
    pub failed: u64,
    /// Requests that exceeded their time budget.
    pub timeouts: u64,
}

impl SttStats {
    fn record_outcome(&self, result: &Result<TranscriptEvent, TranscribeError>) {
        match result {
            Ok(_) => self.0.record_ok(),
            Err(TranscribeError::Timeout) => self.0.record_timeout(),
            Err(_) => self.0.record_failed(),
        }
    }

    /// A `Copy` snapshot of the counters, read for `stage_health`.
    pub fn snapshot(&self) -> SttStatsSnapshot {
        let v = self.0.snapshot();
        SttStatsSnapshot {
            requests: v.requests,
            ok: v.ok,
            failed: v.failed,
            timeouts: v.timeouts,
        }
    }
}

/// The `/v1/audio/transcriptions` response shape we consume. Under
/// `response_format=verbose_json` the backend adds a `segments` array carrying
/// per-segment quality fields; a plain-`json` backend omits it, so `segments`
/// defaults to empty and the transcript still parses.
#[derive(Debug, Deserialize)]
struct TranscriptionResponse {
    text: String,
    #[serde(default, deserialize_with = "lenient_segments")]
    segments: Vec<ResponseSegment>,
}

/// Deserialize the `segments` array defensively so the observability tail can
/// never fail the primary payload. A `null` (or a value that is not an array at
/// all) yields no segments, and any element whose types deviate from
/// [`ResponseSegment`] (a `null` `start`, a stringified numeric, an unexpected
/// shape) is dropped rather than aborting the whole parse. The transcript text
/// must survive a backend whose segment schema differs; a deviating segment
/// degrades the confidence summary, never the transcript.
fn lenient_segments<'de, D>(deserializer: D) -> Result<Vec<ResponseSegment>, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let raw = serde_json::Value::deserialize(deserializer)?;
    let serde_json::Value::Array(elems) = raw else {
        return Ok(Vec::new());
    };
    Ok(elems
        .into_iter()
        .filter_map(|v| serde_json::from_value(v).ok())
        .collect())
}

/// One `verbose_json` segment's quality fields. `start`/`end` bound the segment
/// in seconds (used to duration-weight the `avg_logprob` aggregate); the three
/// quality fields are whisper's per-segment confidence tells. Every field is
/// lenient: a segment (or a whole backend) that omits any of them degrades the
/// confidence *summary*, never the transcript text — parsing the response must
/// not hinge on the observability payload.
#[derive(Debug, Deserialize)]
struct ResponseSegment {
    #[serde(default)]
    start: f64,
    #[serde(default)]
    end: f64,
    #[serde(default)]
    avg_logprob: Option<f32>,
    #[serde(default)]
    no_speech_prob: Option<f32>,
    #[serde(default)]
    compression_ratio: Option<f32>,
}

/// Fold the response's segments into a [`TranscriptConfidence`], or `None` when
/// no segment carries the quality fields (a plain-`json` backend, or one that
/// ships segments without whisper's confidence tells). Only segments carrying
/// all three fields contribute; a degraded segment is skipped rather than
/// defaulting to a misleading value. `avg_logprob` is weighted by each segment's
/// duration so a long segment dominates a short filler one; a degenerate
/// response whose durations sum to zero falls back to an unweighted mean rather
/// than dividing by zero. `no_speech_prob` and `compression_ratio` take the
/// worst (maximum) segment value.
fn summarize_confidence(segments: &[ResponseSegment]) -> Option<TranscriptConfidence> {
    struct Usable {
        start: f64,
        end: f64,
        avg_logprob: f32,
        no_speech_prob: f32,
        compression_ratio: f32,
    }
    let usable: Vec<Usable> = segments
        .iter()
        .filter_map(|s| {
            Some(Usable {
                start: s.start,
                end: s.end,
                avg_logprob: s.avg_logprob?,
                no_speech_prob: s.no_speech_prob?,
                compression_ratio: s.compression_ratio?,
            })
        })
        .collect();
    let (first, rest) = usable.split_first()?;
    let total_dur: f64 = usable.iter().map(|s| (s.end - s.start).max(0.0)).sum();
    let avg_logprob = if total_dur > 0.0 {
        let weighted: f64 = usable
            .iter()
            .map(|s| f64::from(s.avg_logprob) * (s.end - s.start).max(0.0))
            .sum();
        (weighted / total_dur) as f32
    } else {
        let sum: f64 = usable.iter().map(|s| f64::from(s.avg_logprob)).sum();
        (sum / usable.len() as f64) as f32
    };
    let no_speech_prob = rest
        .iter()
        .fold(first.no_speech_prob, |m, s| m.max(s.no_speech_prob));
    let compression_ratio = rest
        .iter()
        .fold(first.compression_ratio, |m, s| m.max(s.compression_ratio));
    Some(TranscriptConfidence {
        avg_logprob,
        no_speech_prob,
        compression_ratio,
        segments: usable.len() as u32,
    })
}

/// STT stage over speaches' `/v1/audio/transcriptions`. Holds one reused client.
#[derive(Debug)]
pub struct HttpTranscriber {
    client: reqwest::Client,
    endpoint: reqwest::Url,
    model: String,
    language: Option<String>,
    stats: Arc<SttStats>,
}

impl HttpTranscriber {
    /// Build the transcriber, resolving the endpoint URL and the reused client.
    /// Fallible: a malformed base URL or a client that will not build is a fatal
    /// startup error, not a per-request failure.
    pub fn new(params: SttParams, stats: Arc<SttStats>) -> Result<Self, BuildError> {
        let (client, endpoint) = build_stage(
            &params.url,
            "v1/audio/transcriptions",
            params.connect_timeout,
            params.timeout,
        )?;
        Ok(Self {
            client,
            endpoint,
            model: params.model,
            language: params.language,
            stats,
        })
    }
}

impl Transcriber for HttpTranscriber {
    fn transcribe(
        &self,
        audio: SegmentAudio,
    ) -> BoxStream<'static, Result<TranscriptEvent, TranscribeError>> {
        let client = self.client.clone();
        let endpoint = self.endpoint.clone();
        let model = self.model.clone();
        let language = self.language.clone();
        let stats = Arc::clone(&self.stats);
        // One request → one item: exactly one final event, or one terminal `Err`,
        // matching the stream contract on the trait.
        stream::once(async move {
            stats.0.start();
            let result = request(&client, endpoint, &model, language.as_deref(), &audio).await;
            stats.record_outcome(&result);
            result
        })
        .boxed()
    }
}

/// Perform one transcription round-trip and produce the settled final event.
async fn request(
    client: &reqwest::Client,
    endpoint: reqwest::Url,
    model: &str,
    language: Option<&str>,
    audio: &SegmentAudio,
) -> Result<TranscriptEvent, TranscribeError> {
    let wav = encode_wav(audio).map_err(|e| TranscribeError::Decode(format!("wav encode: {e}")))?;
    let file = reqwest::multipart::Part::bytes(wav).file_name("segment.wav");
    let mut form = reqwest::multipart::Form::new()
        .part("file", file)
        .text("model", model.to_string())
        // verbose_json returns the per-segment quality fields we summarize;
        // temperature=0 makes decoding deterministic and reduces hallucination.
        .text("response_format", "verbose_json")
        .text("temperature", "0");
    if let Some(lang) = language {
        form = form.text("language", lang.to_string());
    }

    let resp = client
        .post(endpoint)
        .multipart(form)
        .send()
        .await
        .map_err(classify_send::<TranscribeError>)?;

    let status = resp.status();
    if !status.is_success() {
        // Sentinel, not empty string, if the body read itself fails: "server sent
        // no body" and "we could not read the body" are different diagnoses.
        let body = resp
            .text()
            .await
            .unwrap_or_else(|_| "<error body unavailable>".into());
        return Err(TranscribeError::Status {
            code: status.as_u16(),
            body: truncate_body(&body),
        });
    }

    // Reject an honestly-advertised oversized body before reading a byte; the
    // capped read below holds the bound even when `Content-Length` is absent or
    // understated (chunked encoding, or a hostile peer).
    if let Some(len) = resp.content_length()
        && len > MAX_BODY_BYTES
    {
        return Err(TranscribeError::Decode(format!(
            "response body {len} bytes exceeds {MAX_BODY_BYTES}-byte cap"
        )));
    }

    let body = read_body_capped::<TranscribeError>(resp, MAX_BODY_BYTES).await?;
    let parsed: TranscriptionResponse =
        serde_json::from_slice(&body).map_err(|e| TranscribeError::Decode(e.to_string()))?;
    if parsed.text.len() > MAX_TRANSCRIPT_BYTES {
        return Err(TranscribeError::Decode(format!(
            "transcript {} bytes exceeds {MAX_TRANSCRIPT_BYTES}-byte cap",
            parsed.text.len()
        )));
    }
    let confidence = summarize_confidence(&parsed.segments);
    Ok(TranscriptEvent {
        text: parsed.text,
        is_final: true,
        confidence,
    })
}

/// Encode a segment's PCM as a mono S16 WAV in memory. The sink is pre-sized to
/// the exact canonical WAV length (44-byte header + 2 bytes/sample), so it never
/// reallocates while writing. The `Cursor<Vec<u8>>` sink cannot do I/O, so this
/// only errors on a hound-internal condition unreachable for in-range `i16`
/// samples.
fn encode_wav(audio: &SegmentAudio) -> Result<Vec<u8>, hound::Error> {
    let spec = hound::WavSpec {
        channels: 1,
        sample_rate: audio.sample_rate_hz,
        bits_per_sample: 16,
        sample_format: hound::SampleFormat::Int,
    };
    let mut cursor = std::io::Cursor::new(Vec::with_capacity(44 + audio.pcm.len() * 2));
    {
        let mut writer = hound::WavWriter::new(&mut cursor, spec)?;
        for &sample in audio.pcm.iter() {
            writer.write_sample(sample)?;
        }
        writer.finalize()?;
    }
    Ok(cursor.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::http::test_support::{Behavior, find_subslice, spawn_server};
    use tokio::net::TcpListener;

    fn sample_audio() -> SegmentAudio {
        SegmentAudio {
            pcm: Arc::from([1i16, -2, 3, -4, 100, -100].as_slice()),
            sample_rate_hz: 16_000,
        }
    }

    fn params(url: String) -> SttParams {
        SttParams {
            url,
            model: "faster-whisper-small".into(),
            language: Some("en".into()),
            timeout: Duration::from_secs(5),
            connect_timeout: Duration::from_secs(2),
        }
    }

    fn json_ok(body: &str) -> Behavior {
        Behavior::Ok {
            content_type: "application/json",
            body: body.as_bytes().to_vec(),
        }
    }

    #[test]
    fn verbose_json_multi_segment_aggregates() {
        // Two segments of unequal duration: avg_logprob is duration-weighted, so
        // the 3 s segment (-0.5) dominates the 1 s segment (-0.1). Weighted mean =
        // (-0.1*1 + -0.5*3) / 4 = -0.4. no_speech_prob and compression_ratio take
        // the per-segment maximum.
        let body = r#"{
            "text": "hello world",
            "segments": [
                {"start": 0.0, "end": 1.0, "avg_logprob": -0.1, "no_speech_prob": 0.02, "compression_ratio": 1.2},
                {"start": 1.0, "end": 4.0, "avg_logprob": -0.5, "no_speech_prob": 0.30, "compression_ratio": 1.8}
            ]
        }"#;
        let parsed: TranscriptionResponse = serde_json::from_str(body).unwrap();
        let conf = summarize_confidence(&parsed.segments).unwrap();
        assert!(
            (conf.avg_logprob - (-0.4)).abs() < 1e-5,
            "{}",
            conf.avg_logprob
        );
        assert!((conf.no_speech_prob - 0.30).abs() < 1e-6);
        assert!((conf.compression_ratio - 1.8).abs() < 1e-6);
        assert_eq!(conf.segments, 2);
    }

    #[test]
    fn verbose_json_single_segment_aggregates() {
        let body = r#"{
            "text": "hi",
            "segments": [
                {"start": 0.0, "end": 2.0, "avg_logprob": -0.23, "no_speech_prob": 0.05, "compression_ratio": 1.4}
            ]
        }"#;
        let parsed: TranscriptionResponse = serde_json::from_str(body).unwrap();
        let conf = summarize_confidence(&parsed.segments).unwrap();
        assert!((conf.avg_logprob - (-0.23)).abs() < 1e-5);
        assert!((conf.no_speech_prob - 0.05).abs() < 1e-6);
        assert!((conf.compression_ratio - 1.4).abs() < 1e-6);
        assert_eq!(conf.segments, 1);
    }

    #[test]
    fn plain_json_without_segments_parses_and_summarizes_none() {
        // The pre-verbose_json shape (and any backend that ignores the format
        // hint) still parses; the absent segments array yields no confidence.
        let parsed: TranscriptionResponse =
            serde_json::from_str(r#"{"text":"hey jarvis"}"#).unwrap();
        assert_eq!(parsed.text, "hey jarvis");
        assert!(summarize_confidence(&parsed.segments).is_none());
    }

    #[test]
    fn segments_missing_quality_fields_keep_the_text() {
        // A backend that ships segments without whisper's quality fields must not
        // fail the parse — the transcript text is the primary payload, the
        // confidence summary the disposable observability tail.
        let parsed: TranscriptionResponse =
            serde_json::from_str(r#"{"text":"hello","segments":[{"start":0.0,"end":1.0}]}"#)
                .unwrap();
        assert_eq!(parsed.text, "hello");
        assert!(summarize_confidence(&parsed.segments).is_none());
    }

    #[test]
    fn null_segments_keep_the_text() {
        // A backend that emits `"segments": null` (not an array) must not fail the
        // whole parse — the observability tail is never load-bearing for the text.
        let parsed: TranscriptionResponse =
            serde_json::from_str(r#"{"text":"hi","segments":null}"#).unwrap();
        assert_eq!(parsed.text, "hi");
        assert!(summarize_confidence(&parsed.segments).is_none());
    }

    #[test]
    fn wrong_typed_segment_fields_keep_the_text() {
        // One segment has a null `start` and one has a stringified `avg_logprob`;
        // both deviate from the expected numeric types. They drop out of the
        // summary, but the transcript text survives — parsing must not hinge on a
        // backend matching the segment schema exactly.
        let body = r#"{
            "text": "still here",
            "segments": [
                {"start": null, "end": 1.0, "avg_logprob": -0.2, "no_speech_prob": 0.04, "compression_ratio": 1.3},
                {"start": 1.0, "end": 2.0, "avg_logprob": "-0.5", "no_speech_prob": 0.30, "compression_ratio": 1.8}
            ]
        }"#;
        let parsed: TranscriptionResponse = serde_json::from_str(body).unwrap();
        assert_eq!(parsed.text, "still here");
        assert!(summarize_confidence(&parsed.segments).is_none());
    }

    #[test]
    fn degraded_segment_is_skipped_not_defaulted() {
        // One segment carries the quality fields, one omits them. The summary
        // reflects only the usable segment; the degraded one neither corrupts the
        // aggregate with a default nor drops the whole summary.
        let body = r#"{
            "text": "hi there",
            "segments": [
                {"start": 0.0, "end": 1.0, "avg_logprob": -0.2, "no_speech_prob": 0.04, "compression_ratio": 1.3},
                {"start": 1.0, "end": 2.0}
            ]
        }"#;
        let parsed: TranscriptionResponse = serde_json::from_str(body).unwrap();
        let conf = summarize_confidence(&parsed.segments).unwrap();
        assert!((conf.avg_logprob - (-0.2)).abs() < 1e-5);
        assert!((conf.no_speech_prob - 0.04).abs() < 1e-6);
        assert!((conf.compression_ratio - 1.3).abs() < 1e-6);
        assert_eq!(conf.segments, 1);
    }

    #[test]
    fn zero_duration_segments_fall_back_to_unweighted_mean() {
        // A degenerate response whose segment durations sum to zero must not
        // divide by zero; the mean is unweighted: (-0.2 + -0.4) / 2 = -0.3.
        let body = r#"{
            "text": "x",
            "segments": [
                {"start": 0.0, "end": 0.0, "avg_logprob": -0.2, "no_speech_prob": 0.1, "compression_ratio": 1.0},
                {"start": 0.0, "end": 0.0, "avg_logprob": -0.4, "no_speech_prob": 0.2, "compression_ratio": 1.5}
            ]
        }"#;
        let parsed: TranscriptionResponse = serde_json::from_str(body).unwrap();
        let conf = summarize_confidence(&parsed.segments).unwrap();
        assert!(
            (conf.avg_logprob - (-0.3)).abs() < 1e-5,
            "{}",
            conf.avg_logprob
        );
        assert_eq!(conf.segments, 2);
    }

    #[tokio::test]
    async fn verbose_json_confidence_rides_the_final_event() {
        let body = r#"{"text":"ok","segments":[{"start":0.0,"end":1.0,"avg_logprob":-0.15,"no_speech_prob":0.03,"compression_ratio":1.1}]}"#;
        let (url, server) = spawn_server(json_ok(body)).await;
        let t = HttpTranscriber::new(params(url), Arc::new(SttStats::default())).unwrap();

        let event = t.transcribe(sample_audio()).next().await.unwrap().unwrap();
        let conf = event.confidence.expect("verbose_json carries confidence");
        assert!((conf.avg_logprob - (-0.15)).abs() < 1e-5);
        assert_eq!(conf.segments, 1);

        // The request selects verbose_json and pins temperature.
        let req = String::from_utf8_lossy(&server.await.unwrap()).into_owned();
        assert!(req.contains("verbose_json"), "{req}");
        assert!(req.contains(r#"name="temperature""#), "{req}");
    }

    #[test]
    fn wav_round_trips() {
        let audio = sample_audio();
        let bytes = encode_wav(&audio).unwrap();
        let mut reader = hound::WavReader::new(std::io::Cursor::new(bytes)).unwrap();
        assert_eq!(reader.spec().channels, 1);
        assert_eq!(reader.spec().sample_rate, 16_000);
        assert_eq!(reader.spec().bits_per_sample, 16);
        let decoded: Vec<i16> = reader.samples::<i16>().map(Result::unwrap).collect();
        assert_eq!(decoded, audio.pcm.to_vec());
    }

    #[tokio::test]
    async fn success_yields_one_final_event() {
        let (url, server) = spawn_server(json_ok(r#"{"text":"hey jarvis what time is it"}"#)).await;
        let t = HttpTranscriber::new(params(url), Arc::new(SttStats::default())).unwrap();

        let mut s = t.transcribe(sample_audio());
        let first = s.next().await.unwrap();
        let event = first.unwrap();
        assert_eq!(event.text, "hey jarvis what time is it");
        assert!(event.is_final);
        assert!(
            s.next().await.is_none(),
            "stream ends after the final event"
        );

        // The captured request is the shape speaches expects.
        let req = server.await.unwrap();
        let req_text = String::from_utf8_lossy(&req);
        assert!(req_text.starts_with("POST /v1/audio/transcriptions"));
        assert!(
            req_text
                .to_ascii_lowercase()
                .contains("multipart/form-data")
        );
        assert!(req_text.contains(r#"name="model""#));
        assert!(req_text.contains("faster-whisper-small"));
        assert!(req_text.contains(r#"name="language""#));
        assert!(
            find_subslice(&req, b"RIFF").is_some(),
            "carries the WAV bytes"
        );
    }

    #[tokio::test]
    async fn language_omitted_when_absent() {
        let (url, server) = spawn_server(json_ok(r#"{"text":"ok"}"#)).await;
        let mut p = params(url);
        p.language = None;
        let t = HttpTranscriber::new(p, Arc::new(SttStats::default())).unwrap();

        let _ = t.transcribe(sample_audio()).next().await.unwrap().unwrap();
        let req = String::from_utf8_lossy(&server.await.unwrap()).into_owned();
        assert!(!req.contains(r#"name="language""#));
    }

    #[tokio::test]
    async fn non_2xx_is_status_error() {
        let (url, _server) = spawn_server(Behavior::Status(500, "boom".into())).await;
        let stats = Arc::new(SttStats::default());
        let t = HttpTranscriber::new(params(url), Arc::clone(&stats)).unwrap();

        let err = t
            .transcribe(sample_audio())
            .next()
            .await
            .unwrap()
            .unwrap_err();
        match err {
            TranscribeError::Status { code, body } => {
                assert_eq!(code, 500);
                assert_eq!(body, "boom");
            }
            other => panic!("expected Status, got {other:?}"),
        }
        // A non-timeout error lands in `failed`, not `timeouts`.
        assert_eq!(
            stats.snapshot(),
            SttStatsSnapshot {
                requests: 1,
                ok: 0,
                failed: 1,
                timeouts: 0,
            }
        );
    }

    #[tokio::test]
    async fn garbage_json_is_decode_error() {
        let (url, _server) = spawn_server(json_ok("not json at all{")).await;
        let t = HttpTranscriber::new(params(url), Arc::new(SttStats::default())).unwrap();

        let err = t
            .transcribe(sample_audio())
            .next()
            .await
            .unwrap()
            .unwrap_err();
        assert!(matches!(err, TranscribeError::Decode(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn oversized_content_length_is_rejected() {
        // 200 OK claiming a gigabyte body; the stage must reject on the header,
        // never attempt to buffer it. The tiny actual body is never read.
        let raw =
            b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nContent-Length: 1073741824\r\nConnection: close\r\n\r\n{}"
                .to_vec();
        let (url, _server) = spawn_server(Behavior::Raw(raw)).await;
        let t = HttpTranscriber::new(params(url), Arc::new(SttStats::default())).unwrap();

        let err = t
            .transcribe(sample_audio())
            .next()
            .await
            .unwrap()
            .unwrap_err();
        assert!(matches!(err, TranscribeError::Decode(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn unbounded_body_without_content_length_is_rejected() {
        // 200 OK with no Content-Length (the precheck's blind spot): the server
        // streams past the cap and closes. The running byte ceiling must reject it
        // before it buffers unbounded.
        let mut raw =
            b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\n\r\n"
                .to_vec();
        raw.resize(raw.len() + MAX_BODY_BYTES as usize + 1024, b' ');
        let (url, _server) = spawn_server(Behavior::Raw(raw)).await;
        let t = HttpTranscriber::new(params(url), Arc::new(SttStats::default())).unwrap();

        let err = t
            .transcribe(sample_audio())
            .next()
            .await
            .unwrap()
            .unwrap_err();
        assert!(matches!(err, TranscribeError::Decode(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn connect_refused_is_connect_error() {
        // Bind then drop the listener so the port is dead but was never
        // accepting — the "container not up yet" case §3 names.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        drop(listener);
        let t = HttpTranscriber::new(
            params(format!("http://{addr}")),
            Arc::new(SttStats::default()),
        )
        .unwrap();

        let err = t
            .transcribe(sample_audio())
            .next()
            .await
            .unwrap()
            .unwrap_err();
        assert!(matches!(err, TranscribeError::Connect(_)), "got {err:?}");
    }

    #[tokio::test]
    async fn stalled_server_times_out_within_budget() {
        let (url, _server) = spawn_server(Behavior::Stall).await;
        let mut p = params(url);
        p.timeout = Duration::from_millis(200);
        let stats = Arc::new(SttStats::default());
        let t = HttpTranscriber::new(p, Arc::clone(&stats)).unwrap();

        let started = std::time::Instant::now();
        let err = t
            .transcribe(sample_audio())
            .next()
            .await
            .unwrap()
            .unwrap_err();
        assert!(matches!(err, TranscribeError::Timeout), "got {err:?}");
        assert!(
            started.elapsed() < Duration::from_secs(5),
            "timed out on budget"
        );
        assert_eq!(stats.snapshot().timeouts, 1);
    }

    #[tokio::test]
    async fn stats_count_success() {
        let (url, _server) = spawn_server(json_ok(r#"{"text":"hi"}"#)).await;
        let stats = Arc::new(SttStats::default());
        let t = HttpTranscriber::new(params(url), Arc::clone(&stats)).unwrap();

        let _ = t.transcribe(sample_audio()).next().await.unwrap().unwrap();
        let snap = stats.snapshot();
        assert_eq!(snap.requests, 1);
        assert_eq!(snap.ok, 1);
        assert_eq!(snap.failed, 0);
        assert_eq!(snap.timeouts, 0);
    }

    #[test]
    fn unparseable_url_fails_to_build() {
        let err = HttpTranscriber::new(
            params("http://[not a url".into()),
            Arc::new(SttStats::default()),
        )
        .unwrap_err();
        assert!(matches!(err, BuildError::Url { .. }), "got {err:?}");
    }
}
