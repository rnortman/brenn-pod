//! Shared `ort` session plumbing for the listener's ONNX models.
//!
//! Both model stacks — [`OwwModels`](super::oww_stream::OwwModels) and
//! [`SileroModel`](super::silero::SileroModel) — load their sessions with the
//! same CPU/threading settings. Those settings must match: every model runs
//! serially on the one shared listener thread, and the single-thread budget math
//! assumes it. Keeping the loader in one place stops the two stacks drifting on
//! exactly the knobs (thread count, optimization level, execution provider) that
//! have to agree.

use std::path::Path;

use ort::session::{builder::GraphOptimizationLevel, Session};

use crate::wake::WakeError;

/// Load one ONNX model into a single-threaded CPU session, mapping any failure to
/// a [`WakeError::Load`] that names the file. Fatal at startup — never a silently
/// degraded model.
pub(crate) fn load_session(path: &Path) -> Result<Session, WakeError> {
    let named = |detail: String| WakeError::Load {
        model: path.display().to_string(),
        detail,
    };
    Session::builder()
        .map_err(|e| named(e.to_string()))?
        .with_optimization_level(GraphOptimizationLevel::Level3)
        .map_err(|e| named(e.to_string()))?
        .with_intra_threads(1)
        .map_err(|e| named(e.to_string()))?
        .commit_from_file(path)
        .map_err(|e| named(e.to_string()))
}
