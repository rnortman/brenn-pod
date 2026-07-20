//! Failed-allocation probe: an alloc-free hook registered with the ESP-IDF heap
//! allocator that fires in the failing allocator's own context. It is the
//! event-driven counterpart to the periodic heap waypoints — it sees the exact
//! instant an allocation cannot be satisfied, the moment coarse periodic sampling
//! can slip past.
//!
//! Hard constraint: the hook runs *inside* a failing allocation and MUST NOT
//! allocate, lock, or re-enter the allocator. It uses `ets_printf` (writes the ROM
//! UART directly — no heap, no FreeRTOS lock) for the first few failures and a
//! lock-free atomic counter thereafter. It deliberately bypasses the COBS
//! `FramedLogger`: that path allocates and takes a mutex, neither safe here. Near
//! true exhaustion the allocator can fail thousands of times per second, so past a
//! small detail budget the hook does nothing but bump the counter.

use core::ffi::c_char;
use core::sync::atomic::{AtomicBool, AtomicU32, Ordering};

/// Total allocation failures observed since boot. Read cheaply from any context.
static ALLOC_FAIL_COUNT: AtomicU32 = AtomicU32::new(0);

/// While set, [`failed_alloc_hook`] withholds its per-failure detail line (it still
/// bumps the counter). The `PsramIdentity` malloc-stays-internal probe deliberately
/// forces a guaranteed allocation failure; its raw `ets_printf` detail line would
/// otherwise interleave with the mutex-guarded COBS frame stream and corrupt the very
/// response frame that test is about to send.
static DETAIL_SUPPRESSED: AtomicBool = AtomicBool::new(false);

/// RAII guard that suppresses [`failed_alloc_hook`]'s detail line for its lifetime.
/// The failure counter still increments; only the raw console write is withheld, so an
/// intentional probe failure cannot clobber a concurrent framed log line. Scope it as
/// tightly as possible — a concurrent thread's failure during the window is also
/// suppressed (still counted).
pub(crate) struct DetailSuppressed;

impl DetailSuppressed {
    pub(crate) fn new() -> Self {
        DETAIL_SUPPRESSED.store(true, Ordering::Relaxed);
        Self
    }
}

impl Drop for DetailSuppressed {
    fn drop(&mut self) {
        DETAIL_SUPPRESSED.store(false, Ordering::Relaxed);
    }
}

/// Leading failures printed in full before the hook falls silent (past this it only
/// bumps the counter). Detailed output on every failure during an OOM storm would
/// flood the UART and add per-failure cost the hook must not carry.
const DETAIL_LIMIT: u32 = 8;

/// Current allocation-failure count. Lock-free read for instrumentation sites.
pub(crate) fn alloc_fail_count() -> u32 {
    ALLOC_FAIL_COUNT.load(Ordering::Relaxed)
}

/// Remove the single deliberate allocation failure the `PsramIdentity` probe just caused from
/// [`ALLOC_FAIL_COUNT`]. The probe's guaranteed-failing 1 MiB `malloc` fires
/// [`failed_alloc_hook`], which has already counted it by the time `malloc` returns null; this
/// subtracts exactly that one known contribution so the counter stays a clean tripwire for
/// *real* failures (which a zero-count ship gate scores on).
///
/// Subtracting the single known failure — rather than gating the increment under a flag or
/// snapshotting and restoring the whole counter — is deliberate: both of those would also erase
/// a concurrent thread's genuine failure recorded during the probe's malloc window. Here a
/// concurrent real failure is never masked; only our own deterministic one is removed. Call
/// exactly once, and only when the deliberate allocation actually failed (so the count is
/// already ≥ 1, and the `fetch_sub` cannot underflow).
pub(crate) fn discount_deliberate_failure() {
    ALLOC_FAIL_COUNT.fetch_sub(1, Ordering::Relaxed);
}

/// ESP-IDF failed-allocation hook, invoked in the failing allocator's context.
///
/// SAFETY: must not allocate, lock, or re-enter the allocator. `ets_printf` writes
/// the ROM UART directly (no heap, no lock); the counter is a lock-free atomic.
/// `function_name` is a static C string supplied by the caller (or null); it is only
/// passed through to `ets_printf`, never dereferenced here.
unsafe extern "C" fn failed_alloc_hook(size: usize, caps: u32, function_name: *const c_char) {
    // fetch_add returns the prior value; the first failure sees 0.
    let prior = ALLOC_FAIL_COUNT.fetch_add(1, Ordering::Relaxed);
    if prior < DETAIL_LIMIT && !DETAIL_SUPPRESSED.load(Ordering::Relaxed) {
        let name = if function_name.is_null() {
            c"<null>".as_ptr()
        } else {
            function_name
        };
        unsafe {
            esp_idf_svc::sys::ets_printf(
                c"[allocfail] n=%u size=%u caps=0x%x fn=%s\n".as_ptr(),
                prior + 1,
                size as u32,
                caps,
                name,
            );
        }
    }
}

/// Register [`failed_alloc_hook`] with the ESP-IDF heap allocator. Call once during
/// device init, before any streaming starts.
pub(crate) fn register() {
    // SAFETY: one-time FFI registration of a static hook fn at init.
    let err = unsafe {
        esp_idf_svc::sys::heap_caps_register_failed_alloc_callback(Some(failed_alloc_hook))
    };
    if err != esp_idf_svc::sys::ESP_OK {
        // Runs before the COBS logger is installed (register() is deliberately first
        // in main), so report via ets_printf like the hook itself. A silent failure
        // here would leave every later alloc_fail=0 reading indistinguishable from a
        // hook that never armed.
        unsafe {
            esp_idf_svc::sys::ets_printf(
                c"[allocfail] hook registration FAILED err=%d\n".as_ptr(),
                err,
            );
        }
    }
}
