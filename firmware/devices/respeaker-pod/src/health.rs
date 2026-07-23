//! Device health / resource-sanity self-test.
//!
//! [`run_device_health_check`] reads ESP-IDF runtime health metrics (free heap,
//! lifetime-minimum free heap, task stack high-water marks) and asserts each is
//! above a conservative floor. [`evaluate_health`] is the pure threshold logic,
//! split out so it is host-testable without ESP-IDF FFI.

use device_protocol::{
    MallocProbe, Payload, Status, TestData, evaluate_health, test_report_fail_fmt, test_report_ok,
};

use crate::console::{ENCODE_FAILURES, TX_WRITE_FAILURES, WRITER_STATE_ANOMALIES};

// ── Health check thresholds ───────────────────────────────────────────────────
//
// The main-task heap/stack floors and the pure `evaluate_health` over them live in
// `device_protocol` (host-testable without ESP-IDF FFI). The per-thread supervisor
// and streamer stack floors below stay device-side — they guard `run_device_health_check`,
// which does the FFI reads.

/// Minimum acceptable stack high-water mark for the wifi-supervisor thread (bytes).
/// The thread is spawned with an 8192-byte stack (`spawn_wifi_supervisor_thread`),
/// sized for the `associate_from_active_config()` → `esp_wifi_connect()` call chain (the
/// deepest stack consumer in the firmware). A floor of 512 bytes is a near-overflow
/// alarm: if the HWM ever drops below it, raise `stack_size` in
/// `spawn_wifi_supervisor_thread` rather than lowering this floor.
/// HWM and stack_size are both in bytes on xtensa (`StackType_t = uint8_t`).
const SUPERVISOR_STACK_HWM_FLOOR: u32 = 512;

/// Minimum acceptable stack high-water mark for the streamer thread (bytes).
/// The thread is spawned with a 20480-byte stack (`spawn_streamer_thread`); it hosts the
/// `run_segment` drain loop and its per-frame wire-encode chain — the thread whose
/// previously-unmeasured sizing seeded the RTD heap-churn defect. PROVISIONAL 512-byte
/// near-overflow alarm (mirrors `SUPERVISOR_STACK_HWM_FLOOR`), baked from the first HIL
/// observation. If the HWM ever drops below it, raise `stack_size` in
/// `spawn_streamer_thread` rather than lowering this floor.
const STREAMER_STACK_HWM_FLOOR: u32 = 512;

/// Read the current free heap and the boot-wide minimum-ever free heap as a pair,
/// scoped to **internal** RAM (`MALLOC_CAP_INTERNAL`).
///
/// The single home for the internal-RAM free / minimum-ever pair so instrumentation
/// sites don't each carry their own unsafe block and SAFETY rationale.
///
/// Internal-RAM scope is deliberate. With SPIRAM in the heap under
/// `SPIRAM_USE_CAPS_ALLOC`, the whole-heap `esp_get_free_heap_size` /
/// `esp_get_minimum_free_heap_size` counters fold the 8 MB PSRAM pool into their total,
/// which disarms the internal-RAM tripwires (`HEAP_MIN_EVER_FLOOR`, `RTD_HEAP_LOW_FLOOR`)
/// that were derived from internal-only measurements. Querying `MALLOC_CAP_INTERNAL`
/// restores those floors' original semantics: they guard the internal pool that Wi-Fi,
/// lwIP, DMA descriptors, and task stacks actually draw from. PSRAM free is reported
/// separately (`heap_caps_get_free_size(MALLOC_CAP_SPIRAM)`) where needed.
///
/// SAFETY: both are pure-read ESP-IDF heap-registry queries with no side effects.
pub(crate) fn heap_free_min() -> (u32, u32) {
    unsafe {
        (
            esp_idf_svc::sys::heap_caps_get_free_size(esp_idf_svc::sys::MALLOC_CAP_INTERNAL) as u32,
            esp_idf_svc::sys::heap_caps_get_minimum_free_size(esp_idf_svc::sys::MALLOC_CAP_INTERNAL)
                as u32,
        )
    }
}

/// Read a heap waypoint triple: current free, boot-wide minimum-ever free, and the
/// largest contiguous free block (bytes), all scoped to **internal** RAM
/// (`MALLOC_CAP_INTERNAL`).
///
/// The largest-free-block field separates two failure shapes at a glance: heap
/// exhaustion (free low, largest tracks it down) versus fragmentation (free still
/// adequate but the largest contiguous block has collapsed).
///
/// Internal-RAM scope matches `heap_free_min`: the largest-block query uses
/// `MALLOC_CAP_INTERNAL` so the waypoint reflects the same internal pool the floors
/// guard, not the PSRAM-inflated whole-heap view.
///
/// SAFETY: all three are pure-read ESP-IDF heap-registry queries with no side effects.
pub(crate) fn heap_waypoint() -> (u32, u32, u32) {
    let (free_heap, min_heap) = heap_free_min();
    let largest = unsafe {
        esp_idf_svc::sys::heap_caps_get_largest_free_block(esp_idf_svc::sys::MALLOC_CAP_INTERNAL)
    };
    (free_heap, min_heap, largest as u32)
}

/// Device health / resource-sanity self-test.
///
/// Reads ESP-IDF runtime health metrics and asserts each is above a conservative
/// floor. Passing reports carry the measured values as [`TestData::DeviceHealth`];
/// failing ones name the offending metric in the report detail.
pub(crate) fn run_device_health_check() -> (Status, Payload) {
    let (free_heap, min_heap) = heap_free_min();
    // SAFETY: pure-read FreeRTOS query with no side effects. NULL handle for
    // uxTaskGetStackHighWaterMark queries the calling task (protocol loop main task).
    let stack_hwm: u32 =
        unsafe { esp_idf_svc::sys::uxTaskGetStackHighWaterMark(core::ptr::null_mut()) };
    let tx_write_failures = TX_WRITE_FAILURES.load(std::sync::atomic::Ordering::Relaxed);
    let writer_anomalies = WRITER_STATE_ANOMALIES.load(std::sync::atomic::Ordering::Relaxed);
    let encode_failures = ENCODE_FAILURES.load(std::sync::atomic::Ordering::Relaxed);

    // Check the main-task metrics first; return early on failure.
    if let Some(fail) = evaluate_health(free_heap, min_heap, stack_hwm, tx_write_failures) {
        return fail;
    }

    // Also check the wifi-supervisor thread's stack HWM.  The supervisor is always
    // running by the time DeviceHealthCheck executes; a NULL handle here would mean
    // the thread hasn't started or its name changed — both are bugs, not transients.
    // SAFETY: xTaskGetHandle is a read-only FreeRTOS query; the C string is NUL-terminated.
    let supervisor_task = unsafe { esp_idf_svc::sys::xTaskGetHandle(c"wifi-supervisor".as_ptr()) };
    let supervisor_hwm: u32 = if supervisor_task.is_null() {
        0 // null handle: treat as fully-exhausted stack so the check fires
    } else {
        unsafe { esp_idf_svc::sys::uxTaskGetStackHighWaterMark(supervisor_task) }
    };

    if supervisor_hwm < SUPERVISOR_STACK_HWM_FLOOR {
        return test_report_fail_fmt(format_args!(
            "FAIL supervisor_hwm={supervisor_hwm}<{SUPERVISOR_STACK_HWM_FLOOR} heap_free={free_heap} min_heap={min_heap} stack_hwm={stack_hwm} tx_write_failures={tx_write_failures}"
        ));
    }

    // Also check the streamer thread's stack HWM — the thread that hosts the run_segment
    // drain loop and its per-frame wire-encode chain, spawned unconditionally at boot. A null
    // handle means the thread never started or was renamed — a bug, not a transient — so the
    // check fires (HWM 0).
    // SAFETY: xTaskGetHandle is a read-only FreeRTOS query; the C string is NUL-terminated.
    let streamer_task = unsafe { esp_idf_svc::sys::xTaskGetHandle(c"streamer".as_ptr()) };
    let streamer_hwm: u32 = if streamer_task.is_null() {
        0
    } else {
        unsafe { esp_idf_svc::sys::uxTaskGetStackHighWaterMark(streamer_task) }
    };

    if streamer_hwm < STREAMER_STACK_HWM_FLOOR {
        return test_report_fail_fmt(format_args!(
            "FAIL streamer_hwm={streamer_hwm}<{STREAMER_STACK_HWM_FLOOR} heap_free={free_heap} min_heap={min_heap} stack_hwm={stack_hwm} supervisor_hwm={supervisor_hwm} tx_write_failures={tx_write_failures}"
        ));
    }

    // The WRITER state-once gate is never re-cleared in program order, so any observed anomaly
    // is an external modification of the discriminant byte (memory corruption). Unlike
    // tx_write_failures (environmental), a non-zero here is always an invariant violation — fail
    // loudly with full logs instead of the device aborting inside the fault-reporting channel.
    if writer_anomalies != 0 {
        return test_report_fail_fmt(format_args!(
            "FAIL writer_anomalies={writer_anomalies} heap_free={free_heap} min_heap={min_heap} stack_hwm={stack_hwm} supervisor_hwm={supervisor_hwm} streamer_hwm={streamer_hwm} encode_failures={encode_failures} tx_write_failures={tx_write_failures}"
        ));
    }

    // Like writer_anomalies, encode_failures is unreachable-by-design (no reachable frame
    // overflows the encode buffer), so any non-zero value is a firmware bug, not an
    // environmental condition — fail loudly rather than surface it format-only.
    if encode_failures != 0 {
        return test_report_fail_fmt(format_args!(
            "FAIL encode_failures={encode_failures} heap_free={free_heap} min_heap={min_heap} stack_hwm={stack_hwm} supervisor_hwm={supervisor_hwm} streamer_hwm={streamer_hwm} writer_anomalies={writer_anomalies} tx_write_failures={tx_write_failures}"
        ));
    }

    test_report_ok(TestData::DeviceHealth {
        heap_free: free_heap,
        min_heap,
        stack_hwm,
        supervisor_hwm,
        streamer_hwm,
        writer_anomalies,
        encode_failures,
        tx_write_failures,
    })
}

/// Vendor-documented PSRAM size for the XIAO ESP32-S3 R8 module: 8 MiB octal SPI PSRAM.
///
/// Asserted, not measured — the first hardware run of `run_psram_identity` is a gate; a
/// differing observed size is an unexpected reading for human review, never a silent
/// rebake of this constant.
const PSRAM_EXPECTED_SIZE_BYTES: usize = 8 * 1024 * 1024;

/// Probe size for the malloc-stays-internal assertion (1 MiB).
///
/// Chosen larger than the ESP32-S3's entire internal SRAM (~512 KiB) so the request can
/// never be satisfied from internal RAM, yet far below free PSRAM (~8 MB) so PSRAM *could*
/// physically hold it. Under `SPIRAM_USE_CAPS_ALLOC` a libc `malloc()` of this size must
/// therefore either fail (null) or resolve internal — never spill to PSRAM. The probe uses
/// fallible `malloc` (null on failure, never an aborting `Vec`), so a failed probe returns
/// null rather than aborting the device.
const MALLOC_INTERNAL_PROBE_BYTES: usize = 1024 * 1024;

/// PSRAM presence + identity self-test (`TestName::PsramIdentity`).
///
/// Asserts three things, reported as distinct tokens so a failure classifies itself:
/// - presence: the on-module octal PSRAM initialized (`esp_psram_is_initialized()`);
/// - identity: its size matches the vendor-documented 8 MiB (`esp_psram_get_size()`);
/// - allocator-stays-internal: a libc `malloc()` too large for internal RAM must NOT land
///   in the external PSRAM address range. This drives the exact path production Rust code
///   uses — the global allocator on this `std` target lowers to libc `malloc`/`aligned_alloc`.
///   IDF v5.5.4 source facts (why the cap choice matters): the PSRAM region is registered
///   carrying `MALLOC_CAP_DEFAULT` (`esp_psram.c:470,480`), so a bare
///   `heap_caps_malloc(size, MALLOC_CAP_DEFAULT)` request *may legitimately* resolve to
///   PSRAM — that is the wrong path to probe. But libc `malloc`/`calloc`/`realloc` route
///   through the `heap_caps_*_default` wrappers, which under `SPIRAM_USE_CAPS_ALLOC` add
///   `MALLOC_CAP_INTERNAL` to every request (`heap_caps.c` `heap_caps_malloc_default`),
///   keeping production allocations internal-only. An external pointer here means that
///   internal-only guarantee has broken — silently exposing Wi-Fi/lwIP/DMA/stacks to PSRAM
///   latency. The probe either fails (null — internal RAM too small, acceptable) or returns
///   an internal pointer; an external pointer FAILs.
///
/// At 1 MiB against ~512 KiB of internal SRAM the correct-config outcome is a *guaranteed*
/// allocation failure (null). On this build a failing allocation's only side effect is the
/// registered failed-alloc hook (`alloc_probe.rs`), whose raw `ets_printf` would clobber the
/// COBS frame stream carrying this test's own response — so the probe holds an
/// `alloc_probe::DetailSuppressed` guard across the allocation to withhold that raw line, and on
/// the guaranteed null result calls `alloc_probe::discount_deliberate_failure` to remove its own
/// one hook-counted failure so the `alloc_fail` tripwire stays zero for real failures. Without
/// the guard the deliberate failure corrupts the response frame and the host sees the device as
/// unresponsive.
///
/// The free-SPIRAM byte count is reported for observability, not asserted (the capture
/// ring already lives in this pool by the time the test runs, so free < total is expected).
///
/// PASS data: [`TestData::PsramIdentity`] with `init=true`, `size` = 8 MiB, and
/// `malloc_probe` ∈ {`Null`, `Internal`}.
pub(crate) fn run_psram_identity() -> (Status, Payload) {
    // SAFETY: all three are pure-read ESP-IDF queries with no side effects.
    let (initialized, size, spiram_free) = unsafe {
        (
            esp_idf_svc::sys::esp_psram_is_initialized(),
            esp_idf_svc::sys::esp_psram_get_size(),
            esp_idf_svc::sys::heap_caps_get_free_size(esp_idf_svc::sys::MALLOC_CAP_SPIRAM),
        )
    };
    let init = u8::from(initialized);

    if !initialized {
        return test_report_fail_fmt(format_args!(
            "FAIL src=psram init={init} size={size} spiram_free={spiram_free}"
        ));
    }
    if size != PSRAM_EXPECTED_SIZE_BYTES {
        return test_report_fail_fmt(format_args!(
            "FAIL src=psram init={init} size={size} expected={PSRAM_EXPECTED_SIZE_BYTES} spiram_free={spiram_free}"
        ));
    }

    // Allocator-stays-internal probe. A libc malloc() (the Rust global allocator's lowering)
    // larger than internal RAM must fail or resolve internal; an external pointer is a spill.
    // The 1 MiB request is guaranteed to fail on a correct device, so the alloc-fail detail
    // hook is suppressed across the call — its raw ets_printf would otherwise corrupt the COBS
    // response frame. The guard drops (re-enabling detail) as soon as the block resolves.
    // SAFETY: malloc is fallible (returns null on failure, never aborts); esp_ptr_external_ram
    // is a pure-read address classification; free releases exactly the block just allocated
    // (freeing null is a no-op, but the null branch skips it).
    let (spilled, external) = {
        let _detail_guard = crate::alloc_probe::DetailSuppressed::new();
        unsafe {
            let ptr = esp_idf_svc::sys::malloc(MALLOC_INTERNAL_PROBE_BYTES as ::core::ffi::c_uint);
            if ptr.is_null() {
                // The 1 MiB request is guaranteed to fail on a correct device; the failed-alloc
                // hook has already counted it. Remove exactly that one deliberate failure so the
                // alloc_fail tripwire stays zero for real allocation failures.
                crate::alloc_probe::discount_deliberate_failure();
                (false, false)
            } else {
                let external = esp_idf_svc::sys::esp_ptr_external_ram(ptr as *const _);
                esp_idf_svc::sys::free(ptr);
                (true, external)
            }
        }
    };
    let malloc_probe = if !spilled {
        MallocProbe::Null
    } else if external {
        MallocProbe::External
    } else {
        MallocProbe::Internal
    };

    if spilled && external {
        return test_report_fail_fmt(format_args!(
            "FAIL src=psram init={init} size={size} spiram_free={spiram_free} malloc_probe={malloc_probe:?}"
        ));
    }

    test_report_ok(TestData::PsramIdentity {
        init: initialized,
        size: size as u32,
        spiram_free: spiram_free as u32,
        malloc_probe,
    })
}

// Pure-threshold unit tests for `evaluate_health` (and its floor constants) live in
// `device_protocol`, alongside the function; they run host-native there.
