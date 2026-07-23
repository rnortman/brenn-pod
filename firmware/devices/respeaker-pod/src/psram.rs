//! PSRAM-backed owned buffer for large, CPU-access-only sample storage.
//!
//! The on-module octal PSRAM (enabled in `sdkconfig.defaults` as a CAPS-only pool)
//! is reachable only through `heap_caps_malloc(..., MALLOC_CAP_SPIRAM)`; plain
//! `malloc()` stays on internal RAM. This module wraps that allocation in an owned
//! `[T]` buffer so large CPU-indexed storage lives in PSRAM instead of the starved
//! internal heap, while Wi-Fi, lwIP, DMA descriptors, and task stacks keep their
//! internal RAM. Two users: the capture ring's 64 KB of `i16` samples
//! (`PsramBuf<i16>`) and the inbound playout ring's 64 KB of `u8` bytes
//! (`PsramBuf<u8>`).
//!
//! Only CPU-indexed data belongs here: PSRAM is not DMA-capable in this config.

use core::ops::{Deref, DerefMut};
use esp_idf_svc::sys::{MALLOC_CAP_SPIRAM, heap_caps_calloc, heap_caps_free};

/// Free bytes currently available in the PSRAM (`MALLOC_CAP_SPIRAM`) pool.
///
/// A pure-read query used by the boot init-log lines that report SPIRAM headroom
/// after each large PSRAM allocation. Owns the single SAFETY justification for the
/// FFI query so each call site does not re-argue it.
pub(crate) fn spiram_free_bytes() -> usize {
    // SAFETY: pure-read query of the PSRAM pool's free bytes, no side effects.
    unsafe { esp_idf_svc::sys::heap_caps_get_free_size(MALLOC_CAP_SPIRAM) }
}

/// Element type whose all-zero bit pattern is a valid, initialized value.
///
/// `PsramBuf` exposes `calloc`-zeroed memory as `&[T]`/`&mut [T]` via `from_raw_parts`,
/// which is instant UB for any `T` whose all-zero pattern is not a valid `T` (a niche
/// type — `NonZeroU8`, `bool`, most enums, `char`). Bounding `new_zeroed` on this trait
/// makes that a compile error instead of silent firmware memory corruption. Implement it
/// only for element types you have confirmed are zero-valid.
///
/// # Safety
/// Implementers assert every all-zero bit pattern of `T` is a valid, fully-initialized
/// value of `T`.
pub(crate) unsafe trait ZeroValid {}
// Both are plain integers: all-zero is the value 0.
unsafe impl ZeroValid for u8 {}
unsafe impl ZeroValid for i16 {}

/// An owned `[T]` allocation living in PSRAM, zero-initialized at construction.
///
/// Allocated once at boot, freed on `Drop`. Panics on allocation failure — the same
/// failure-is-fatal posture as the internal-heap `vec![0; ...]` it replaces (which
/// aborts via the global alloc-error handler on OOM), and unreachable with a healthy
/// 8 MB part since `SPIRAM_IGNORE_NOTFOUND=n` aborts boot before this point if PSRAM
/// is missing.
///
/// `T` must have a valid all-zero bit pattern (enforced by the [`ZeroValid`] bound on
/// [`new_zeroed`](Self::new_zeroed), since the buffer is `calloc`-zeroed) and align to at
/// most 4 bytes (enforced by a const assert there, since `heap_caps` guarantees only
/// 4-byte alignment); the primitive `i16`/`u8` element types both satisfy this.
pub(crate) struct PsramBuf<T> {
    ptr: *mut T,
    len: usize,
}

// SAFETY: `PsramBuf<T>` owns a unique heap allocation and hands out references only
// through `&self`/`&mut self`, so Rust's borrow rules govern aliasing exactly as they
// would for `Box<[T]>`. The underlying PSRAM is ordinary CPU-addressable memory. The
// `Send`/`Sync` bounds on `T` mirror `Box<[T]>`'s own auto-trait conditions.
unsafe impl<T: Send> Send for PsramBuf<T> {}
unsafe impl<T: Sync> Sync for PsramBuf<T> {}

impl<T> PsramBuf<T> {
    /// Allocate `len` zeroed `T` elements in PSRAM.
    ///
    /// Uses `heap_caps_calloc` so the buffer is zero-initialized, matching the
    /// `vec![0; len]` semantics it replaces. `heap_caps` returns memory aligned to
    /// at least 4 bytes, enough for `i16`/`u8`.
    ///
    /// Precondition: `len > 0`. The IDF allocator returns NULL for a zero-byte request,
    /// which trips the allocation-failure assertion below; this type only ever holds a
    /// non-empty ring.
    ///
    /// The [`ZeroValid`] bound guarantees the `calloc`-zeroed block is a valid `[T]`, and
    /// the per-instantiation const assert rejects any `T` whose alignment exceeds the
    /// 4-byte `heap_caps` guarantee.
    pub(crate) fn new_zeroed(len: usize) -> Self
    where
        T: ZeroValid,
    {
        const {
            assert!(
                core::mem::align_of::<T>() <= 4,
                "PsramBuf element alignment exceeds the 4-byte heap_caps_calloc guarantee"
            );
        }
        // SAFETY: FFI call into the ESP-IDF heap allocator. `size_of::<T>()` is the
        // element size; MALLOC_CAP_SPIRAM selects the PSRAM pool. The returned pointer
        // is either null (handled below) or a valid, zeroed, suitably-aligned block of
        // `len * size_of::<T>()` bytes owned exclusively by this `PsramBuf`.
        let ptr = unsafe { heap_caps_calloc(len, core::mem::size_of::<T>(), MALLOC_CAP_SPIRAM) }
            as *mut T;
        assert!(
            !ptr.is_null(),
            "PSRAM allocation failed: {len} elements ({} bytes) from MALLOC_CAP_SPIRAM",
            len * core::mem::size_of::<T>()
        );
        Self { ptr, len }
    }
}

impl<T> Deref for PsramBuf<T> {
    type Target = [T];

    fn deref(&self) -> &[T] {
        // SAFETY: `ptr` is non-null (asserted at construction), points to exactly `len`
        // contiguous, initialized `T` values, and stays valid until `Drop`. No other
        // reference to this allocation exists except through `self`.
        unsafe { core::slice::from_raw_parts(self.ptr, self.len) }
    }
}

impl<T> DerefMut for PsramBuf<T> {
    fn deref_mut(&mut self) -> &mut [T] {
        // SAFETY: same invariants as `deref`; `&mut self` guarantees exclusive access,
        // so the returned mutable slice is the sole live reference to the allocation.
        unsafe { core::slice::from_raw_parts_mut(self.ptr, self.len) }
    }
}

impl<T> Drop for PsramBuf<T> {
    fn drop(&mut self) {
        // SAFETY: `ptr` came from `heap_caps_calloc` and has not been freed; freeing it
        // exactly once here matches the single allocation.
        unsafe { heap_caps_free(self.ptr as *mut core::ffi::c_void) };
    }
}
