//! User-LED GPIO: the shared [`LED`] driver singleton and its controllability
//! self-test.
//!
//! [`run_gpio_self_test`] drives GPIO21 High then Low and reads back the pad
//! level to catch stuck-pad faults. The [`LED`] driver is also shared with the
//! background blink task wired up in `main`.

use device_protocol::{Payload, Status};
use esp_idf_svc::hal::gpio::{InputOutput, PinDriver};
use std::sync::Mutex;

use device_protocol::{test_report_fail, test_report_fail_detail, test_report_ok_detail, TestData};

/// Mutex-guarded LED driver (GPIO21, active-LOW).
///
/// Shared between the background blink task and the GPIO self-test handler.
/// Configured as `InputOutput` so `is_high()`/`is_low()` read the actual pad
/// level (not just the output latch), giving the GPIO self-test real fault
/// coverage for stuck-pad conditions.
/// Initialized once in `main` before any thread uses it.
pub(crate) static LED: Mutex<Option<PinDriver<'static, InputOutput>>> = Mutex::new(None);

/// GPIO21 controllability self-test.
///
/// Drives the pin High then Low, reading back the pad level (not just the output
/// latch) via `is_high()`/`is_low()` to catch stuck-pad faults. Does not prove
/// photon emission (no photosensor on board). Pin is left High (LED off) on exit.
pub(crate) fn run_gpio_self_test() -> (Status, Payload) {
    let mut guard = LED
        .lock()
        .unwrap_or_else(|_| panic!("LED mutex poisoned — another thread panicked holding it"));
    let led = match guard.as_mut() {
        Some(d) => d,
        None => return test_report_fail("LED driver not initialized"),
    };

    // Drive HIGH (LED off, active-LOW); read back pad level.
    if let Err(e) = led.set_high() {
        return test_report_fail_detail("set_high error", &e);
    }
    if !led.is_high() {
        // Restore to known-off state before returning.
        let _ = led.set_high();
        return test_report_fail("set_high pad readback != High");
    }

    // Drive LOW (LED on, active-LOW); read back pad level.
    if let Err(e) = led.set_low() {
        let _ = led.set_high(); // restore
        return test_report_fail_detail("set_low error", &e);
    }
    if !led.is_low() {
        let _ = led.set_high(); // restore
        return test_report_fail("set_low pad readback != Low");
    }

    // Restore LED off (active-LOW → High). A failure here would leave the LED
    // stuck on while the test reports PASS.
    if let Err(e) = led.set_high() {
        return test_report_fail_detail("restore set_high error", &e);
    }

    test_report_ok_detail(
        TestData::None,
        format_args!("GPIO21 high+low pad readback correct"),
    )
}
