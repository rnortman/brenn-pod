fn main() {
    // The embuild/esp-idf-sys work only applies to the device target (it compiles the
    // ESP-IDF C SDK for Xtensa). Host builds get a no-op build script so the crate can be
    // compiled and tested on the host triple. Device builds are byte-identical in behavior.
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("espidf") {
        embuild::espidf::sysenv::output();
    }
}
