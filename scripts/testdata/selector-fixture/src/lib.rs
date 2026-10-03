#[cfg(test)]
mod tests {
    use std::{env, fs};

    #[test]
    fn selector_known_writes_marker() {
        let marker = env::var_os("SELECTOR_FIXTURE_MARKER")
            .expect("SELECTOR_FIXTURE_MARKER must be set by the selector contract test");
        fs::write(marker, b"selected\n").expect("write selector marker");
    }

    #[test]
    fn selector_harness_argument_probe() {
        assert!(
            env::args().any(|arg| arg == "--nocapture"),
            "cargo test harness arguments were not forwarded"
        );
    }

    #[test]
    fn unrelated_selector_suffix_collision() {}
}
