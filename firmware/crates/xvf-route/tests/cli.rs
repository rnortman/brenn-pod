//! The built command's exit status and output on a command line that does not
//! parse. Touches no device.

#[test]
fn a_command_line_that_does_not_parse_exits_2_with_one_json_object() {
    for args in [
        &["set", "--card", "Flex", "--op-all", "1,2,3"][..],
        &["show"][..],
        &[][..],
    ] {
        let out = std::process::Command::new(env!("CARGO_BIN_EXE_xvf-route"))
            .args(args)
            .output()
            .expect("the binary runs");
        assert_eq!(out.status.code(), Some(2), "{args:?}");
        let stdout = String::from_utf8(out.stdout).unwrap();
        assert_eq!(stdout.lines().count(), 1, "{args:?}: {stdout}");
        let line: serde_json::Value = serde_json::from_str(&stdout).expect("JSON");
        assert_eq!(line["outcome"], "usage", "{args:?}");
        assert!(
            String::from_utf8_lossy(&out.stderr).contains("usage: xvf-route"),
            "{}",
            String::from_utf8_lossy(&out.stderr)
        );
    }
}
