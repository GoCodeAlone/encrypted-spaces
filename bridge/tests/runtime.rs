use serde_json::{json, Value};
use std::io::Write;
use std::process::{Command, Stdio};

fn invoke(operation: &str, actor: &str, payload: Value) -> Value {
    let frame = serde_json::to_string(&json!({
        "version": 1,
        "request_id": format!("runtime-{operation}"),
        "actor_id": actor,
        "operation": operation,
        "payload": payload,
    }))
    .expect("request JSON")
        + "\n";
    let mut child = Command::new(env!("CARGO_BIN_EXE_encrypted-spaces-bridge"))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .spawn()
        .expect("spawn bridge");
    child
        .stdin
        .take()
        .expect("bridge stdin")
        .write_all(frame.as_bytes())
        .expect("write bridge frame");
    let output = child.wait_with_output().expect("wait for bridge");
    assert!(output.status.success(), "bridge exited: {output:?}");
    serde_json::from_slice(
        output
            .stdout
            .split(|byte| *byte == b'\n')
            .next()
            .expect("response"),
    )
    .expect("bridge response JSON")
}

fn assert_future_success(response: &Value, operation: &str) {
    assert_eq!(response["version"], 1, "{operation} response version");
    assert_eq!(response["ok"], true, "{operation} is still RED: {response}");
}

fn red_operation(operation: &str) {
    let actor = format!("actor-{operation}");
    let response = invoke(
        operation,
        &actor,
        json!({"key": format!("key-{operation}"), "value": format!("value-{operation}")}),
    );
    assert_future_success(&response, operation);
}

#[test]
fn runtime_space_lifecycle_is_red() {
    for operation in [
        "space.create",
        "space.join",
        "space.snapshot",
        "space.restore",
        "space.sync",
    ] {
        red_operation(operation);
    }
}

#[test]
fn runtime_table_insert_select_are_red() {
    for operation in ["table.insert", "table.select"] {
        red_operation(operation);
    }
}

#[test]
fn runtime_list_create_append_read_are_red() {
    for operation in ["list.create", "list.append", "list.read"] {
        red_operation(operation);
    }
}

#[test]
fn runtime_text_create_edit_read_are_red() {
    for operation in ["text.create", "text.edit", "text.read"] {
        red_operation(operation);
    }
}

#[test]
fn runtime_file_put_get_are_red() {
    for operation in ["file.put", "file.get"] {
        red_operation(operation);
    }
}

#[test]
fn runtime_member_invite_join_remove_are_red() {
    for operation in ["member.invite", "member.join", "member.remove"] {
        red_operation(operation);
    }
}

#[test]
fn release_contract_is_red_until_dist_and_notice_exist() {
    let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    let workflow = std::fs::read_to_string(root.join(".github/workflows/release-bridge.yml"))
        .expect("release contract workflow");
    let matrix =
        std::fs::read_to_string(root.join(".github/scripts/ci_matrix.json")).expect("CI matrix");
    let patches = std::fs::read_to_string(root.join("PATCHES.md")).expect("PATCHES ledger");
    let cargo = std::fs::read_to_string(root.join("Cargo.toml")).expect("workspace manifest");
    let lock = std::fs::read_to_string(root.join("Cargo.lock")).expect("workspace lockfile");

    for asset in ["backend", "bridge"] {
        assert!(workflow.contains(asset), "workflow omits {asset} asset");
    }
    for target in ["linux-amd64", "linux-arm64", "macos-amd64", "macos-arm64"] {
        assert!(workflow.contains(target), "workflow omits {target}");
    }
    for archive in [
        "encrypted-spaces-backend-linux-amd64.tar.gz",
        "encrypted-spaces-backend-linux-arm64.tar.gz",
        "encrypted-spaces-backend-macos-amd64.tar.gz",
        "encrypted-spaces-backend-macos-arm64.tar.gz",
        "encrypted-spaces-bridge-linux-amd64.tar.gz",
        "encrypted-spaces-bridge-linux-arm64.tar.gz",
        "encrypted-spaces-bridge-macos-amd64.tar.gz",
        "encrypted-spaces-bridge-macos-arm64.tar.gz",
    ] {
        assert!(workflow.contains(archive), "workflow omits {archive}");
    }
    for marker in [
        "DIST_CONTRACT: dist",
        "actions/checkout@11bd71901bbe5b1630ceea73d27597364c9af683",
        "test -s \"$manifest\"",
        "test -s \"$archive\"",
        "test -s \"$checksum\"",
        "test -s \"$provenance\"",
        "test -s \"$license\"",
        "test -s \"$notice\"",
        "sha256sum",
        "provenance",
        "LICENSE",
        "NOTICE",
        "4cda0ae",
        "1.94.1",
        "RISC0_SKIP_BUILD: 1",
    ] {
        assert!(
            workflow.contains(marker),
            "workflow omits release marker {marker}"
        );
    }
    assert!(!workflow.contains("if: ${{ false }}"));
    assert!(!workflow.contains("echo \"${{ matrix.component }}-${{ matrix.target }}\""));
    assert!(!workflow.contains("Future asset contract"));
    assert!(workflow.contains("release_ready: false"));
    assert!(workflow.contains("does not publish"));
    assert!(matrix.contains("encrypted-spaces-bridge"));
    assert!(cargo.contains("kdl = { version = \"=6.5.0\""));
    assert!(lock.contains("name = \"kdl\"\nversion = \"6.5.0\""));
    assert!(patches.contains("4cda0ae"));
    assert!(patches.contains("800495f"));
    assert!(patches.contains("NOT_IMPLEMENTED"));

    let mut missing = vec!["NOTICE".to_owned()];
    for asset in [
        "encrypted-spaces-backend-linux-amd64.tar.gz",
        "encrypted-spaces-backend-linux-arm64.tar.gz",
        "encrypted-spaces-backend-macos-amd64.tar.gz",
        "encrypted-spaces-backend-macos-arm64.tar.gz",
        "encrypted-spaces-bridge-linux-amd64.tar.gz",
        "encrypted-spaces-bridge-linux-arm64.tar.gz",
        "encrypted-spaces-bridge-macos-amd64.tar.gz",
        "encrypted-spaces-bridge-macos-arm64.tar.gz",
    ] {
        let path = root.join("dist").join(asset);
        if !path.is_file() || std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0) == 0 {
            missing.push(format!("dist/{asset}"));
        }
    }
    assert!(
        missing.len() == 0,
        "release contract RED: missing NOTICE/dist release assets: {}",
        missing.join(", ")
    );
}
