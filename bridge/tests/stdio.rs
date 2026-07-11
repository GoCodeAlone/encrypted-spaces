use serde_json::{json, Value};
use std::io::Write;
use std::process::{Command, Stdio};

const MAX_FRAME_BYTES: usize = 64 * 1024;

fn invoke(frame: &str) -> Vec<Value> {
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
    String::from_utf8(output.stdout)
        .expect("bridge stdout is UTF-8")
        .lines()
        .map(|line| serde_json::from_str(line).expect("bridge emits JSONL"))
        .collect()
}

fn request(operation: &str, actor: &str, payload: Value) -> String {
    serde_json::to_string(&json!({
        "version": 1,
        "request_id": format!("request-{operation}"),
        "actor_id": actor,
        "operation": operation,
        "payload": payload,
    }))
    .expect("request JSON")
        + "\n"
}

fn actor(operation: &str) -> String {
    format!("actor-{operation}")
}

fn assert_future_success(response: &Value, operation: &str) {
    assert_eq!(response["version"], 1, "{operation} response version");
    assert_eq!(response["ok"], true, "{operation} is still RED: {response}");
}

#[test]
fn protocol_hello_is_red() {
    let actor = actor("hello");
    let response = &invoke(&request("hello", &actor, json!({})))[0];
    assert_future_success(response, "hello");
}

#[test]
fn protocol_version_is_red() {
    let actor = actor("version");
    let response = &invoke(&request("version", &actor, json!({})))[0];
    assert_future_success(response, "version");
}

#[test]
fn protocol_bare_space_lifecycle_names_are_rejected() {
    for operation in ["create", "join", "snapshot", "restore", "sync"] {
        let actor = actor(operation);
        let response = &invoke(&request(operation, &actor, json!({})))[0];
        assert_eq!(
            response["ok"], false,
            "bare operation accepted: {operation}"
        );
        assert_eq!(response["error"]["code"], "INVALID_JSON");
    }
}

#[test]
fn protocol_malformed_frame_has_stable_error() {
    let response = &invoke("not-json\n")[0];
    assert_eq!(response["ok"], false);
    assert_eq!(response["error"]["code"], "INVALID_JSON");
    assert_eq!(response["error"]["message"], "malformed JSONL frame");
}

#[test]
fn protocol_oversized_frame_has_stable_error() {
    let frame = format!("{}\n", "x".repeat(MAX_FRAME_BYTES + 1));
    let response = &invoke(&frame)[0];
    assert_eq!(response["ok"], false);
    assert_eq!(response["error"]["code"], "FRAME_TOO_LARGE");
    assert_eq!(
        response["error"]["message"],
        "JSONL frame exceeds maximum size"
    );
}

#[test]
fn protocol_cancellation_is_red() {
    let actor = actor("cancel");
    let response = &invoke(&request(
        "cancel",
        &actor,
        json!({"request_id": "request-1"}),
    ))[0];
    assert_future_success(response, "cancel");
}

#[test]
fn protocol_errors_redact_secret_material() {
    let secret = "request-secret-value";
    let actor = actor("secret-check");
    let response = &invoke(&request(
        "table.insert",
        &actor,
        json!({"value": "value-parametric", "secret": secret}),
    ))[0];
    let serialized = response.to_string();
    assert!(
        !serialized.contains(secret),
        "secret leaked in response: {serialized}"
    );
    assert_eq!(response["error"]["code"], "NOT_IMPLEMENTED");
}

#[test]
fn protocol_close_is_red() {
    let actor = actor("close");
    let response = &invoke(&request("close", &actor, json!({})))[0];
    assert_future_success(response, "close");
}

#[test]
fn protocol_shutdown_is_red() {
    let actor = actor("shutdown");
    let response = &invoke(&request("shutdown", &actor, json!({})))[0];
    assert_future_success(response, "shutdown");
}

#[test]
fn protocol_process_exits_after_stdin_eof() {
    let responses = invoke("");
    assert!(responses.is_empty(), "EOF should not create a response");
}
