use serde_json::{json, Value};
use std::io::{BufRead, BufReader, Read, Write};
use std::process::{Child, ChildStdin, Command, ExitStatus, Stdio};
use std::sync::{mpsc, Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const MAX_FRAME_BYTES: usize = 64 * 1024;
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(2);
const EXIT_TIMEOUT: Duration = Duration::from_secs(2);
const DEFAULT_ACTOR_ID: &str = "configured-test-actor";
type StderrCapture = Arc<Mutex<Option<Result<Vec<u8>, String>>>>;

enum StdoutEvent {
    Line(String),
    Eof,
    Error(String),
}

struct Bridge {
    child: Child,
    stdin: Option<ChildStdin>,
    stdout: mpsc::Receiver<StdoutEvent>,
    stdout_thread: Option<JoinHandle<()>>,
    stderr: StderrCapture,
    stderr_thread: Option<JoinHandle<()>>,
}

impl Bridge {
    fn spawn(actor_id: &str) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_encrypted-spaces-bridge"))
            .env("ENCRYPTED_SPACES_ACTOR_ID", actor_id)
            .env(
                "ENCRYPTED_SPACES_SCHEMA_PATH",
                concat!(env!("CARGO_MANIFEST_DIR"), "/../demos/tauri/app_schema.kdl"),
            )
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .expect("spawn bridge");
        let stdin = child.stdin.take().expect("bridge stdin");
        let stdout = child.stdout.take().expect("bridge stdout");
        let stderr = child.stderr.take().expect("bridge stderr");

        let (stdout_sender, stdout_receiver) = mpsc::channel();
        let stdout_thread = thread::spawn(move || {
            let mut reader = BufReader::new(stdout);
            loop {
                let mut line = String::new();
                match reader.read_line(&mut line) {
                    Ok(0) => {
                        let _ = stdout_sender.send(StdoutEvent::Eof);
                        break;
                    }
                    Ok(_) => {
                        if stdout_sender.send(StdoutEvent::Line(line)).is_err() {
                            break;
                        }
                    }
                    Err(error) => {
                        let _ = stdout_sender.send(StdoutEvent::Error(error.to_string()));
                        break;
                    }
                }
            }
        });

        let stderr_bytes = Arc::new(Mutex::new(None));
        let captured_stderr = Arc::clone(&stderr_bytes);
        let stderr_thread = thread::spawn(move || {
            let mut reader = BufReader::new(stderr);
            let mut bytes = Vec::new();
            let result = reader
                .read_to_end(&mut bytes)
                .map(|_| bytes)
                .map_err(|error| error.to_string());
            *captured_stderr.lock().expect("lock stderr capture") = Some(result);
        });

        Self {
            child,
            stdin: Some(stdin),
            stdout: stdout_receiver,
            stdout_thread: Some(stdout_thread),
            stderr: stderr_bytes,
            stderr_thread: Some(stderr_thread),
        }
    }

    fn send_raw(&mut self, frame: &[u8]) {
        let stdin = self.stdin.as_mut().expect("bridge stdin is open");
        stdin.write_all(frame).expect("write bridge frame");
        stdin.flush().expect("flush bridge frame");
    }

    fn send_request(&mut self, request_id: &str, operation: &str, payload: Value) {
        self.send_raw(request(request_id, operation, payload).as_bytes());
    }

    fn receive(&self) -> Value {
        match self.stdout.recv_timeout(RESPONSE_TIMEOUT) {
            Ok(StdoutEvent::Line(line)) => {
                serde_json::from_str(&line).expect("bridge response is JSONL")
            }
            Ok(StdoutEvent::Eof) => panic!("bridge stdout closed before a response"),
            Ok(StdoutEvent::Error(error)) => panic!("bridge stdout read failed: {error}"),
            Err(mpsc::RecvTimeoutError::Timeout) => {
                panic!("bridge did not respond within {RESPONSE_TIMEOUT:?}")
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                panic!("bridge stdout reader disconnected")
            }
        }
    }

    fn expect_stdout_eof(&self) {
        match self.stdout.recv_timeout(RESPONSE_TIMEOUT) {
            Ok(StdoutEvent::Eof) => {}
            Ok(StdoutEvent::Line(_)) => panic!("bridge emitted an unexpected response"),
            Ok(StdoutEvent::Error(error)) => panic!("bridge stdout read failed: {error}"),
            Err(error) => panic!("bridge stdout did not close: {error}"),
        }
    }

    fn close_stdin(&mut self) {
        self.stdin.take();
    }

    fn wait_for_exit(&mut self) -> ExitStatus {
        let deadline = Instant::now() + EXIT_TIMEOUT;
        loop {
            if let Some(status) = self.child.try_wait().expect("poll bridge process") {
                return status;
            }
            assert!(
                Instant::now() < deadline,
                "bridge did not exit within {EXIT_TIMEOUT:?}"
            );
            thread::sleep(Duration::from_millis(10));
        }
    }

    fn finish(mut self) -> (ExitStatus, String) {
        self.close_stdin();
        let status = self.wait_for_exit();
        self.join_readers();
        let stderr = self
            .stderr
            .lock()
            .expect("lock stderr capture")
            .take()
            .expect("stderr capture completed")
            .expect("read bridge stderr");
        let stderr = String::from_utf8(stderr).expect("bridge stderr is UTF-8");
        (status, stderr)
    }

    fn join_readers(&mut self) {
        if let Some(thread) = self.stdout_thread.take() {
            thread.join().expect("join stdout reader");
        }
        if let Some(thread) = self.stderr_thread.take() {
            thread.join().expect("join stderr reader");
        }
    }
}

impl Drop for Bridge {
    fn drop(&mut self) {
        self.stdin.take();
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
        self.join_readers();
    }
}

fn request(request_id: &str, operation: &str, payload: Value) -> String {
    serde_json::to_string(&json!({
        "version": 1,
        "request_id": request_id,
        "operation": operation,
        "payload": payload,
    }))
    .expect("request JSON")
        + "\n"
}

fn exact_size_request(size: usize) -> String {
    let empty = request("exact-max", "table.select", json!({"padding": ""}));
    let padding = "x".repeat(size - (empty.len() - 1));
    let frame = request("exact-max", "table.select", json!({"padding": padding}));
    assert_eq!(frame.len() - 1, size, "frame size excludes newline");
    frame
}

fn assert_future_success(response: &Value, operation: &str) {
    assert_eq!(response["version"], 1, "{operation} response version");
    assert_eq!(response["ok"], true, "{operation} is still RED: {response}");
}

#[test]
fn protocol_hello_reports_process_configured_actor() {
    for actor_id in ["configured-actor-one", "configured-actor-two"] {
        let mut bridge = Bridge::spawn(actor_id);
        bridge.send_request("hello-request", "hello", json!({}));
        let response = bridge.receive();
        assert_future_success(&response, "hello");
        assert_eq!(response["result"]["actor_id"], actor_id);
    }
}

#[test]
fn protocol_version_reports_bridge_contract() {
    let mut bridge = Bridge::spawn(DEFAULT_ACTOR_ID);
    bridge.send_request("version-request", "version", json!({}));
    assert_future_success(&bridge.receive(), "version");
}

#[test]
fn protocol_request_supplied_actor_id_is_rejected() {
    let mut bridge = Bridge::spawn(DEFAULT_ACTOR_ID);
    let frame = serde_json::to_string(&json!({
        "version": 1,
        "request_id": "actor-in-frame",
        "actor_id": "caller-selected-actor",
        "operation": "table.select",
        "payload": {},
    }))
    .expect("request JSON")
        + "\n";
    bridge.send_raw(frame.as_bytes());

    let response = bridge.receive();
    assert_eq!(response["request_id"], "actor-in-frame");
    assert_eq!(response["ok"], false);
    assert_eq!(response["error"]["code"], "INVALID_REQUEST");
}

#[test]
fn protocol_bare_space_lifecycle_names_are_unknown() {
    for operation in ["create", "join", "snapshot", "restore", "sync"] {
        let mut bridge = Bridge::spawn(DEFAULT_ACTOR_ID);
        bridge.send_request("bare-operation", operation, json!({}));
        let response = bridge.receive();
        assert_eq!(
            response["ok"], false,
            "bare operation accepted: {operation}"
        );
        assert_eq!(response["error"]["code"], "UNKNOWN_OPERATION");
    }
}

#[test]
fn protocol_malformed_frame_has_stable_error() {
    let mut bridge = Bridge::spawn(DEFAULT_ACTOR_ID);
    bridge.send_raw(b"not-json\n");
    let response = bridge.receive();
    assert_eq!(response["ok"], false);
    assert_eq!(response["error"]["code"], "INVALID_JSON");
    assert_eq!(response["error"]["message"], "malformed JSONL frame");
}

#[test]
fn protocol_accepts_a_frame_at_the_exact_limit() {
    let mut bridge = Bridge::spawn(DEFAULT_ACTOR_ID);
    bridge.send_raw(exact_size_request(MAX_FRAME_BYTES).as_bytes());
    let response = bridge.receive();
    assert_eq!(response["request_id"], "exact-max");
    assert_eq!(response["error"]["code"], "NOT_IMPLEMENTED");
}

#[test]
fn protocol_oversize_error_is_prompt_while_stdin_remains_open() {
    let mut bridge = Bridge::spawn(DEFAULT_ACTOR_ID);
    bridge.send_raw(&vec![b'x'; MAX_FRAME_BYTES + 1]);

    let response = bridge.receive();
    assert_eq!(response["ok"], false);
    assert_eq!(response["error"]["code"], "FRAME_TOO_LARGE");
    assert_eq!(
        response["error"]["message"],
        "JSONL frame exceeds maximum size"
    );
}

#[test]
fn protocol_process_terminates_after_oversize_without_draining_stdin() {
    let mut bridge = Bridge::spawn(DEFAULT_ACTOR_ID);
    bridge.send_raw(&vec![b'x'; MAX_FRAME_BYTES + 1]);
    assert_eq!(bridge.receive()["error"]["code"], "FRAME_TOO_LARGE");

    let status = bridge.wait_for_exit();
    assert!(status.success(), "bridge exited with {status}");
}

#[test]
fn protocol_recovers_in_malformed_invalid_valid_order() {
    let mut bridge = Bridge::spawn(DEFAULT_ACTOR_ID);
    bridge.send_raw(b"not-json\n");
    let invalid = serde_json::to_string(&json!({
        "version": 2,
        "request_id": "invalid-request",
        "operation": "table.select",
        "payload": {},
    }))
    .expect("request JSON")
        + "\n";
    bridge.send_raw(invalid.as_bytes());
    bridge.send_request("valid-request", "table.select", json!({}));

    let malformed = bridge.receive();
    let invalid = bridge.receive();
    let valid = bridge.receive();
    assert_eq!(malformed["error"]["code"], "INVALID_JSON");
    assert_eq!(invalid["request_id"], "invalid-request");
    assert_eq!(invalid["error"]["code"], "INVALID_REQUEST");
    assert_eq!(valid["request_id"], "valid-request");
    assert_eq!(valid["error"]["code"], "NOT_IMPLEMENTED");
}

#[test]
fn protocol_validation_error_preserves_request_id() {
    let mut bridge = Bridge::spawn(DEFAULT_ACTOR_ID);
    let frame = serde_json::to_string(&json!({
        "version": 2,
        "request_id": "unsupported-version",
        "operation": "table.select",
        "payload": {},
    }))
    .expect("request JSON")
        + "\n";
    bridge.send_raw(frame.as_bytes());

    let response = bridge.receive();
    assert_eq!(response["request_id"], "unsupported-version");
    assert_eq!(response["error"]["code"], "INVALID_REQUEST");
}

#[test]
fn protocol_invalid_oversized_request_id_is_not_reflected() {
    let mut bridge = Bridge::spawn(DEFAULT_ACTOR_ID);
    let oversized_request_id = "x".repeat(257);
    let frame = serde_json::to_string(&json!({
        "version": 1,
        "request_id": oversized_request_id,
        "operation": "table.select",
        "payload": {},
    }))
    .expect("request JSON")
        + "\n";
    bridge.send_raw(frame.as_bytes());

    let response = bridge.receive();
    assert_eq!(response["request_id"], Value::Null);
    assert_eq!(response["error"]["code"], "INVALID_REQUEST");
}

#[test]
fn protocol_unknown_operation_is_distinct_from_malformed_json() {
    let mut bridge = Bridge::spawn(DEFAULT_ACTOR_ID);
    bridge.send_request("unknown-request", "space.unknown", json!({}));
    bridge.send_raw(b"{not-json}\n");

    let unknown = bridge.receive();
    let malformed = bridge.receive();
    assert_eq!(unknown["request_id"], "unknown-request");
    assert_eq!(unknown["error"]["code"], "UNKNOWN_OPERATION");
    assert_eq!(malformed["error"]["code"], "INVALID_JSON");
}

#[test]
fn protocol_cancellation_interrupts_sync_and_keeps_process_usable_is_red() {
    let mut bridge = Bridge::spawn(DEFAULT_ACTOR_ID);
    bridge.send_request("hello-before-cancel", "hello", json!({}));
    assert_future_success(&bridge.receive(), "hello");

    bridge.send_request(
        "sync-to-cancel",
        "space.sync",
        json!({"space_id": "cancel-contract-space", "wait_for_change_ms": 30_000}),
    );
    bridge.send_request(
        "cancel-request",
        "cancel",
        json!({"request_id": "sync-to-cancel"}),
    );

    let canceled = bridge.receive();
    assert_eq!(canceled["request_id"], "sync-to-cancel");
    assert_eq!(canceled["ok"], false);
    assert_eq!(canceled["error"]["code"], "CANCELED");

    let cancel_ack = bridge.receive();
    assert_eq!(cancel_ack["request_id"], "cancel-request");
    assert_eq!(cancel_ack["ok"], true);

    bridge.send_request("version-after-cancel", "version", json!({}));
    assert_future_success(&bridge.receive(), "version after cancel");
}

#[test]
fn protocol_validation_errors_redact_secret_material() {
    let secret = "request-secret-value";
    let mut bridge = Bridge::spawn(DEFAULT_ACTOR_ID);
    let frame = serde_json::to_string(&json!({
        "version": 2,
        "request_id": "secret-validation",
        "operation": "table.insert",
        "payload": {"value": "value-parametric", "secret": secret},
    }))
    .expect("request JSON")
        + "\n";
    bridge.send_raw(frame.as_bytes());

    let response = bridge.receive();
    assert_eq!(response["error"]["code"], "INVALID_REQUEST");
    let serialized = response.to_string();
    assert!(
        !serialized.contains(secret),
        "secret leaked in bridge response"
    );
    let (status, stderr) = bridge.finish();
    assert!(status.success(), "bridge exited with {status}");
    assert!(!stderr.contains(secret), "secret leaked in bridge stderr");
}

#[test]
fn protocol_runtime_sdk_errors_are_deterministic_and_redacted_is_red() {
    let secrets = ["runtime-secret-alpha", "runtime-secret-beta"];
    let mut bridge = Bridge::spawn(DEFAULT_ACTOR_ID);

    for (index, secret) in secrets.iter().enumerate() {
        bridge.send_request(
            &format!("restore-secret-{index}"),
            "space.restore",
            json!({"snapshot": {"secret": secret}}),
        );
    }

    let responses = [bridge.receive(), bridge.receive()];
    for (index, response) in responses.iter().enumerate() {
        assert_eq!(response["request_id"], format!("restore-secret-{index}"));
        assert_eq!(response["ok"], false);
        assert_eq!(response["error"]["code"], "SDK_ERROR");
        assert_eq!(
            response["error"]["message"],
            "encrypted spaces operation failed"
        );
        for secret in secrets {
            assert!(
                !response.to_string().contains(secret),
                "secret leaked in runtime response"
            );
        }
    }
    assert_eq!(responses[0]["error"], responses[1]["error"]);

    let (status, stderr) = bridge.finish();
    assert!(status.success(), "bridge exited with {status}");
    for secret in secrets {
        assert!(!stderr.contains(secret), "secret leaked in bridge stderr");
    }
}

#[test]
fn protocol_close_is_red() {
    let mut bridge = Bridge::spawn(DEFAULT_ACTOR_ID);
    bridge.send_request("close-request", "close", json!({}));
    assert_future_success(&bridge.receive(), "close");
}

#[test]
fn protocol_shutdown_is_red() {
    let mut bridge = Bridge::spawn(DEFAULT_ACTOR_ID);
    bridge.send_request("shutdown-request", "shutdown", json!({}));
    assert_future_success(&bridge.receive(), "shutdown");
}

#[test]
fn protocol_process_exits_after_stdin_eof() {
    let mut bridge = Bridge::spawn(DEFAULT_ACTOR_ID);
    bridge.close_stdin();
    let status = bridge.wait_for_exit();
    assert!(status.success(), "bridge exited with {status}");
    bridge.expect_stdout_eof();
}
