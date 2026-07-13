use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fs;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, Command, ExitStatus, Stdio};
use std::sync::{mpsc, OnceLock};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

const PROTOCOL_VERSION: u64 = 1;
const UPSTREAM_COMMIT: &str = "4cda0ae87698135aa672990e6e68cf7873847426";
const RUST_TOOLCHAIN: &str = "1.94.1";
const RESPONSE_TIMEOUT: Duration = Duration::from_secs(60);
const EXIT_TIMEOUT: Duration = Duration::from_secs(2);
const BACKEND_START_TIMEOUT: Duration = Duration::from_secs(10);

struct BackendProcess {
    child: Child,
    schema_path: PathBuf,
    space_root: PathBuf,
    url: String,
}

impl BackendProcess {
    fn spawn(scenario: &str, schema: &str) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("reserve backend port");
        let port = listener.local_addr().expect("backend address").port();
        drop(listener);

        let fixture_root = std::env::temp_dir().join(format!(
            "encrypted-spaces-backend-{}-{scenario}",
            std::process::id()
        ));
        let schema_path = fixture_root.join("schema.kdl");
        let space_root = fixture_root.join("spaces");
        fs::create_dir_all(&space_root).expect("create backend fixture root");
        fs::write(&schema_path, schema).expect("write backend schema fixture");
        let mut command = Command::new(backend_binary());
        command.args([
            "--schema",
            schema_path.to_str().expect("schema path is UTF-8"),
            "--space-root",
            space_root.to_str().expect("space root is UTF-8"),
            "--bind-addr",
            "127.0.0.1",
            "--port",
            &port.to_string(),
        ]);
        if std::env::var_os("ENCRYPTED_SPACES_REQUIRE_REAL_PROOF").is_none() {
            command.env("RISC0_DEV_MODE", "1");
        } else {
            command.env_remove("RISC0_DEV_MODE");
            command.env_remove("RISC0_SKIP_BUILD");
        }
        let child = command
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn backend fixture");
        let mut backend = Self {
            child,
            schema_path,
            space_root,
            url: format!("ws://127.0.0.1:{port}/ws"),
        };
        backend.wait_for_health(port);
        backend
    }

    fn wait_for_health(&mut self, port: u16) {
        let deadline = Instant::now() + BACKEND_START_TIMEOUT;
        loop {
            if let Some(status) = self.child.try_wait().expect("poll backend process") {
                panic!("backend exited before health check with {status}");
            }
            if backend_is_healthy(port) {
                return;
            }
            assert!(
                Instant::now() < deadline,
                "backend did not become healthy within {BACKEND_START_TIMEOUT:?}"
            );
            thread::sleep(Duration::from_millis(25));
        }
    }
}

impl Drop for BackendProcess {
    fn drop(&mut self) {
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
        if let Some(root) = self.schema_path.parent() {
            let _ = fs::remove_dir_all(root);
        } else {
            let _ = fs::remove_dir_all(&self.space_root);
        }
    }
}

fn backend_binary() -> &'static Path {
    static BINARY: OnceLock<PathBuf> = OnceLock::new();
    BINARY
        .get_or_init(|| {
            if let Some(path) = std::env::var_os("ENCRYPTED_SPACES_BACKEND_TEST_BINARY") {
                return PathBuf::from(path);
            }
            let root = Path::new(env!("CARGO_MANIFEST_DIR"))
                .parent()
                .expect("workspace root");
            let target = root.join("target/bridge-backend-fixture");
            let status = Command::new("cargo")
                .args(["build", "--locked", "-p", "encrypted-spaces-backend-server"])
                .current_dir(root)
                .env("CARGO_TARGET_DIR", &target)
                .env("RISC0_SKIP_BUILD", "1")
                .status()
                .expect("build backend fixture");
            assert!(
                status.success(),
                "backend fixture build failed with {status}"
            );
            target.join("debug/encrypted-spaces-backend-server")
        })
        .as_path()
}

fn backend_is_healthy(port: u16) -> bool {
    let Ok(mut stream) = TcpStream::connect(("127.0.0.1", port)) else {
        return false;
    };
    let _ = stream.set_read_timeout(Some(Duration::from_millis(250)));
    if stream
        .write_all(b"GET /healthz HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n")
        .is_err()
    {
        return false;
    }
    let mut response = String::new();
    stream.read_to_string(&mut response).is_ok() && response.starts_with("HTTP/1.1 200")
}

enum StdoutEvent {
    Line(String),
    Eof,
    Error(String),
}

struct BridgeProcess {
    child: Child,
    stdin: Option<ChildStdin>,
    stdout: mpsc::Receiver<StdoutEvent>,
    stdout_thread: Option<JoinHandle<()>>,
    scenario: String,
    next_request: usize,
    schema_path: Option<PathBuf>,
}

impl BridgeProcess {
    fn spawn(scenario: &str, actor: &str) -> Self {
        Self::spawn_with_schema(scenario, actor, SCHEMA_KDL)
    }

    fn spawn_with_schema(scenario: &str, actor: &str, schema: &str) -> Self {
        Self::spawn_with_backend(scenario, actor, schema, None)
    }

    fn spawn_with_backend(
        scenario: &str,
        actor: &str,
        schema: &str,
        backend_url: Option<&str>,
    ) -> Self {
        let schema_path = std::env::temp_dir().join(format!(
            "encrypted-spaces-bridge-{}-{}-{}.kdl",
            std::process::id(),
            scenario,
            actor
        ));
        fs::write(&schema_path, schema).expect("write bridge schema fixture");
        let bridge_binary = std::env::var_os("ENCRYPTED_SPACES_BRIDGE_TEST_BINARY")
            .map(PathBuf::from)
            .unwrap_or_else(|| PathBuf::from(env!("CARGO_BIN_EXE_encrypted-spaces-bridge")));
        let mut command = Command::new(bridge_binary);
        command
            .env("ENCRYPTED_SPACES_CLIENT_LABEL", actor)
            .env("ENCRYPTED_SPACES_SCHEMA_PATH", &schema_path)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped());
        if let Some(backend_url) = backend_url {
            command.env("ENCRYPTED_SPACES_BACKEND_URL", backend_url);
        }
        let mut child = command.spawn().expect("spawn bridge");
        let stdin = child.stdin.take().expect("bridge stdin");
        let stdout = child.stdout.take().expect("bridge stdout");
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
        Self {
            child,
            stdin: Some(stdin),
            stdout: stdout_receiver,
            stdout_thread: Some(stdout_thread),
            scenario: format!("{scenario}-{actor}"),
            next_request: 1,
            schema_path: Some(schema_path),
        }
    }

    fn exchange(&mut self, operation: &str, payload: Value) -> Observation {
        self.exchange_inner(operation, payload, None)
    }

    fn exchange_with_actor_field(
        &mut self,
        operation: &str,
        payload: Value,
        actor: &str,
    ) -> Observation {
        self.exchange_inner(operation, payload, Some(actor))
    }

    fn exchange_inner(
        &mut self,
        operation: &str,
        payload: Value,
        actor_override: Option<&str>,
    ) -> Observation {
        let pending = self.send_inner(operation, payload, actor_override);
        self.receive(pending, RESPONSE_TIMEOUT)
    }

    fn send(&mut self, operation: &str, payload: Value) -> PendingObservation {
        self.send_inner(operation, payload, None)
    }

    fn send_with_request_id(
        &mut self,
        request_id: String,
        operation: &str,
        payload: Value,
    ) -> PendingObservation {
        let request = json!({
            "version": PROTOCOL_VERSION,
            "request_id": request_id,
            "operation": operation,
            "payload": payload,
        });
        let frame = serde_json::to_string(&request).expect("request JSON");
        let stdin = self.stdin.as_mut().expect("bridge stdin is open");
        writeln!(stdin, "{frame}").expect("write bridge frame");
        stdin.flush().expect("flush bridge frame");
        PendingObservation {
            operation: operation.to_owned(),
            request_id,
        }
    }

    fn send_inner(
        &mut self,
        operation: &str,
        payload: Value,
        actor_override: Option<&str>,
    ) -> PendingObservation {
        let request_id = format!(
            "runtime-{}-{:02}-{}",
            self.scenario,
            self.next_request,
            operation.replace('.', "-")
        );
        self.next_request += 1;
        let mut request = json!({
            "version": PROTOCOL_VERSION,
            "request_id": request_id,
            "operation": operation,
            "payload": payload,
        });
        if let Some(actor) = actor_override {
            request["actor_id"] = json!(actor);
        }
        let frame = serde_json::to_string(&request).expect("request JSON");
        let stdin = self.stdin.as_mut().expect("bridge stdin is open");
        writeln!(stdin, "{frame}").expect("write bridge frame");
        stdin.flush().expect("flush bridge frame");
        PendingObservation {
            operation: operation.to_owned(),
            request_id,
        }
    }

    fn receive(&mut self, pending: PendingObservation, timeout: Duration) -> Observation {
        let line = match self.stdout.recv_timeout(timeout) {
            Ok(StdoutEvent::Line(line)) => line,
            Ok(StdoutEvent::Eof) => {
                panic!("bridge exited before responding to {}", pending.operation)
            }
            Ok(StdoutEvent::Error(error)) => {
                panic!(
                    "bridge stdout read failed while handling {}: {error}",
                    pending.operation
                )
            }
            Err(mpsc::RecvTimeoutError::Timeout) => {
                panic!(
                    "bridge did not respond to {} within {timeout:?}",
                    pending.operation
                )
            }
            Err(mpsc::RecvTimeoutError::Disconnected) => {
                panic!(
                    "bridge stdout reader disconnected while handling {}",
                    pending.operation
                )
            }
        };
        let response = serde_json::from_str(&line).expect("bridge response JSON");
        Observation {
            operation: pending.operation,
            request_id: pending.request_id,
            response,
        }
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

    fn join_reader(&mut self) {
        if let Some(thread) = self.stdout_thread.take() {
            thread.join().expect("join bridge stdout reader");
        }
    }

    fn remove_schema_fixture(&mut self) {
        if let Some(path) = self.schema_path.take() {
            fs::remove_file(path).expect("remove bridge schema fixture");
        }
    }

    fn finish(mut self) {
        self.stdin.take();
        let status = self.wait_for_exit();
        self.join_reader();
        self.remove_schema_fixture();
        assert!(status.success(), "bridge exited with {status}");
    }
}

impl Drop for BridgeProcess {
    fn drop(&mut self) {
        self.stdin.take();
        if self.child.try_wait().ok().flatten().is_none() {
            let _ = self.child.kill();
            let _ = self.child.wait();
        }
        if let Some(thread) = self.stdout_thread.take() {
            let _ = thread.join();
        }
        if let Some(path) = self.schema_path.take() {
            let _ = fs::remove_file(path);
        }
    }
}

struct Observation {
    operation: String,
    request_id: String,
    response: Value,
}

struct PendingObservation {
    operation: String,
    request_id: String,
}

struct Scenario {
    name: String,
    backend: BackendProcess,
    bridges: BTreeMap<String, BridgeProcess>,
    observations: Vec<Observation>,
    failures: Vec<String>,
}

impl Scenario {
    fn start(name: &str, actors: &[&str]) -> Self {
        let backend = BackendProcess::spawn(name, SCHEMA_KDL);
        let bridges = actors
            .iter()
            .map(|actor| {
                (
                    (*actor).to_owned(),
                    BridgeProcess::spawn_with_backend(name, actor, SCHEMA_KDL, Some(&backend.url)),
                )
            })
            .collect();
        Self {
            name: name.to_owned(),
            backend,
            bridges,
            observations: Vec::new(),
            failures: Vec::new(),
        }
    }

    fn request(&mut self, actor: &str, operation: &str, payload: Value) -> usize {
        let observation = self
            .bridges
            .get_mut(actor)
            .unwrap_or_else(|| panic!("scenario actor {actor} has no bridge process"))
            .exchange(operation, payload);
        self.record(observation)
    }

    fn send(&mut self, actor: &str, operation: &str, payload: Value) -> PendingObservation {
        self.bridges
            .get_mut(actor)
            .unwrap_or_else(|| panic!("scenario actor {actor} has no bridge process"))
            .send(operation, payload)
    }

    fn receive(&mut self, actor: &str, pending: PendingObservation, timeout: Duration) -> usize {
        let observation = self
            .bridges
            .get_mut(actor)
            .unwrap_or_else(|| panic!("scenario actor {actor} has no bridge process"))
            .receive(pending, timeout);
        self.record(observation)
    }

    fn record(&mut self, observation: Observation) -> usize {
        let actual_version = observation.response.get("version").and_then(Value::as_u64);
        if actual_version != Some(PROTOCOL_VERSION) {
            self.failures.push(format!(
                "{}: version {:?}, expected {PROTOCOL_VERSION}",
                observation.operation, actual_version
            ));
        }
        let actual_request_id = observation
            .response
            .get("request_id")
            .and_then(Value::as_str);
        let uncorrelated_duplicate = actual_request_id.is_none()
            && observation.response["error"]["code"].as_str() == Some("DUPLICATE_REQUEST_ID");
        if actual_request_id != Some(observation.request_id.as_str()) && !uncorrelated_duplicate {
            self.failures.push(format!(
                "{}: request_id {:?}, expected {}",
                observation.operation, actual_request_id, observation.request_id
            ));
        }
        self.observations.push(observation);
        self.observations.len() - 1
    }

    fn restart(&mut self, actor: &str) {
        let previous = self
            .bridges
            .remove(actor)
            .unwrap_or_else(|| panic!("scenario actor {actor} has no bridge process"));
        previous.finish();
        self.bridges.insert(
            actor.to_owned(),
            BridgeProcess::spawn_with_backend(
                &self.name,
                actor,
                SCHEMA_KDL,
                Some(&self.backend.url),
            ),
        );
    }

    fn returned_string(&self, index: usize, field: &str, fallback: &str) -> String {
        self.observations[index].response["result"][field]
            .as_str()
            .unwrap_or(fallback)
            .to_owned()
    }

    fn returned_value(&self, index: usize, field: &str, fallback: Value) -> Value {
        self.observations[index].response["result"]
            .get(field)
            .cloned()
            .unwrap_or(fallback)
    }

    fn verify<T, F>(&mut self, index: usize, validate: F)
    where
        T: DeserializeOwned,
        F: FnOnce(&T) -> Result<(), String>,
    {
        let observation = &self.observations[index];
        let failure = match observation.response.get("ok").and_then(Value::as_bool) {
            Some(true) => match observation.response.get("result") {
                Some(result) => match serde_json::from_value::<T>(result.clone()) {
                    Ok(result) => validate(&result).err(),
                    Err(error) => Some(format!("typed result mismatch: {error}")),
                },
                None => Some("missing typed result".to_owned()),
            },
            Some(false) => Some(format!(
                "missing typed result ({})",
                observation.response["error"]["code"]
                    .as_str()
                    .unwrap_or("missing error code")
            )),
            None => Some("missing boolean ok field".to_owned()),
        };
        if let Some(failure) = failure {
            self.failures
                .push(format!("{}: {failure}", observation.operation));
        }
    }

    fn verify_error<T, F>(&mut self, index: usize, validate: F)
    where
        T: DeserializeOwned,
        F: FnOnce(&T) -> Result<(), String>,
    {
        let observation = &self.observations[index];
        let failure = match observation.response.get("ok").and_then(Value::as_bool) {
            Some(false) => match observation.response.get("error") {
                Some(error) => match serde_json::from_value::<T>(error.clone()) {
                    Ok(error) => validate(&error).err(),
                    Err(error) => Some(format!("typed error mismatch: {error}")),
                },
                None => Some("missing typed error".to_owned()),
            },
            Some(true) => Some("expected typed error, received success".to_owned()),
            None => Some("missing boolean ok field".to_owned()),
        };
        if let Some(failure) = failure {
            self.failures
                .push(format!("{}: {failure}", observation.operation));
        }
    }

    fn finish(mut self) {
        for (_, bridge) in std::mem::take(&mut self.bridges) {
            bridge.finish();
        }
        assert!(
            self.failures.is_empty(),
            "runtime scenario failures:\n{}",
            self.failures.join("\n")
        );
    }
}

#[derive(Deserialize)]
struct HelloResult {
    protocol_version: u64,
    client_label: String,
    schema_sha256: String,
    data_commitment: String,
    ff_guest_image_id: Vec<u32>,
}

#[derive(Deserialize)]
struct VersionResult {
    version: String,
    protocol_version: u64,
}

#[derive(Deserialize)]
struct SpaceCreateResult {
    space_id: String,
    schema_sha256: String,
}

#[derive(Deserialize)]
struct SnapshotResult {
    space_id: String,
    snapshot: Value,
}

#[derive(Deserialize)]
struct RestoreResult {
    space_id: String,
    restored: bool,
}

#[derive(Deserialize)]
struct InviteResult {
    space_id: String,
    member_id: i64,
    invite: Value,
}

#[derive(Deserialize)]
struct JoinResult {
    space_id: String,
    member_id: i64,
    joined: bool,
}

#[derive(Deserialize)]
struct SyncResult {
    space_id: String,
    synced: bool,
    #[serde(default)]
    wait_trigger: Option<String>,
}

#[derive(Deserialize)]
struct CancelResult {
    canceled: bool,
}

#[derive(Deserialize)]
struct CloseResult {
    closed: bool,
}

#[derive(Deserialize)]
struct ShutdownResult {
    shutting_down: bool,
}

#[derive(Deserialize)]
struct RemoveResult {
    space_id: String,
    member_id: i64,
    removed: bool,
}

#[derive(Deserialize)]
struct TableInsertResult {
    row_id: i64,
}

#[derive(Deserialize)]
struct TableSelectResult {
    rows: Vec<Value>,
}

#[derive(Deserialize)]
struct BridgeError {
    code: String,
}

#[derive(Deserialize)]
struct ListCreateResult {
    space_id: String,
    table: String,
    row_id: i64,
    column: String,
    list_ref: Value,
}

#[derive(Deserialize)]
struct ListAppendResult {
    space_id: String,
    list_ref: Value,
    item_ref: Value,
}

#[derive(Deserialize)]
struct ListItemResult {
    item_ref: Value,
    position: u64,
    value: Value,
}

#[derive(Deserialize)]
struct ListReadResult {
    space_id: String,
    list_ref: Value,
    items: Vec<ListItemResult>,
}

#[derive(Deserialize)]
struct TextCreateResult {
    space_id: String,
    table: String,
    row_id: i64,
    column: String,
    text_ref: Value,
}

#[derive(Deserialize)]
struct TextEditResult {
    space_id: String,
    text_ref: Value,
    edited: bool,
}

#[derive(Deserialize)]
struct TextReadResult {
    space_id: String,
    text_ref: Value,
    text: String,
}

#[derive(Deserialize)]
struct FilePutResult {
    space_id: String,
    digest: String,
}

#[derive(Deserialize)]
struct FileGetResult {
    space_id: String,
    digest: String,
    bytes_base64: String,
}

const TABLE: &str = "bridge_records";
const LIST_COLUMN: &str = "items";
const TEXT_COLUMN: &str = "document";
const FILE_DIGEST_PLACEHOLDER: &str =
    "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef";
const SCHEMA_KDL: &str = r#"
table "bridge_records" auto_increment=#true {
    column "id"         type="int"     plaintext=#true
    column "rank"       type="int"
    column "label"      type="text"
    column "items"      type="list"
    column "document"   type="list"
    column "attachment" type="fileref"
}
"#;
fn create_payload() -> Value {
    json!({})
}

fn join_payload(invite: Value) -> Value {
    json!({"invite": invite})
}

fn parent_row(label: &str, rank: i64) -> Value {
    json!({
        "rank": rank,
        "label": label,
        "items": 0,
        "document": 0,
        "attachment": FILE_DIGEST_PLACEHOLDER,
    })
}

fn valid_space_id(space_id: &str) -> bool {
    space_id.len() == 32 && space_id.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn valid_created_space(result: &SpaceCreateResult) -> bool {
    valid_space_id(&result.space_id) && valid_digest(&result.schema_sha256)
}

fn valid_digest(digest: &str) -> bool {
    digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn schema_digest(schema: &str) -> String {
    format!("{:x}", Sha256::digest(schema.as_bytes()))
}

fn sdk_data_commitment(schema: &str, suffix: &str) -> String {
    let schema_path = std::env::temp_dir().join(format!(
        "encrypted-spaces-sdk-commitment-{}-{suffix}.kdl",
        std::process::id()
    ));
    fs::write(&schema_path, schema).expect("write SDK commitment schema");
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("build SDK commitment runtime");
    let commitment = runtime
        .block_on(async {
            let transport = encrypted_spaces_sdk::LocalTransport::from_schema_file(
                schema_path.to_str().expect("schema path is UTF-8"),
            )
            .await?;
            transport.get_root_hash().await
        })
        .expect("compute SDK data commitment");
    fs::remove_file(schema_path).expect("remove SDK commitment schema");
    commitment
        .iter()
        .map(|byte| format!("{byte:02x}"))
        .collect()
}

fn expected_ff_guest_id() -> Vec<u32> {
    match std::env::var("ENCRYPTED_SPACES_EXPECTED_FF_GUEST_ID") {
        Ok(value) => value
            .split(',')
            .map(|word| word.parse::<u32>().expect("guest image ID word"))
            .collect(),
        Err(_) => encrypted_spaces_ffproof::EXTEND_FF_ID.to_vec(),
    }
}

fn is_opaque_ref(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::String(value) => !value.is_empty(),
        Value::Array(value) => !value.is_empty(),
        Value::Object(value) => !value.is_empty(),
        Value::Bool(_) | Value::Number(_) => true,
    }
}

fn row_matches(row: &Value, row_id: i64, label: &str, rank: i64) -> bool {
    row["id"].as_i64() == Some(row_id)
        && row["rank"].as_i64() == Some(rank)
        && row["label"].as_str() == Some(label)
        && !row["items"].is_null()
        && !row["document"].is_null()
        && row["attachment"].as_str() == Some(FILE_DIGEST_PLACEHOLDER)
}

#[test]
fn runtime_request_actor_override_is_rejected() {
    let configured_actor = "actor-configured-parametric";
    let mut bridge = BridgeProcess::spawn("actor-override", configured_actor);
    let observation =
        bridge.exchange_with_actor_field("hello", json!({}), "actor-frame-override-parametric");
    bridge.finish();

    assert_eq!(observation.response["version"], PROTOCOL_VERSION);
    assert_eq!(observation.response["request_id"], observation.request_id);
    assert_eq!(
        observation.response["ok"], false,
        "request-level actor override reached the runtime"
    );
    assert_eq!(observation.response["error"]["code"], "INVALID_REQUEST");
}

#[test]
fn runtime_request_trust_override_is_rejected() {
    let mut bridge = BridgeProcess::spawn("trust-override", "actor-trust-parametric");
    let observation = bridge.exchange(
        "space.create",
        json!({
            "schema_kdl": "table \"attacker\" {}",
            "data_commitment": "00".repeat(32),
            "ff_guest_image_id": [1, 2, 3, 4, 5, 6, 7, 8],
        }),
    );
    bridge.finish();

    assert_eq!(observation.response["version"], PROTOCOL_VERSION);
    assert_eq!(observation.response["request_id"], observation.request_id);
    assert_eq!(observation.response["ok"], false);
    assert_eq!(observation.response["error"]["code"], "INVALID_REQUEST");
}

#[test]
fn runtime_hello_health_metadata_is_process_bound() {
    let modified_schema = format!(
        "{SCHEMA_KDL}\ntable \"schema_change_probe\" {{\n    column \"id\" type=\"int\" plaintext=#true\n}}\n"
    );
    let mut first = BridgeProcess::spawn_with_schema("trust-first", "actor-first", SCHEMA_KDL);
    let mut second = BridgeProcess::spawn_with_schema("trust-second", "actor-second", SCHEMA_KDL);
    let mut changed =
        BridgeProcess::spawn_with_schema("trust-changed", "actor-changed", &modified_schema);

    let first_response = first.exchange("hello", json!({})).response;
    let second_response = second.exchange("hello", json!({})).response;
    let changed_response = changed.exchange("hello", json!({})).response;
    first.finish();
    second.finish();
    changed.finish();

    let expected_commitment = sdk_data_commitment(SCHEMA_KDL, "stable");
    let changed_commitment = sdk_data_commitment(&modified_schema, "changed");
    let expected_guest_id = expected_ff_guest_id();

    for response in [&first_response, &second_response, &changed_response] {
        assert_eq!(response["ok"], true, "hello trust metadata failed");
        assert!(
            response["result"].get("actor_id").is_none(),
            "process metadata must not be represented as a trusted actor identity"
        );
        assert!(
            response["result"].get("client_label").is_some(),
            "hello must expose process metadata as an untrusted client label"
        );
    }
    let first: HelloResult =
        serde_json::from_value(first_response["result"].clone()).expect("first hello result");
    let second: HelloResult =
        serde_json::from_value(second_response["result"].clone()).expect("second hello result");
    let changed: HelloResult =
        serde_json::from_value(changed_response["result"].clone()).expect("changed hello result");

    assert_eq!(first.client_label, "actor-first");
    assert_eq!(second.client_label, "actor-second");
    assert_eq!(changed.client_label, "actor-changed");
    assert_eq!(first.schema_sha256, schema_digest(SCHEMA_KDL));
    assert_eq!(second.schema_sha256, first.schema_sha256);
    assert_eq!(changed.schema_sha256, schema_digest(&modified_schema));
    assert_ne!(changed.schema_sha256, first.schema_sha256);
    assert_eq!(first.data_commitment, expected_commitment);
    assert_eq!(second.data_commitment, first.data_commitment);
    assert_eq!(changed.data_commitment, changed_commitment);
    assert_ne!(changed.data_commitment, first.data_commitment);
    assert_eq!(first.ff_guest_image_id, expected_guest_id);
    assert_eq!(second.ff_guest_image_id, first.ff_guest_image_id);
    assert_eq!(changed.ff_guest_image_id, first.ff_guest_image_id);
}

#[test]
fn runtime_create_restore_uses_backend_and_survives_process_restart() {
    let actor = "actor-create-restore-parametric";
    let mut scenario = Scenario::start("create-restore", &[actor]);

    let create = scenario.request(actor, "space.create", json!({}));
    let space_id = scenario.returned_string(create, "space_id", "missing-create-space");
    let snapshot = scenario.request(actor, "space.snapshot", json!({"space_id": space_id}));
    let snapshot_value =
        scenario.returned_value(snapshot, "snapshot", json!({"missing": "snapshot"}));
    scenario.restart(actor);
    let restore = scenario.request(actor, "space.restore", json!({"snapshot": snapshot_value}));

    scenario.verify::<SpaceCreateResult, _>(create, |result| {
        valid_created_space(result)
            .then_some(())
            .ok_or_else(|| "space.create returned an invalid SpaceId".to_owned())
    });
    scenario.verify::<SnapshotResult, _>(snapshot, |result| {
        (result.space_id == space_id && is_opaque_ref(&result.snapshot))
            .then_some(())
            .ok_or_else(|| "space.snapshot returned the wrong space or no snapshot".to_owned())
    });
    scenario.verify::<RestoreResult, _>(restore, |result| {
        (result.space_id == space_id && result.restored)
            .then_some(())
            .ok_or_else(|| "space.restore did not restore after process restart".to_owned())
    });
    scenario.finish();
}

#[test]
fn runtime_restore_rejects_snapshot_from_foreign_trust_bundle() {
    let backend = BackendProcess::spawn("restore-trust", SCHEMA_KDL);
    let mut source = BridgeProcess::spawn_with_backend(
        "restore-trust-source",
        "source-client",
        SCHEMA_KDL,
        Some(&backend.url),
    );
    let create = source.exchange("space.create", json!({}));
    let space_id = create.response["result"]["space_id"]
        .as_str()
        .expect("created space ID")
        .to_owned();
    let snapshot = source.exchange("space.snapshot", json!({"space_id": space_id}));
    let snapshot = snapshot.response["result"]["snapshot"].clone();
    source.finish();

    let modified_schema = format!(
        "{SCHEMA_KDL}\ntable \"foreign_trust_probe\" {{\n    column \"id\" type=\"int\" plaintext=#true\n}}\n"
    );
    let mut target = BridgeProcess::spawn_with_backend(
        "restore-trust-target",
        "target-client",
        &modified_schema,
        Some(&backend.url),
    );
    let restore = target.exchange("space.restore", json!({"snapshot": snapshot}));

    assert_eq!(restore.response["ok"], false);
    assert_eq!(restore.response["error"]["code"], "TRUST_MISMATCH");
    target.finish();
    drop(backend);
}

#[test]
fn runtime_snapshot_sync_runs_verified_backend_recovery() {
    let actor = "actor-snapshot-sync-parametric";
    let mut scenario = Scenario::start("snapshot-sync", &[actor]);

    let create = scenario.request(actor, "space.create", json!({}));
    let space_id = scenario.returned_string(create, "space_id", "missing-sync-space");
    let sync = scenario.request(actor, "space.sync", json!({"space_id": space_id}));

    scenario.verify::<SyncResult, _>(sync, |result| {
        (result.space_id == space_id && result.synced && result.wait_trigger.is_none())
            .then_some(())
            .ok_or_else(|| "space.sync did not complete verified recovery".to_owned())
    });
    scenario.finish();
}

#[test]
#[ignore = "release gate: generates and verifies a real RISC Zero receipt"]
fn runtime_packaged_backend_generates_real_fast_forward_receipt() {
    assert_eq!(
        std::env::var("ENCRYPTED_SPACES_REQUIRE_REAL_PROOF").as_deref(),
        Ok("1"),
        "real-proof release gate was not explicitly enabled"
    );
    assert!(std::env::var_os("RISC0_DEV_MODE").is_none());
    assert!(std::env::var_os("RISC0_SKIP_BUILD").is_none());

    let owner = "owner-real-proof-release";
    let member = "member-real-proof-release";
    let proof_timeout = Duration::from_secs(3600);
    let mut scenario = Scenario::start("real-proof-release", &[owner, member]);

    let create_pending = scenario.send(owner, "space.create", json!({}));
    let create = scenario.receive(owner, create_pending, proof_timeout);
    let space_id = scenario.returned_string(create, "space_id", "missing-real-proof-space");
    let invite_pending = scenario.send(owner, "member.invite", json!({"space_id": space_id}));
    let invite = scenario.receive(owner, invite_pending, proof_timeout);
    let invite_value = scenario.returned_value(invite, "invite", json!({"missing": "invite"}));
    let join_pending = scenario.send(member, "space.join", join_payload(invite_value));
    scenario.receive(member, join_pending, proof_timeout);

    let snapshot_pending = scenario.send(owner, "space.snapshot", json!({"space_id": space_id}));
    let snapshot = scenario.receive(owner, snapshot_pending, proof_timeout);
    let current_change_id = scenario.observations[snapshot].response["result"]["snapshot"]["state"]
        ["current_change_id"]
        .as_u64()
        .expect("snapshot current_change_id");
    let changes_to_boundary = 5 - (current_change_id % 5);
    let mut last_insert = None;
    for offset in 0..changes_to_boundary {
        let pending = scenario.send(
            owner,
            "table.insert",
            json!({
                "space_id": space_id,
                "table": TABLE,
                "row": parent_row(
                    &format!("real-proof-boundary-{offset}"),
                    900 + offset as i64,
                ),
            }),
        );
        last_insert = Some(scenario.receive(owner, pending, proof_timeout));
    }

    let sync_pending = scenario.send(member, "space.sync", json!({"space_id": space_id}));
    let sync = scenario.receive(member, sync_pending, proof_timeout);
    scenario.verify::<TableInsertResult, _>(
        last_insert.expect("proof-boundary insert"),
        |result| {
            (result.row_id > 0)
                .then_some(())
                .ok_or_else(|| "proof-boundary insert failed".to_owned())
        },
    );
    scenario.verify::<SyncResult, _>(sync, |result| {
        (result.space_id == space_id && result.synced)
            .then_some(())
            .ok_or_else(|| "second client did not verify real fast-forward receipt".to_owned())
    });
    scenario.finish();
}

#[test]
fn runtime_sync_wait_wakes_and_runs_verified_recovery() {
    let owner = "owner-sync-wait-parametric";
    let member = "member-sync-wait-parametric";
    let mut scenario = Scenario::start("sync-wait", &[owner, member]);
    let create = scenario.request(owner, "space.create", json!({}));
    let space_id = scenario.returned_string(create, "space_id", "missing-wait-space");
    let invite = scenario.request(owner, "member.invite", json!({"space_id": space_id}));
    let invite_value = scenario.returned_value(invite, "invite", json!({"missing": "invite"}));
    scenario.request(member, "space.join", join_payload(invite_value));
    scenario.request(owner, "space.sync", json!({"space_id": space_id}));

    let pending = scenario.send(
        owner,
        "space.sync",
        json!({"space_id": space_id, "wait_for_change_ms": 30_000}),
    );
    let registration_barrier = scenario.request(owner, "version", json!({}));
    scenario.request(
        member,
        "table.insert",
        json!({
            "space_id": space_id,
            "table": TABLE,
            "row": parent_row("wake-on-real-change-parametric", 613),
        }),
    );
    let sync = scenario.receive(owner, pending, RESPONSE_TIMEOUT);

    scenario.verify::<VersionResult, _>(registration_barrier, |result| {
        (!result.version.is_empty() && result.protocol_version == PROTOCOL_VERSION)
            .then_some(())
            .ok_or_else(|| "ordinary request blocked behind waiting sync".to_owned())
    });

    scenario.verify::<SyncResult, _>(sync, |result| {
        (result.space_id == space_id
            && result.synced
            && result.wait_trigger.as_deref() == Some("change"))
        .then_some(())
        .ok_or_else(|| "space.sync did not wake on the real remote change".to_owned())
    });
    scenario.finish();
}

#[test]
fn runtime_cancellation_interrupts_valid_wait_and_keeps_process_usable() {
    let actor = "actor-cancel-wait-parametric";
    let mut scenario = Scenario::start("cancel-wait", &[actor]);
    let create = scenario.request(actor, "space.create", json!({}));
    let space_id = scenario.returned_string(create, "space_id", "missing-cancel-space");
    let waiting = scenario.send(
        actor,
        "space.sync",
        json!({"space_id": space_id, "wait_for_change_ms": 30_000}),
    );
    let waiting_request_id = waiting.request_id.clone();
    let cancel = scenario.send(actor, "cancel", json!({"request_id": waiting_request_id}));
    let canceled = scenario.receive(actor, waiting, RESPONSE_TIMEOUT);
    let cancel_ack = scenario.receive(actor, cancel, RESPONSE_TIMEOUT);
    let version = scenario.request(actor, "version", json!({}));

    scenario.verify_error::<BridgeError, _>(canceled, |error| {
        (error.code == "CANCELED")
            .then_some(())
            .ok_or_else(|| "valid waiting sync did not return CANCELED".to_owned())
    });
    scenario.verify::<CancelResult, _>(cancel_ack, |result| {
        result
            .canceled
            .then_some(())
            .ok_or_else(|| "cancel acknowledgment did not report cancellation".to_owned())
    });
    scenario.verify::<VersionResult, _>(version, |result| {
        (!result.version.is_empty() && result.protocol_version == PROTOCOL_VERSION)
            .then_some(())
            .ok_or_else(|| "bridge was unusable after cancellation".to_owned())
    });
    scenario.finish();
}

#[test]
fn runtime_shutdown_cancels_pending_wait_before_exit() {
    let actor = "actor-shutdown-wait";
    let mut scenario = Scenario::start("shutdown-wait", &[actor]);
    let create = scenario.request(actor, "space.create", create_payload());
    let space_id = scenario.returned_string(create, "space_id", "missing-shutdown-space");
    let waiting = scenario.send(
        actor,
        "space.sync",
        json!({"space_id": space_id, "wait_for_change_ms": 30_000}),
    );
    let shutdown = scenario.send(actor, "shutdown", json!({}));

    let canceled = scenario.receive(actor, waiting, RESPONSE_TIMEOUT);
    let shutdown_ack = scenario.receive(actor, shutdown, RESPONSE_TIMEOUT);
    scenario.verify_error::<BridgeError, _>(canceled, |error| {
        (error.code == "CANCELED")
            .then_some(())
            .ok_or_else(|| "shutdown did not cancel the pending wait".to_owned())
    });
    scenario.verify::<ShutdownResult, _>(shutdown_ack, |result| {
        result
            .shutting_down
            .then_some(())
            .ok_or_else(|| "shutdown was not acknowledged".to_owned())
    });

    let mut bridge = scenario
        .bridges
        .remove(actor)
        .expect("shutdown bridge process");
    let status = bridge.wait_for_exit();
    bridge.join_reader();
    bridge.remove_schema_fixture();
    assert!(status.success(), "bridge exited with {status}");
    scenario.finish();
}

#[test]
fn runtime_eof_cancels_pending_wait_before_exit() {
    let actor = "actor-eof-wait";
    let mut scenario = Scenario::start("eof-wait", &[actor]);
    let create = scenario.request(actor, "space.create", create_payload());
    let space_id = scenario.returned_string(create, "space_id", "missing-eof-space");
    let waiting = scenario.send(
        actor,
        "space.sync",
        json!({"space_id": space_id, "wait_for_change_ms": 30_000}),
    );
    scenario
        .bridges
        .get_mut(actor)
        .expect("EOF bridge process")
        .stdin
        .take();

    let canceled = scenario.receive(actor, waiting, RESPONSE_TIMEOUT);
    scenario.verify_error::<BridgeError, _>(canceled, |error| {
        (error.code == "CANCELED")
            .then_some(())
            .ok_or_else(|| "EOF did not cancel the pending wait".to_owned())
    });

    let mut bridge = scenario.bridges.remove(actor).expect("EOF bridge process");
    let status = bridge.wait_for_exit();
    bridge.join_reader();
    bridge.remove_schema_fixture();
    assert!(status.success(), "bridge exited with {status}");
    scenario.finish();
}

#[test]
fn runtime_sync_wait_reports_timeout_without_remote_change() {
    let actor = "actor-wait-timeout-parametric";
    let mut scenario = Scenario::start("wait-timeout", &[actor]);
    let create = scenario.request(actor, "space.create", json!({}));
    let space_id = scenario.returned_string(create, "space_id", "missing-timeout-space");
    let sync = scenario.request(
        actor,
        "space.sync",
        json!({"space_id": space_id, "wait_for_change_ms": 50}),
    );

    scenario.verify::<SyncResult, _>(sync, |result| {
        (result.space_id == space_id
            && result.synced
            && result.wait_trigger.as_deref() == Some("timeout"))
        .then_some(())
        .ok_or_else(|| "waiting sync did not report timeout completion".to_owned())
    });
    scenario.finish();
}

#[test]
fn runtime_rejects_request_id_reuse_while_wait_is_pending() {
    let actor = "actor-duplicate-id-parametric";
    let mut scenario = Scenario::start("duplicate-id", &[actor]);
    let create = scenario.request(actor, "space.create", json!({}));
    let space_id = scenario.returned_string(create, "space_id", "missing-duplicate-space");
    let waiting = scenario.send(
        actor,
        "space.sync",
        json!({"space_id": space_id, "wait_for_change_ms": 30_000}),
    );
    let waiting_request_id = waiting.request_id.clone();
    scenario.request(actor, "version", json!({}));
    let duplicate = scenario
        .bridges
        .get_mut(actor)
        .expect("actor bridge")
        .send_with_request_id(waiting_request_id.clone(), "version", json!({}));
    let duplicate = scenario.receive(actor, duplicate, RESPONSE_TIMEOUT);
    let cancel = scenario.send(actor, "cancel", json!({"request_id": waiting_request_id}));
    let canceled = scenario.receive(actor, waiting, RESPONSE_TIMEOUT);
    let cancel_ack = scenario.receive(actor, cancel, RESPONSE_TIMEOUT);

    scenario.verify_error::<BridgeError, _>(duplicate, |error| {
        (error.code == "DUPLICATE_REQUEST_ID")
            .then_some(())
            .ok_or_else(|| "pending request ID was reused".to_owned())
    });
    scenario.verify_error::<BridgeError, _>(canceled, |error| {
        (error.code == "CANCELED")
            .then_some(())
            .ok_or_else(|| "pending wait was not canceled".to_owned())
    });
    scenario.verify::<CancelResult, _>(cancel_ack, |result| {
        result
            .canceled
            .then_some(())
            .ok_or_else(|| "cancel acknowledgment was false".to_owned())
    });
    scenario.finish();
}

#[test]
fn runtime_cancel_validation_cannot_reuse_a_pending_request_id() {
    let actor = "actor-cancel-duplicate-parametric";
    let mut scenario = Scenario::start("cancel-duplicate", &[actor]);
    let create = scenario.request(actor, "space.create", json!({}));
    let space_id = scenario.returned_string(create, "space_id", "missing-cancel-space");
    let waiting = scenario.send(
        actor,
        "space.sync",
        json!({"space_id": space_id, "wait_for_change_ms": 30_000}),
    );
    let waiting_request_id = waiting.request_id.clone();
    scenario.request(actor, "version", json!({}));

    let malformed = scenario
        .bridges
        .get_mut(actor)
        .expect("actor bridge")
        .send_with_request_id(waiting_request_id.clone(), "cancel", json!({}));
    let malformed = scenario.receive(actor, malformed, RESPONSE_TIMEOUT);
    let self_targeting = scenario
        .bridges
        .get_mut(actor)
        .expect("actor bridge")
        .send_with_request_id(
            waiting_request_id.clone(),
            "cancel",
            json!({"request_id": waiting_request_id}),
        );
    let self_targeting = scenario.receive(actor, self_targeting, RESPONSE_TIMEOUT);

    let cancel = scenario.send(actor, "cancel", json!({"request_id": waiting_request_id}));
    let canceled = scenario.receive(actor, waiting, RESPONSE_TIMEOUT);
    let cancel_ack = scenario.receive(actor, cancel, RESPONSE_TIMEOUT);

    for duplicate in [malformed, self_targeting] {
        scenario.verify_error::<BridgeError, _>(duplicate, |error| {
            (error.code == "DUPLICATE_REQUEST_ID")
                .then_some(())
                .ok_or_else(|| "cancel validation reused a pending correlation ID".to_owned())
        });
    }
    scenario.verify_error::<BridgeError, _>(canceled, |error| {
        (error.code == "CANCELED")
            .then_some(())
            .ok_or_else(|| "pending wait was not canceled after duplicate probes".to_owned())
    });
    scenario.verify::<CancelResult, _>(cancel_ack, |result| {
        result
            .canceled
            .then_some(())
            .ok_or_else(|| "cancel acknowledgment was false".to_owned())
    });
    scenario.finish();
}

#[test]
fn runtime_space_lifecycle_survives_restart_and_membership_changes() {
    let owner = "owner-lifecycle-parametric";
    let member = "member-lifecycle-parametric";
    let mut scenario = Scenario::start("lifecycle", &[owner, member]);

    let hello = scenario.request(owner, "hello", json!({}));
    let create = scenario.request(owner, "space.create", create_payload());
    let space_id = scenario.returned_string(create, "space_id", "missing-lifecycle-space");
    let snapshot_label = "snapshot-state-parametric";
    let snapshot_rank = 211;
    let snapshot_insert = scenario.request(
        owner,
        "table.insert",
        json!({
            "space_id": space_id,
            "table": TABLE,
            "row": parent_row(snapshot_label, snapshot_rank),
        }),
    );
    let snapshot_row_id = scenario.observations[snapshot_insert].response["result"]["row_id"]
        .as_i64()
        .unwrap_or(-1);
    let snapshot = scenario.request(owner, "space.snapshot", json!({"space_id": space_id}));
    let snapshot_value = scenario.returned_value(
        snapshot,
        "snapshot",
        json!({"missing": "lifecycle-snapshot"}),
    );
    scenario.restart(owner);
    let restore = scenario.request(owner, "space.restore", json!({"snapshot": snapshot_value}));
    let restored_select = scenario.request(
        owner,
        "table.select",
        json!({
            "space_id": space_id,
            "table": TABLE,
            "where": {"id": snapshot_row_id},
        }),
    );
    let invite = scenario.request(owner, "member.invite", json!({"space_id": space_id}));
    let member_id = scenario.observations[invite].response["result"]["member_id"]
        .as_i64()
        .unwrap_or(-1);
    let invite_value =
        scenario.returned_value(invite, "invite", json!({"missing": "lifecycle-invite"}));
    let join = scenario.request(member, "space.join", join_payload(invite_value));
    let sync_label = "post-join-sync-parametric";
    let sync_rank = 307;
    let sync_insert = scenario.request(
        owner,
        "table.insert",
        json!({
            "space_id": space_id,
            "table": TABLE,
            "row": parent_row(sync_label, sync_rank),
        }),
    );
    let sync_row_id = scenario.observations[sync_insert].response["result"]["row_id"]
        .as_i64()
        .unwrap_or(-1);
    let sync = scenario.request(member, "space.sync", json!({"space_id": space_id}));
    let synced_select = scenario.request(
        member,
        "table.select",
        json!({
            "space_id": space_id,
            "table": TABLE,
            "where": {"id": sync_row_id},
        }),
    );
    let remove = scenario.request(
        owner,
        "member.remove",
        json!({"space_id": space_id, "member_id": member_id}),
    );

    scenario.verify::<HelloResult, _>(hello, |result| {
        (result.protocol_version == PROTOCOL_VERSION
            && result.client_label == owner
            && valid_digest(&result.schema_sha256)
            && valid_digest(&result.data_commitment)
            && result.ff_guest_image_id.len() == 8)
            .then_some(())
            .ok_or_else(|| "hello returned invalid process-bound trust metadata".to_owned())
    });
    scenario.verify::<SpaceCreateResult, _>(create, |result| {
        valid_created_space(result)
            .then_some(())
            .ok_or_else(|| "space.create returned an invalid SpaceId".to_owned())
    });
    scenario.verify::<SnapshotResult, _>(snapshot, |result| {
        (result.space_id == space_id && !result.snapshot.is_null())
            .then_some(())
            .ok_or_else(|| "space.snapshot returned the wrong space or no snapshot".to_owned())
    });
    scenario.verify::<TableInsertResult, _>(snapshot_insert, |result| {
        (result.row_id > 0)
            .then_some(())
            .ok_or_else(|| "snapshot fixture row has no auto-assigned ID".to_owned())
    });
    scenario.verify::<RestoreResult, _>(restore, |result| {
        (result.space_id == space_id && result.restored)
            .then_some(())
            .ok_or_else(|| "space.restore did not restore the returned snapshot".to_owned())
    });
    scenario.verify::<TableSelectResult, _>(restored_select, |result| {
        (result.rows.len() == 1
            && row_matches(
                &result.rows[0],
                snapshot_row_id,
                snapshot_label,
                snapshot_rank,
            ))
        .then_some(())
        .ok_or_else(|| "restored space did not retain the pre-snapshot row".to_owned())
    });
    scenario.verify::<InviteResult, _>(invite, |result| {
        (result.space_id == space_id && result.member_id > 0 && is_opaque_ref(&result.invite))
            .then_some(())
            .ok_or_else(|| "member.invite returned no numeric member ID or invite".to_owned())
    });
    scenario.verify::<JoinResult, _>(join, |result| {
        (result.space_id == space_id && result.member_id == member_id && result.joined)
            .then_some(())
            .ok_or_else(|| "space.join did not consume the returned invite".to_owned())
    });
    scenario.verify::<TableInsertResult, _>(sync_insert, |result| {
        (result.row_id > 0)
            .then_some(())
            .ok_or_else(|| "post-join sync fixture row has no auto-assigned ID".to_owned())
    });
    scenario.verify::<SyncResult, _>(sync, |result| {
        (result.space_id == space_id && result.synced)
            .then_some(())
            .ok_or_else(|| "space.sync did not report a completed sync".to_owned())
    });
    scenario.verify::<TableSelectResult, _>(synced_select, |result| {
        (result.rows.len() == 1 && row_matches(&result.rows[0], sync_row_id, sync_label, sync_rank))
            .then_some(())
            .ok_or_else(|| "space.sync did not expose the owner's post-join row".to_owned())
    });
    scenario.verify::<RemoveResult, _>(remove, |result| {
        (result.space_id == space_id && result.member_id == member_id && result.removed)
            .then_some(())
            .ok_or_else(|| "member.remove did not remove the joined member".to_owned())
    });
    scenario.finish();
}

#[test]
fn runtime_table_insert_select_round_trip_verified_rows() {
    let actor = "actor-table-parametric";
    let label = "table-value-parametric";
    let rank = 41;
    let row = parent_row(label, rank);
    let mut scenario = Scenario::start("table", &[actor]);
    let create = scenario.request(actor, "space.create", create_payload());
    let space_id = scenario.returned_string(create, "space_id", "missing-table-space");
    let insert = scenario.request(
        actor,
        "table.insert",
        json!({"space_id": space_id, "table": TABLE, "row": row}),
    );
    let row_id = scenario.observations[insert].response["result"]["row_id"]
        .as_i64()
        .unwrap_or(-1);
    let select = scenario.request(
        actor,
        "table.select",
        json!({"space_id": space_id, "table": TABLE, "where": {"id": row_id}}),
    );

    scenario.verify::<SpaceCreateResult, _>(create, |result| {
        valid_created_space(result)
            .then_some(())
            .ok_or_else(|| "space.create returned an invalid SpaceId".to_owned())
    });
    scenario.verify::<TableInsertResult, _>(insert, |result| {
        (result.row_id > 0)
            .then_some(())
            .ok_or_else(|| "table.insert returned no auto-assigned row ID".to_owned())
    });
    scenario.verify::<TableSelectResult, _>(select, |result| {
        (result.rows.len() == 1 && row_matches(&result.rows[0], row_id, label, rank))
            .then_some(())
            .ok_or_else(|| "table.select did not return the auto-ID row content".to_owned())
    });
    scenario.finish();
}

#[test]
fn runtime_list_create_append_read_round_trip() {
    let actor = "actor-list-parametric";
    let label = "list-parent-parametric";
    let rank = 73;
    let item = json!({"key": "list-key-parametric", "value": 73});
    let mut scenario = Scenario::start("list", &[actor]);
    let create_space = scenario.request(actor, "space.create", create_payload());
    let space_id = scenario.returned_string(create_space, "space_id", "missing-list-space");
    let insert = scenario.request(
        actor,
        "table.insert",
        json!({"space_id": space_id, "table": TABLE, "row": parent_row(label, rank)}),
    );
    let row_id = scenario.observations[insert].response["result"]["row_id"]
        .as_i64()
        .unwrap_or(-1);
    let create = scenario.request(
        actor,
        "list.create",
        json!({"space_id": space_id, "table": TABLE, "row_id": row_id, "column": LIST_COLUMN}),
    );
    let list_ref = scenario.returned_value(
        create,
        "list_ref",
        json!({"missing": "list-handle-reference"}),
    );
    let append = scenario.request(
        actor,
        "list.append",
        json!({"space_id": space_id, "list_ref": list_ref, "value": item}),
    );
    let item_ref = scenario.returned_value(
        append,
        "item_ref",
        json!({"missing": "list-item-reference"}),
    );
    let read = scenario.request(
        actor,
        "list.read",
        json!({"space_id": space_id, "list_ref": list_ref}),
    );

    scenario.verify::<SpaceCreateResult, _>(create_space, |result| {
        valid_created_space(result)
            .then_some(())
            .ok_or_else(|| "space.create returned an invalid SpaceId".to_owned())
    });
    scenario.verify::<TableInsertResult, _>(insert, |result| {
        (result.row_id > 0)
            .then_some(())
            .ok_or_else(|| "list parent row has no auto-assigned ID".to_owned())
    });
    scenario.verify::<ListCreateResult, _>(create, |result| {
        (result.space_id == space_id
            && result.table == TABLE
            && result.row_id == row_id
            && result.column == LIST_COLUMN
            && is_opaque_ref(&result.list_ref))
        .then_some(())
        .ok_or_else(|| "list.create returned no scoped Space list handle".to_owned())
    });
    scenario.verify::<ListAppendResult, _>(append, |result| {
        (result.space_id == space_id
            && result.list_ref == list_ref
            && is_opaque_ref(&result.item_ref))
        .then_some(())
        .ok_or_else(|| "list.append returned no SDK list item reference".to_owned())
    });
    scenario.verify::<ListReadResult, _>(read, |result| {
        (result.space_id == space_id
            && result.list_ref == list_ref
            && result.items.len() == 1
            && result.items[0].item_ref == item_ref
            && result.items[0].position == 0
            && result.items[0].value == item)
            .then_some(())
            .ok_or_else(|| "list.read did not return the appended item".to_owned())
    });
    scenario.finish();
}

#[test]
fn runtime_text_create_edit_read_round_trip() {
    let actor = "actor-text-parametric";
    let edited = "edited text parametric";
    let mut scenario = Scenario::start("text", &[actor]);
    let create_space = scenario.request(actor, "space.create", create_payload());
    let space_id = scenario.returned_string(create_space, "space_id", "missing-text-space");
    let insert = scenario.request(
        actor,
        "table.insert",
        json!({"space_id": space_id, "table": TABLE, "row": parent_row("text-parent-parametric", 89)}),
    );
    let row_id = scenario.observations[insert].response["result"]["row_id"]
        .as_i64()
        .unwrap_or(-1);
    let create = scenario.request(
        actor,
        "text.create",
        json!({"space_id": space_id, "table": TABLE, "row_id": row_id, "column": TEXT_COLUMN}),
    );
    let text_ref = scenario.returned_value(
        create,
        "text_ref",
        json!({"missing": "text-handle-reference"}),
    );
    let edit = scenario.request(
        actor,
        "text.edit",
        json!({
            "space_id": space_id,
            "text_ref": text_ref,
            "position": 0,
            "delete_count": 0,
            "insert": edited,
        }),
    );
    let read = scenario.request(
        actor,
        "text.read",
        json!({"space_id": space_id, "text_ref": text_ref}),
    );
    let out_of_range = scenario.request(
        actor,
        "text.edit",
        json!({
            "space_id": space_id,
            "text_ref": text_ref,
            "position": edited.chars().count() + 1,
            "delete_count": 0,
            "insert": "must-not-commit",
        }),
    );
    let read_after_rejection = scenario.request(
        actor,
        "text.read",
        json!({"space_id": space_id, "text_ref": text_ref}),
    );
    let wrong_kind = scenario.request(
        actor,
        "list.read",
        json!({"space_id": space_id, "list_ref": text_ref}),
    );

    scenario.verify::<SpaceCreateResult, _>(create_space, |result| {
        valid_created_space(result)
            .then_some(())
            .ok_or_else(|| "space.create returned an invalid SpaceId".to_owned())
    });
    scenario.verify::<TableInsertResult, _>(insert, |result| {
        (result.row_id > 0)
            .then_some(())
            .ok_or_else(|| "text parent row has no auto-assigned ID".to_owned())
    });
    scenario.verify::<TextCreateResult, _>(create, |result| {
        (result.space_id == space_id
            && result.table == TABLE
            && result.row_id == row_id
            && result.column == TEXT_COLUMN
            && is_opaque_ref(&result.text_ref))
        .then_some(())
        .ok_or_else(|| "text.create returned no scoped Space textarea handle".to_owned())
    });
    scenario.verify::<TextEditResult, _>(edit, |result| {
        (result.space_id == space_id && result.text_ref == text_ref && result.edited)
            .then_some(())
            .ok_or_else(|| "text.edit did not apply the positional edit".to_owned())
    });
    scenario.verify::<TextReadResult, _>(read, |result| {
        (result.space_id == space_id && result.text_ref == text_ref && result.text == edited)
            .then_some(())
            .ok_or_else(|| "text.read did not return the edited textarea".to_owned())
    });
    scenario.verify_error::<BridgeError, _>(out_of_range, |error| {
        (error.code == "INVALID_REQUEST")
            .then_some(())
            .ok_or_else(|| "text.edit did not reject an out-of-range edit preflight".to_owned())
    });
    scenario.verify::<TextReadResult, _>(read_after_rejection, |result| {
        (result.text == edited)
            .then_some(())
            .ok_or_else(|| "rejected text.edit changed the document".to_owned())
    });
    scenario.verify_error::<BridgeError, _>(wrong_kind, |error| {
        (error.code == "INVALID_REQUEST")
            .then_some(())
            .ok_or_else(|| "list.read accepted a text capability".to_owned())
    });
    scenario.finish();
}

#[test]
fn runtime_opaque_refs_cannot_cross_space_boundaries() {
    let actor = "actor-ref-space-boundary";
    let mut scenario = Scenario::start("ref-space-boundary", &[actor]);
    let first_create = scenario.request(actor, "space.create", create_payload());
    let first_space = scenario.returned_string(first_create, "space_id", "missing-first-space");
    let first_insert = scenario.request(
        actor,
        "table.insert",
        json!({
            "space_id": first_space,
            "table": TABLE,
            "row": parent_row("first-space-parent", 101),
        }),
    );
    let first_row_id = scenario.observations[first_insert].response["result"]["row_id"]
        .as_i64()
        .unwrap_or(-1);
    let list_create = scenario.request(
        actor,
        "list.create",
        json!({
            "space_id": first_space,
            "table": TABLE,
            "row_id": first_row_id,
            "column": LIST_COLUMN,
        }),
    );
    let stale_list_ref = scenario.returned_value(
        list_create,
        "list_ref",
        json!({"missing": "first-space-list-ref"}),
    );
    let text_create = scenario.request(
        actor,
        "text.create",
        json!({
            "space_id": first_space,
            "table": TABLE,
            "row_id": first_row_id,
            "column": TEXT_COLUMN,
        }),
    );
    let stale_text_ref = scenario.returned_value(
        text_create,
        "text_ref",
        json!({"missing": "first-space-text-ref"}),
    );

    let close = scenario.request(actor, "close", json!({}));
    let second_create = scenario.request(actor, "space.create", create_payload());
    let second_space = scenario.returned_string(second_create, "space_id", "missing-second-space");
    let second_insert = scenario.request(
        actor,
        "table.insert",
        json!({
            "space_id": second_space,
            "table": TABLE,
            "row": parent_row("second-space-parent", 202),
        }),
    );
    let stale_append = scenario.request(
        actor,
        "list.append",
        json!({
            "space_id": second_space,
            "list_ref": stale_list_ref,
            "value": {"must": "not-cross-space"},
        }),
    );
    let stale_edit = scenario.request(
        actor,
        "text.edit",
        json!({
            "space_id": second_space,
            "text_ref": stale_text_ref,
            "position": 0,
            "delete_count": 0,
            "insert": "must-not-cross-space",
        }),
    );

    scenario.verify::<CloseResult, _>(close, |result| {
        result
            .closed
            .then_some(())
            .ok_or_else(|| "close did not release the first Space".to_owned())
    });
    scenario.verify::<TableInsertResult, _>(second_insert, |result| {
        (result.row_id == first_row_id)
            .then_some(())
            .ok_or_else(|| "test did not reproduce matching row coordinates".to_owned())
    });
    scenario.verify_error::<BridgeError, _>(stale_append, |error| {
        (error.code == "INVALID_STATE")
            .then_some(())
            .ok_or_else(|| "stale list capability crossed the Space boundary".to_owned())
    });
    scenario.verify_error::<BridgeError, _>(stale_edit, |error| {
        (error.code == "INVALID_STATE")
            .then_some(())
            .ok_or_else(|| "stale text capability crossed the Space boundary".to_owned())
    });
    scenario.finish();
}

#[test]
fn runtime_file_put_get_round_trip_encrypted_bytes() {
    let actor = "actor-file-parametric";
    let bytes_base64 = "YnJpZGdlLWZpbGUtcGFyYW1ldHJpYw==";
    let mut scenario = Scenario::start("file", &[actor]);
    let create_space = scenario.request(actor, "space.create", create_payload());
    let space_id = scenario.returned_string(create_space, "space_id", "missing-file-space");
    let put = scenario.request(
        actor,
        "file.put",
        json!({"space_id": space_id, "bytes_base64": bytes_base64}),
    );
    let digest = scenario.returned_string(put, "digest", "missing-file-digest");
    let get = scenario.request(
        actor,
        "file.get",
        json!({"space_id": space_id, "digest": digest}),
    );

    scenario.verify::<SpaceCreateResult, _>(create_space, |result| {
        valid_created_space(result)
            .then_some(())
            .ok_or_else(|| "space.create returned an invalid SpaceId".to_owned())
    });
    scenario.verify::<FilePutResult, _>(put, |result| {
        (result.space_id == space_id && valid_digest(&result.digest))
            .then_some(())
            .ok_or_else(|| "file.put returned an invalid content digest".to_owned())
    });
    scenario.verify::<FileGetResult, _>(get, |result| {
        (result.space_id == space_id
            && result.digest == digest
            && result.bytes_base64 == bytes_base64)
            .then_some(())
            .ok_or_else(|| "file.get did not return the stored bytes and digest".to_owned())
    });
    scenario.finish();
}

#[test]
fn runtime_member_invite_join_remove_enforces_revocation() {
    let owner = "owner-membership-parametric";
    let member = "member-membership-parametric";
    let mut scenario = Scenario::start("membership", &[owner, member]);
    let create_space = scenario.request(owner, "space.create", create_payload());
    let space_id = scenario.returned_string(create_space, "space_id", "missing-member-space");
    let invite = scenario.request(owner, "member.invite", json!({"space_id": space_id}));
    let member_id = scenario.observations[invite].response["result"]["member_id"]
        .as_i64()
        .unwrap_or(-1);
    let invite_value =
        scenario.returned_value(invite, "invite", json!({"missing": "membership-invite"}));
    let join = scenario.request(member, "member.join", join_payload(invite_value));
    let retained_label = "owner-retained-after-removal-parametric";
    let retained_rank = 401;
    let retained_insert = scenario.request(
        owner,
        "table.insert",
        json!({
            "space_id": space_id,
            "table": TABLE,
            "row": parent_row(retained_label, retained_rank),
        }),
    );
    let retained_row_id = scenario.observations[retained_insert].response["result"]["row_id"]
        .as_i64()
        .unwrap_or(-1);
    let remove = scenario.request(
        owner,
        "member.remove",
        json!({"space_id": space_id, "member_id": member_id}),
    );
    let removed_select = scenario.request(
        member,
        "table.select",
        json!({
            "space_id": space_id,
            "table": TABLE,
            "where": {"id": retained_row_id},
        }),
    );
    let removed_write = scenario.request(
        member,
        "table.insert",
        json!({
            "space_id": space_id,
            "table": TABLE,
            "row": parent_row("removed-member-write-parametric", 409),
        }),
    );
    let owner_select = scenario.request(
        owner,
        "table.select",
        json!({
            "space_id": space_id,
            "table": TABLE,
            "where": {"id": retained_row_id},
        }),
    );

    scenario.verify::<SpaceCreateResult, _>(create_space, |result| {
        valid_created_space(result)
            .then_some(())
            .ok_or_else(|| "space.create returned an invalid SpaceId".to_owned())
    });
    scenario.verify::<InviteResult, _>(invite, |result| {
        (result.space_id == space_id && result.member_id > 0 && is_opaque_ref(&result.invite))
            .then_some(())
            .ok_or_else(|| "member.invite returned no numeric member ID or invite".to_owned())
    });
    scenario.verify::<JoinResult, _>(join, |result| {
        (result.space_id == space_id && result.member_id == member_id && result.joined)
            .then_some(())
            .ok_or_else(|| "member.join did not join the invited SDK member".to_owned())
    });
    scenario.verify::<TableInsertResult, _>(retained_insert, |result| {
        (result.row_id > 0)
            .then_some(())
            .ok_or_else(|| "removal fixture row has no auto-assigned ID".to_owned())
    });
    scenario.verify::<RemoveResult, _>(remove, |result| {
        (result.space_id == space_id && result.member_id == member_id && result.removed)
            .then_some(())
            .ok_or_else(|| "member.remove did not remove the joined member".to_owned())
    });
    scenario.verify_error::<BridgeError, _>(removed_select, |error| {
        (error.code == "ACCESS_DENIED")
            .then_some(())
            .ok_or_else(|| {
                format!(
                    "removed member select returned {}, expected ACCESS_DENIED",
                    error.code
                )
            })
    });
    scenario.verify_error::<BridgeError, _>(removed_write, |error| {
        (error.code == "ACCESS_DENIED")
            .then_some(())
            .ok_or_else(|| {
                format!(
                    "removed member write returned {}, expected ACCESS_DENIED",
                    error.code
                )
            })
    });
    scenario.verify::<TableSelectResult, _>(owner_select, |result| {
        (result.rows.len() == 1
            && row_matches(
                &result.rows[0],
                retained_row_id,
                retained_label,
                retained_rank,
            ))
        .then_some(())
        .ok_or_else(|| "owner could not read the row after member removal".to_owned())
    });
    scenario.finish();
}

const RELEASE_ARCHIVES: [&str; 8] = [
    "encrypted-spaces-backend-linux-amd64.tar.gz",
    "encrypted-spaces-backend-linux-arm64.tar.gz",
    "encrypted-spaces-backend-macos-amd64.tar.gz",
    "encrypted-spaces-backend-macos-arm64.tar.gz",
    "encrypted-spaces-bridge-linux-amd64.tar.gz",
    "encrypted-spaces-bridge-linux-arm64.tar.gz",
    "encrypted-spaces-bridge-macos-amd64.tar.gz",
    "encrypted-spaces-bridge-macos-arm64.tar.gz",
];

fn nonempty(path: &Path) -> bool {
    path.is_file() && fs::metadata(path).is_ok_and(|metadata| metadata.len() > 0)
}

fn assert_pinned_actions(workflow: &str) {
    let uses: Vec<&str> = workflow
        .lines()
        .map(str::trim)
        .filter_map(|line| line.strip_prefix("uses: "))
        .collect();
    assert!(!uses.is_empty(), "release workflow has no actions");
    for action in uses {
        let revision = action
            .rsplit_once('@')
            .map(|(_, revision)| revision)
            .expect("action revision");
        assert!(
            revision.len() == 40 && revision.bytes().all(|byte| byte.is_ascii_hexdigit()),
            "release action is not pinned to a commit: {action}"
        );
    }
}

#[test]
fn release_contract_builds_and_publishes_native_assets() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    let workflow = fs::read_to_string(root.join(".github/workflows/release-bridge.yml"))
        .expect("release workflow");
    let publisher = fs::read_to_string(root.join(".github/workflows/release-publish.yml"))
        .expect("trusted release publisher");
    let patches = fs::read_to_string(root.join("PATCHES.md")).expect("PATCHES ledger");
    let cargo = fs::read_to_string(root.join("Cargo.toml")).expect("workspace manifest");
    let lock = fs::read_to_string(root.join("Cargo.lock")).expect("workspace lockfile");
    let toolchain =
        fs::read_to_string(root.join("rust-toolchain.toml")).expect("pinned Rust toolchain");
    let bridge_manifest =
        fs::read_to_string(root.join("bridge/Cargo.toml")).expect("bridge manifest");
    let ffproof_manifest =
        fs::read_to_string(root.join("ffproof/Cargo.toml")).expect("ffproof manifest");
    let bridge_main = fs::read_to_string(root.join("bridge/src/main.rs")).expect("bridge main");
    let backend_config =
        fs::read_to_string(root.join("backend/server/src/app_config.rs")).expect("backend config");

    assert!(!workflow.contains("workflow_dispatch:"));
    assert!(!workflow.contains("pull_request:"));
    assert!(workflow.contains("branches:"));
    assert!(workflow.contains("- main"));
    assert!(workflow.contains("tags:"));
    assert!(workflow.contains("- 'v*'"));
    assert!(!workflow.contains("  publish:"));
    assert!(!workflow.contains("attestations: write"));
    assert!(!workflow.contains("id-token: write"));
    assert!(!workflow.contains("contents: write"));
    assert!(!workflow.contains("RISC0_SKIP_BUILD:"));
    assert_pinned_actions(&workflow);
    assert_pinned_actions(&publisher);

    let legal = workflow.find("  legal:").expect("legal job");
    let assets = workflow.find("  assets:").expect("asset matrix");
    let real_proof = workflow.find("  real-proof:").expect("real proof job");
    let aggregate = workflow.find("  aggregate:").expect("aggregate job");
    assert!(
        legal < assets && assets < real_proof && real_proof < aggregate,
        "release jobs have unexpected layout"
    );
    assert!(workflow[assets..real_proof].contains("needs: legal"));

    for marker in [
        "workflow_run:",
        "- Encrypted Spaces Release",
        "types: [completed]",
        "github.event.workflow_run.conclusion == 'success'",
        "github.event.workflow_run.event == 'push'",
        "github.event.workflow_run.id",
        "github.event.workflow_run.head_sha",
        "run-id:",
        "ref: main",
        "git fetch origin main:refs/remotes/origin/main",
        "git merge-base --is-ancestor \"$HEAD_SHA\" origin/main",
        "refs/tags/v$RELEASE_VERSION",
        "actions/attest@a1948c3f048ba23858d222213b7c278aabede763",
        "attestations: write",
        "id-token: write",
        "bundle-path",
        "release-manifest.json",
        "actions/download-artifact@",
        "actions/upload-artifact@",
        "gh release create",
        "  aggregate:",
        "  publish:",
    ] {
        assert!(publisher.contains(marker), "publisher omits {marker}");
    }
    assert!(!publisher.contains("workflow_dispatch:"));
    assert_eq!(
        publisher
            .matches("git merge-base --is-ancestor \"$HEAD_SHA\" origin/main")
            .count(),
        2,
        "main ancestry must be checked before attestation and publication"
    );

    for marker in [
        "RELEASE_READY: true",
        "RELEASE_VERSION: 0.1.0",
        "UPSTREAM_COMMIT: 4cda0ae87698135aa672990e6e68cf7873847426",
        "RUST_VERSION: 1.94.1",
        "RZUP_VERSION: 0.5.1",
        "RISC0_VERSION: 3.0.5",
        "RISC0_RUST_VERSION: 1.94.1",
        "cargo install rzup --version \"$RZUP_VERSION\" --locked",
        "rzup install cargo-risczero \"$RISC0_VERSION\"",
        "rzup install r0vm \"$RISC0_VERSION\"",
        "rzup install rust \"$RISC0_RUST_VERSION\"",
        "cargo risczero --version",
        "--features real-proofs",
        "ENCRYPTED_SPACES_BRIDGE_TEST_BINARY",
        "ENCRYPTED_SPACES_BACKEND_TEST_BINARY",
        "cargo test -p encrypted-spaces-bridge --locked --features real-proofs runtime_",
        "runtime_packaged_backend_generates_real_fast_forward_receipt",
        "ENCRYPTED_SPACES_REQUIRE_REAL_PROOF: 1",
        "ENCRYPTED_SPACES_REQUEST_TIMEOUT_MS: 3600000",
        "command -v r0vm",
        "env -u RISC0_DEV_MODE -u RISC0_SKIP_BUILD",
        "test -x",
        "cmp",
        "file -b",
        "--version",
        "tar -czf",
        "sha256sum",
        "shasum -a 256",
        "GITHUB_SHA",
        "actions/upload-artifact@",
        "actions/download-artifact@",
        "name: release-backend-linux-amd64",
        "name: release-bridge-linux-amd64",
        "tar -xzf dist/encrypted-spaces-backend-linux-amd64.tar.gz",
    ] {
        assert!(workflow.contains(marker), "release workflow omits {marker}");
    }
    assert!(!workflow.contains("github.event_name != 'pull_request'"));
    for archive in RELEASE_ARCHIVES {
        assert!(
            workflow.contains(archive),
            "release workflow omits {archive}"
        );
    }

    assert!(
        nonempty(&root.join("LICENSE")),
        "LICENSE is absent or empty"
    );
    assert!(nonempty(&root.join("NOTICE")), "NOTICE is absent or empty");
    assert!(cargo.contains("version = \"0.1.0\""));
    assert!(cargo.contains("kdl = { version = \"=6.5.0\""));
    assert!(bridge_manifest.contains("real-proofs = [\"encrypted-spaces-ffproof/real-proofs\"]"));
    assert!(ffproof_manifest.contains("real-proofs = [\"risc0-zkvm/prove\"]"));
    assert!(lock.contains("name = \"kdl\"\nversion = \"6.5.0\""));
    assert!(toolchain.contains("channel = \"1.94.1\""));
    assert!(toolchain.contains("\"rustfmt\""));
    assert!(toolchain.contains("\"clippy\""));
    assert!(bridge_main.contains("--version"));
    assert!(backend_config.contains("version"));
    assert!(patches.contains(UPSTREAM_COMMIT));
    assert!(patches.contains("800495f"));
    assert!(patches.contains(&format!("Rust `{RUST_TOOLCHAIN}`")));
    assert!(!patches.contains("Pending Release Work"));
    assert!(!patches.contains("NOT_IMPLEMENTED"));
}

#[test]
fn upstream_sync_opens_ci_gated_automerge_prs() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    let workflow = fs::read_to_string(root.join(".github/workflows/upstream-sync.yml"))
        .expect("upstream sync workflow");
    let compatibility = fs::read_to_string(root.join(".github/workflows/upstream-compat.yml"))
        .expect("trusted upstream compatibility workflow");

    assert!(workflow.contains("schedule:"));
    assert!(workflow.contains("cron:"));
    assert!(workflow.contains("workflow_dispatch:"));
    assert!(workflow.contains("contents: write"));
    assert!(workflow.contains("pull-requests: write"));
    assert!(workflow.contains("actions: write"));
    assert!(workflow.contains("https://github.com/encrypted-spaces/prototype.git"));
    assert!(workflow.contains("git fetch upstream main"));
    assert!(workflow.contains("upstream/main"));
    assert!(workflow.contains("gh pr create"));
    assert!(workflow.contains("git merge --no-edit upstream/main"));
    assert!(workflow.contains("git merge --abort"));
    assert!(workflow.contains("trigger_and_wait upstream-compat.yml"));
    assert!(workflow.contains("--ref main"));
    assert!(workflow.contains("candidate_sha"));
    assert!(workflow.contains("candidate_sha=\"$(git rev-parse \"origin/$branch\")\""));
    assert!(workflow.contains("correlation_id"));
    assert!(workflow.contains("--json databaseId,displayTitle,headSha"));
    assert!(workflow.contains("gh pr view \"$PR_URL\""));
    assert!(!workflow.contains("trigger_and_wait build-prototype.yml"));
    assert!(!workflow.contains("trigger_and_wait lint-prototype.yml"));
    assert!(workflow.contains("gh run watch"));
    assert!(workflow.contains("--force-with-lease=\"refs/heads/main:$FORK_MAIN_SHA\""));
    assert!(workflow.contains("$CANDIDATE_SHA:refs/heads/main"));
    assert!(!workflow.contains("gh pr merge"));
    assert!(!workflow.contains("--auto"));
    assert!(!workflow.contains("--squash"));
    assert!(workflow.contains("remains open"));
    assert_pinned_actions(&workflow);

    assert!(compatibility.contains("workflow_dispatch:"));
    assert!(compatibility.contains("candidate_sha:"));
    assert!(compatibility.contains("correlation_id:"));
    assert!(compatibility.contains("run-name:"));
    assert!(compatibility.contains("inputs.correlation_id"));
    assert!(compatibility.contains("permissions:\n  contents: read"));
    assert!(compatibility.contains("runs-on: ubuntu-24.04"));
    assert!(compatibility.contains("ref: ${{ inputs.candidate_sha }}"));
    assert!(compatibility.contains("fetch-depth: 0"));
    assert!(compatibility.contains("git merge-base --is-ancestor \"$FORK_MAIN_SHA\" HEAD"));
    assert!(compatibility.contains("git merge-base --is-ancestor \"$UPSTREAM_SHA\" HEAD"));
    assert!(compatibility.contains("persist-credentials: false"));
    assert!(!compatibility.contains("secrets."));
    assert!(compatibility.contains("cargo fmt --all -- --check"));
    assert!(
        compatibility.contains("cargo clippy --workspace --all-targets --locked -- -D warnings")
    );
    assert!(compatibility.contains("cargo test --workspace --locked"));
    assert_pinned_actions(&compatibility);
}
