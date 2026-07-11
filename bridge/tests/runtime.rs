use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::{json, Value};
use std::collections::BTreeMap;
use std::fs;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};

const PROTOCOL_VERSION: u64 = 1;
const UPSTREAM_COMMIT: &str = "4cda0ae87698135aa672990e6e68cf7873847426";
const RUST_TOOLCHAIN: &str = "1.94.1";

struct BridgeProcess {
    child: Child,
    stdin: Option<ChildStdin>,
    stdout: BufReader<ChildStdout>,
    scenario: String,
    next_request: usize,
}

impl BridgeProcess {
    fn spawn(scenario: &str, actor: &str) -> Self {
        let mut child = Command::new(env!("CARGO_BIN_EXE_encrypted-spaces-bridge"))
            .env("ENCRYPTED_SPACES_ACTOR_ID", actor)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .spawn()
            .expect("spawn bridge");
        let stdin = child.stdin.take().expect("bridge stdin");
        let stdout = BufReader::new(child.stdout.take().expect("bridge stdout"));
        Self {
            child,
            stdin: Some(stdin),
            stdout,
            scenario: format!("{scenario}-{actor}"),
            next_request: 1,
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

        let mut line = String::new();
        self.stdout
            .read_line(&mut line)
            .expect("read bridge response");
        assert!(
            !line.is_empty(),
            "bridge exited before responding to {operation}"
        );
        let response = serde_json::from_str(&line).expect("bridge response JSON");
        Observation {
            operation: operation.to_owned(),
            request_id,
            response,
        }
    }

    fn finish(mut self) {
        drop(self.stdin.take());
        let status = self.child.wait().expect("wait for bridge");
        assert!(status.success(), "bridge exited with {status}");
    }
}

struct Observation {
    operation: String,
    request_id: String,
    response: Value,
}

struct Scenario {
    bridges: BTreeMap<String, BridgeProcess>,
    observations: Vec<Observation>,
    failures: Vec<String>,
}

impl Scenario {
    fn start(name: &str, actors: &[&str]) -> Self {
        let bridges = actors
            .iter()
            .map(|actor| ((*actor).to_owned(), BridgeProcess::spawn(name, actor)))
            .collect();
        Self {
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
        if actual_request_id != Some(observation.request_id.as_str()) {
            self.failures.push(format!(
                "{}: request_id {:?}, expected {}",
                observation.operation, actual_request_id, observation.request_id
            ));
        }
        self.observations.push(observation);
        self.observations.len() - 1
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
}

#[derive(Deserialize)]
struct SpaceCreateResult {
    space_id: String,
    schema: Value,
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
    member_id: String,
    invite: Value,
}

#[derive(Deserialize)]
struct JoinResult {
    space_id: String,
    member_id: String,
    joined: bool,
}

#[derive(Deserialize)]
struct SyncResult {
    space_id: String,
    change_id: u64,
}

#[derive(Deserialize)]
struct RemoveResult {
    space_id: String,
    member_id: String,
    removed: bool,
}

#[derive(Deserialize)]
struct TableInsertResult {
    space_id: String,
    table: String,
    row_id: Value,
    row: Value,
}

#[derive(Deserialize)]
struct TableSelectResult {
    space_id: String,
    table: String,
    rows: Vec<Value>,
}

#[derive(Deserialize)]
struct ListCreateResult {
    space_id: String,
    list_id: String,
    name: String,
}

#[derive(Deserialize)]
struct ListAppendResult {
    space_id: String,
    list_id: String,
    index: usize,
    value: Value,
}

#[derive(Deserialize)]
struct ListReadResult {
    space_id: String,
    list_id: String,
    items: Vec<Value>,
}

#[derive(Deserialize)]
struct TextResult {
    space_id: String,
    text_id: String,
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

fn concrete_schema(name: &str) -> Value {
    json!({
        "tables": [{
            "name": name,
            "columns": [
                {"name": "id", "type": "integer", "primary_key": true},
                {"name": "label", "type": "text", "nullable": false}
            ]
        }]
    })
}

fn valid_digest(digest: &str) -> bool {
    digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit())
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
fn runtime_space_lifecycle_is_red() {
    let owner = "owner-lifecycle-parametric";
    let member = "member-lifecycle-parametric";
    let schema = concrete_schema("lifecycle_records");
    let mut scenario = Scenario::start("lifecycle", &[owner, member]);

    let hello = scenario.request(owner, "hello", json!({}));
    let create = scenario.request(owner, "space.create", json!({"schema": schema}));
    let space_id = scenario.returned_string(create, "space_id", "missing-lifecycle-space");
    let snapshot = scenario.request(owner, "space.snapshot", json!({"space_id": space_id}));
    let snapshot_value = scenario.returned_value(
        snapshot,
        "snapshot",
        json!({"missing": "lifecycle-snapshot"}),
    );
    let restore = scenario.request(
        owner,
        "space.restore",
        json!({"space_id": space_id, "snapshot": snapshot_value}),
    );
    let invite = scenario.request(
        owner,
        "member.invite",
        json!({"space_id": space_id, "member_id": member}),
    );
    let invite_value =
        scenario.returned_value(invite, "invite", json!({"missing": "lifecycle-invite"}));
    let join = scenario.request(
        member,
        "space.join",
        json!({"space_id": space_id, "invite": invite_value}),
    );
    let sync = scenario.request(member, "space.sync", json!({"space_id": space_id}));
    let remove = scenario.request(
        owner,
        "member.remove",
        json!({"space_id": space_id, "member_id": member}),
    );

    scenario.verify::<HelloResult, _>(hello, |result| {
        (result.protocol_version == PROTOCOL_VERSION)
            .then_some(())
            .ok_or_else(|| "hello returned the wrong protocol version".to_owned())
    });
    scenario.verify::<SpaceCreateResult, _>(create, |result| {
        (!result.space_id.is_empty() && result.schema == schema)
            .then_some(())
            .ok_or_else(|| "space.create did not return its concrete schema".to_owned())
    });
    scenario.verify::<SnapshotResult, _>(snapshot, |result| {
        (result.space_id == space_id && !result.snapshot.is_null())
            .then_some(())
            .ok_or_else(|| "space.snapshot returned the wrong space or no snapshot".to_owned())
    });
    scenario.verify::<RestoreResult, _>(restore, |result| {
        (result.space_id == space_id && result.restored)
            .then_some(())
            .ok_or_else(|| "space.restore did not restore the returned snapshot".to_owned())
    });
    scenario.verify::<InviteResult, _>(invite, |result| {
        (result.space_id == space_id && result.member_id == member && !result.invite.is_null())
            .then_some(())
            .ok_or_else(|| "member.invite returned an invalid invite".to_owned())
    });
    scenario.verify::<JoinResult, _>(join, |result| {
        (result.space_id == space_id && result.member_id == member && result.joined)
            .then_some(())
            .ok_or_else(|| "space.join did not consume the returned invite".to_owned())
    });
    scenario.verify::<SyncResult, _>(sync, |result| {
        (result.space_id == space_id && result.change_id > 0)
            .then_some(())
            .ok_or_else(|| "space.sync returned no applied change".to_owned())
    });
    scenario.verify::<RemoveResult, _>(remove, |result| {
        (result.space_id == space_id && result.member_id == member && result.removed)
            .then_some(())
            .ok_or_else(|| "member.remove did not remove the joined member".to_owned())
    });
    scenario.finish();
}

#[test]
fn runtime_table_insert_select_are_red() {
    let actor = "actor-table-parametric";
    let schema = concrete_schema("records_parametric");
    let row = json!({"id": 41, "label": "table-value-parametric"});
    let mut scenario = Scenario::start("table", &[actor]);
    let create = scenario.request(actor, "space.create", json!({"schema": schema}));
    let space_id = scenario.returned_string(create, "space_id", "missing-table-space");
    let insert = scenario.request(
        actor,
        "table.insert",
        json!({"space_id": space_id, "table": "records_parametric", "row": row}),
    );
    let row_id = scenario.returned_value(insert, "row_id", json!(41));
    let select = scenario.request(
        actor,
        "table.select",
        json!({"space_id": space_id, "table": "records_parametric", "where": {"id": row_id}}),
    );

    scenario.verify::<SpaceCreateResult, _>(create, |result| {
        (!result.space_id.is_empty() && result.schema == schema)
            .then_some(())
            .ok_or_else(|| "space.create did not return the table schema".to_owned())
    });
    scenario.verify::<TableInsertResult, _>(insert, |result| {
        (result.space_id == space_id
            && result.table == "records_parametric"
            && result.row_id == json!(41)
            && result.row == row)
            .then_some(())
            .ok_or_else(|| "table.insert returned the wrong typed row".to_owned())
    });
    scenario.verify::<TableSelectResult, _>(select, |result| {
        (result.space_id == space_id
            && result.table == "records_parametric"
            && result.rows == vec![row])
        .then_some(())
        .ok_or_else(|| "table.select did not return the inserted row".to_owned())
    });
    scenario.finish();
}

#[test]
fn runtime_list_create_append_read_are_red() {
    let actor = "actor-list-parametric";
    let schema = concrete_schema("list_records_parametric");
    let item = json!({"key": "list-key-parametric", "value": 73});
    let mut scenario = Scenario::start("list", &[actor]);
    let create_space = scenario.request(actor, "space.create", json!({"schema": schema}));
    let space_id = scenario.returned_string(create_space, "space_id", "missing-list-space");
    let create = scenario.request(
        actor,
        "list.create",
        json!({"space_id": space_id, "name": "queue_parametric"}),
    );
    let list_id = scenario.returned_string(create, "list_id", "missing-list-id");
    let append = scenario.request(
        actor,
        "list.append",
        json!({"space_id": space_id, "list_id": list_id, "value": item}),
    );
    let read = scenario.request(
        actor,
        "list.read",
        json!({"space_id": space_id, "list_id": list_id}),
    );

    scenario.verify::<SpaceCreateResult, _>(create_space, |result| {
        (!result.space_id.is_empty() && result.schema == schema)
            .then_some(())
            .ok_or_else(|| "space.create did not return the list schema".to_owned())
    });
    scenario.verify::<ListCreateResult, _>(create, |result| {
        (result.space_id == space_id
            && !result.list_id.is_empty()
            && result.name == "queue_parametric")
            .then_some(())
            .ok_or_else(|| "list.create returned the wrong list".to_owned())
    });
    scenario.verify::<ListAppendResult, _>(append, |result| {
        (result.space_id == space_id
            && result.list_id == list_id
            && result.index == 0
            && result.value == item)
            .then_some(())
            .ok_or_else(|| "list.append returned the wrong item".to_owned())
    });
    scenario.verify::<ListReadResult, _>(read, |result| {
        (result.space_id == space_id && result.list_id == list_id && result.items == vec![item])
            .then_some(())
            .ok_or_else(|| "list.read did not return the appended item".to_owned())
    });
    scenario.finish();
}

#[test]
fn runtime_text_create_edit_read_are_red() {
    let actor = "actor-text-parametric";
    let schema = concrete_schema("text_records_parametric");
    let initial = "initial text parametric";
    let edited = "edited text parametric";
    let mut scenario = Scenario::start("text", &[actor]);
    let create_space = scenario.request(actor, "space.create", json!({"schema": schema}));
    let space_id = scenario.returned_string(create_space, "space_id", "missing-text-space");
    let create = scenario.request(
        actor,
        "text.create",
        json!({"space_id": space_id, "name": "document_parametric", "text": initial}),
    );
    let text_id = scenario.returned_string(create, "text_id", "missing-text-id");
    let edit = scenario.request(
        actor,
        "text.edit",
        json!({"space_id": space_id, "text_id": text_id, "text": edited}),
    );
    let read = scenario.request(
        actor,
        "text.read",
        json!({"space_id": space_id, "text_id": text_id}),
    );

    scenario.verify::<SpaceCreateResult, _>(create_space, |result| {
        (!result.space_id.is_empty() && result.schema == schema)
            .then_some(())
            .ok_or_else(|| "space.create did not return the text schema".to_owned())
    });
    scenario.verify::<TextResult, _>(create, |result| {
        (result.space_id == space_id && !result.text_id.is_empty() && result.text == initial)
            .then_some(())
            .ok_or_else(|| "text.create returned the wrong document".to_owned())
    });
    scenario.verify::<TextResult, _>(edit, |result| {
        (result.space_id == space_id && result.text_id == text_id && result.text == edited)
            .then_some(())
            .ok_or_else(|| "text.edit returned the wrong document".to_owned())
    });
    scenario.verify::<TextResult, _>(read, |result| {
        (result.space_id == space_id && result.text_id == text_id && result.text == edited)
            .then_some(())
            .ok_or_else(|| "text.read did not return the edited document".to_owned())
    });
    scenario.finish();
}

#[test]
fn runtime_file_put_get_are_red() {
    let actor = "actor-file-parametric";
    let schema = concrete_schema("file_records_parametric");
    let bytes_base64 = "YnJpZGdlLWZpbGUtcGFyYW1ldHJpYw==";
    let mut scenario = Scenario::start("file", &[actor]);
    let create_space = scenario.request(actor, "space.create", json!({"schema": schema}));
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
        (!result.space_id.is_empty() && result.schema == schema)
            .then_some(())
            .ok_or_else(|| "space.create did not return the file schema".to_owned())
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
fn runtime_member_invite_join_remove_are_red() {
    let owner = "owner-membership-parametric";
    let member = "member-membership-parametric";
    let schema = concrete_schema("membership_records_parametric");
    let mut scenario = Scenario::start("membership", &[owner, member]);
    let create_space = scenario.request(owner, "space.create", json!({"schema": schema}));
    let space_id = scenario.returned_string(create_space, "space_id", "missing-member-space");
    let invite = scenario.request(
        owner,
        "member.invite",
        json!({"space_id": space_id, "member_id": member}),
    );
    let invite_value =
        scenario.returned_value(invite, "invite", json!({"missing": "membership-invite"}));
    let join = scenario.request(
        member,
        "member.join",
        json!({"space_id": space_id, "invite": invite_value}),
    );
    let remove = scenario.request(
        owner,
        "member.remove",
        json!({"space_id": space_id, "member_id": member}),
    );

    scenario.verify::<SpaceCreateResult, _>(create_space, |result| {
        (!result.space_id.is_empty() && result.schema == schema)
            .then_some(())
            .ok_or_else(|| "space.create did not return the membership schema".to_owned())
    });
    scenario.verify::<InviteResult, _>(invite, |result| {
        (result.space_id == space_id && result.member_id == member && !result.invite.is_null())
            .then_some(())
            .ok_or_else(|| "member.invite returned the wrong member invite".to_owned())
    });
    scenario.verify::<JoinResult, _>(join, |result| {
        (result.space_id == space_id && result.member_id == member && result.joined)
            .then_some(())
            .ok_or_else(|| "member.join did not join the invited member".to_owned())
    });
    scenario.verify::<RemoveResult, _>(remove, |result| {
        (result.space_id == space_id && result.member_id == member && result.removed)
            .then_some(())
            .ok_or_else(|| "member.remove did not remove the joined member".to_owned())
    });
    scenario.finish();
}

#[derive(Deserialize)]
struct ReleaseManifest {
    upstream_commit: String,
    rust_toolchain: String,
    artifacts: Vec<ReleaseArtifact>,
}

#[derive(Deserialize)]
struct ReleaseArtifact {
    component: String,
    target: String,
    archive: String,
    sha256: String,
}

#[derive(Deserialize)]
struct ProvenanceStatement {
    #[serde(rename = "_type")]
    statement_type: String,
    #[serde(rename = "predicateType")]
    predicate_type: String,
    subject: Vec<ProvenanceSubject>,
    predicate: ProvenancePredicate,
}

#[derive(Deserialize)]
struct ProvenanceSubject {
    name: String,
    digest: ProvenanceDigest,
}

#[derive(Deserialize)]
struct ProvenanceDigest {
    sha256: String,
}

#[derive(Deserialize)]
struct ProvenancePredicate {
    #[serde(rename = "buildDefinition")]
    build_definition: ProvenanceBuildDefinition,
}

#[derive(Deserialize)]
struct ProvenanceBuildDefinition {
    #[serde(rename = "externalParameters")]
    external_parameters: ProvenanceParameters,
}

#[derive(Deserialize)]
struct ProvenanceParameters {
    upstream_commit: String,
    rust_toolchain: String,
}

const RELEASE_ARCHIVES: [(&str, &str, &str); 8] = [
    (
        "backend",
        "linux-amd64",
        "encrypted-spaces-backend-linux-amd64.tar.gz",
    ),
    (
        "backend",
        "linux-arm64",
        "encrypted-spaces-backend-linux-arm64.tar.gz",
    ),
    (
        "backend",
        "macos-amd64",
        "encrypted-spaces-backend-macos-amd64.tar.gz",
    ),
    (
        "backend",
        "macos-arm64",
        "encrypted-spaces-backend-macos-arm64.tar.gz",
    ),
    (
        "bridge",
        "linux-amd64",
        "encrypted-spaces-bridge-linux-amd64.tar.gz",
    ),
    (
        "bridge",
        "linux-arm64",
        "encrypted-spaces-bridge-linux-arm64.tar.gz",
    ),
    (
        "bridge",
        "macos-amd64",
        "encrypted-spaces-bridge-macos-amd64.tar.gz",
    ),
    (
        "bridge",
        "macos-arm64",
        "encrypted-spaces-bridge-macos-arm64.tar.gz",
    ),
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

fn verify_checksum(dist: &Path, archive: &str, expected_digest: &str) {
    let checksum = dist.join("checksums").join(format!("{archive}.sha256"));
    let contents = fs::read_to_string(&checksum).expect("checksum file");
    let mut fields = contents.split_whitespace();
    assert_eq!(
        fields.next(),
        Some(expected_digest),
        "{archive} checksum digest"
    );
    assert_eq!(
        fields.next().map(|name| name.trim_start_matches('*')),
        Some(archive),
        "{archive} checksum filename"
    );
    assert!(
        fields.next().is_none(),
        "{archive} checksum has extra fields"
    );

    let status = if cfg!(target_os = "linux") {
        Command::new("sha256sum")
            .args(["-c", &format!("checksums/{archive}.sha256")])
            .current_dir(dist)
            .status()
    } else if cfg!(target_os = "macos") {
        Command::new("shasum")
            .args(["-a", "256", "-c", &format!("checksums/{archive}.sha256")])
            .current_dir(dist)
            .status()
    } else {
        panic!("release checksum verification requires Linux or macOS");
    }
    .expect("execute checksum verifier");
    assert!(
        status.success(),
        "checksum verification failed for {archive}"
    );
}

fn inspect_archive(dist: &Path, component: &str, archive: &str) {
    let output = Command::new("tar")
        .args(["-tzf", archive])
        .current_dir(dist)
        .output()
        .expect("list release archive");
    assert!(
        output.status.success(),
        "cannot list {archive}: {}",
        String::from_utf8_lossy(&output.stderr)
    );
    let listing = String::from_utf8(output.stdout).expect("archive listing is UTF-8");
    let binary = format!("encrypted-spaces-{component}");
    for required in [binary.as_str(), "LICENSE", "NOTICE"] {
        assert!(
            listing
                .lines()
                .any(|entry| entry.trim_end_matches('/').rsplit('/').next() == Some(required)),
            "{archive} omits {required}"
        );
    }
}

fn verify_provenance(dist: &Path, archive: &str, expected_digest: &str) {
    let path = dist
        .join("provenance")
        .join(format!("{archive}.intoto.jsonl"));
    let contents = fs::read_to_string(path).expect("provenance JSONL");
    let statements: Vec<ProvenanceStatement> = serde_json::Deserializer::from_str(&contents)
        .into_iter()
        .collect::<Result<_, _>>()
        .expect("parse provenance JSONL");
    assert!(!statements.is_empty(), "{archive} provenance is empty");
    assert!(
        statements.iter().any(|statement| {
            statement.statement_type == "https://in-toto.io/Statement/v1"
                && statement.predicate_type == "https://slsa.dev/provenance/v1"
                && statement.subject.iter().any(|subject| {
                    subject.name == archive && subject.digest.sha256 == expected_digest
                })
                && statement
                    .predicate
                    .build_definition
                    .external_parameters
                    .upstream_commit
                    == UPSTREAM_COMMIT
                && statement
                    .predicate
                    .build_definition
                    .external_parameters
                    .rust_toolchain
                    == RUST_TOOLCHAIN
        }),
        "{archive} provenance does not bind its digest, upstream commit, and toolchain"
    );
}

#[test]
fn release_contract_is_red_until_dist_and_notice_exist() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("..");
    let dist = root.join("dist");
    let workflow = fs::read_to_string(root.join(".github/workflows/release-bridge.yml"))
        .expect("release contract workflow");
    let patches = fs::read_to_string(root.join("PATCHES.md")).expect("PATCHES ledger");
    let cargo = fs::read_to_string(root.join("Cargo.toml")).expect("workspace manifest");
    let lock = fs::read_to_string(root.join("Cargo.lock")).expect("workspace lockfile");

    assert!(workflow.contains("workflow_dispatch:"));
    assert!(
        !workflow.contains("\n  push:"),
        "release workflow must be manual-only"
    );
    assert!(
        !workflow.contains("\n  release:"),
        "release workflow must not publish"
    );
    assert_pinned_actions(&workflow);
    let legal = workflow.find("  legal:").expect("independent legal job");
    let assets = workflow.find("  assets:").expect("independent asset job");
    let aggregate = workflow.find("  aggregate:").expect("aggregate job");
    assert!(
        legal < assets && assets < aggregate,
        "release jobs have unexpected layout"
    );
    assert!(
        !workflow[assets..aggregate].contains("needs:"),
        "asset checks must not depend on the legal gate"
    );
    for marker in [
        "needs: [legal, assets]",
        "if: ${{ always() }}",
        "sha256sum -c",
        "shasum -a 256 -c",
        "jq -e",
        "tar -tzf",
        "RISC0_SKIP_BUILD: 1",
    ] {
        assert!(workflow.contains(marker), "release workflow omits {marker}");
    }
    for (_, _, archive) in RELEASE_ARCHIVES {
        assert!(
            workflow.contains(archive),
            "release workflow omits {archive}"
        );
    }
    assert!(cargo.contains("kdl = { version = \"=6.5.0\""));
    assert!(lock.contains("name = \"kdl\"\nversion = \"6.5.0\""));
    assert!(patches.contains(UPSTREAM_COMMIT));
    assert!(patches.contains("800495f"));
    assert!(patches.contains(&format!("Rust `{RUST_TOOLCHAIN}`")));

    let mut missing = Vec::new();
    let notice = root.join("NOTICE");
    if !nonempty(&notice) {
        missing.push("NOTICE".to_owned());
    }
    let manifest_path = dist.join("release-manifest.json");
    if !nonempty(&manifest_path) {
        missing.push("dist/release-manifest.json".to_owned());
    }
    for (_, _, archive) in RELEASE_ARCHIVES {
        for path in [
            dist.join(archive),
            dist.join("checksums").join(format!("{archive}.sha256")),
            dist.join("provenance")
                .join(format!("{archive}.intoto.jsonl")),
        ] {
            if !nonempty(&path) {
                missing.push(
                    path.strip_prefix(&root)
                        .unwrap_or(&path)
                        .display()
                        .to_string(),
                );
            }
        }
    }
    assert!(
        missing.is_empty(),
        "release contract RED: missing legal/release artifacts: {}",
        missing.join(", ")
    );

    assert!(
        nonempty(&root.join("LICENSE")),
        "LICENSE is absent or empty"
    );
    let manifest: ReleaseManifest =
        serde_json::from_slice(&fs::read(&manifest_path).expect("read release manifest"))
            .expect("parse release manifest");
    assert_eq!(manifest.upstream_commit, UPSTREAM_COMMIT);
    assert_eq!(manifest.rust_toolchain, RUST_TOOLCHAIN);
    assert_eq!(manifest.artifacts.len(), RELEASE_ARCHIVES.len());
    for (component, target, archive) in RELEASE_ARCHIVES {
        let artifact = manifest
            .artifacts
            .iter()
            .find(|artifact| artifact.archive == archive)
            .unwrap_or_else(|| panic!("release manifest omits {archive}"));
        assert_eq!(artifact.component, component, "{archive} component");
        assert_eq!(artifact.target, target, "{archive} target");
        assert!(valid_digest(&artifact.sha256), "{archive} invalid sha256");
        verify_checksum(&dist, archive, &artifact.sha256);
        verify_provenance(&dist, archive, &artifact.sha256);
        inspect_archive(&dist, component, archive);
    }
}
