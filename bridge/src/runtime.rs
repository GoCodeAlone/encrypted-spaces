use crate::protocol::Response;
use crate::schema::{Operation, Request, PROTOCOL_VERSION};
use base64::{
    engine::general_purpose::{STANDARD, URL_SAFE_NO_PAD},
    Engine,
};
use encrypted_spaces_sdk::{
    ApplicationSchema, File, LocalTransport, SdkErrorType, Space, SpaceInvite, WebSocketTransport,
};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::fs;
use std::future::Future;
use std::io;
use std::time::Duration;

const CLIENT_LABEL_ENV: &str = "ENCRYPTED_SPACES_CLIENT_LABEL";
const SCHEMA_PATH_ENV: &str = "ENCRYPTED_SPACES_SCHEMA_PATH";
const BACKEND_URL_ENV: &str = "ENCRYPTED_SPACES_BACKEND_URL";
const REQUEST_TIMEOUT_MS_ENV: &str = "ENCRYPTED_SPACES_REQUEST_TIMEOUT_MS";
const DEFAULT_BACKEND_URL: &str = "ws://127.0.0.1:8080/ws";
const DEFAULT_REQUEST_TIMEOUT_MS: u64 = 30_000;
const MAX_REQUEST_TIMEOUT_MS: u64 = 3_600_000;

pub struct Runtime {
    executor: tokio::runtime::Runtime,
    process: ProcessConfig,
    space: Option<Space>,
    shutdown_requested: bool,
}

struct ProcessConfig {
    client_label: String,
    schema_sha256: String,
    schema: Vec<u8>,
    data_commitment_bytes: [u8; 32],
    data_commitment: String,
    ff_guest_image_id: [u32; 8],
    backend_url: String,
    request_timeout: Duration,
}

#[derive(Serialize)]
struct HelloResult<'a> {
    protocol_version: u16,
    client_label: &'a str,
    schema_sha256: &'a str,
    data_commitment: &'a str,
    ff_guest_image_id: [u32; 8],
}

#[derive(Serialize)]
struct VersionResult {
    version: &'static str,
    protocol_version: u16,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EmptyPayload {}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SpacePayload {
    space_id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RestorePayload {
    snapshot: Value,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct SyncPayload {
    space_id: String,
    #[serde(default)]
    wait_for_change_ms: Option<u64>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TableInsertPayload {
    space_id: String,
    table: String,
    row: Value,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TableSelectPayload {
    space_id: String,
    table: String,
    #[serde(rename = "where")]
    where_clause: serde_json::Map<String, Value>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ListCreatePayload {
    space_id: String,
    table: String,
    row_id: i64,
    column: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ListAppendPayload {
    space_id: String,
    list_ref: String,
    value: Value,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ListReadPayload {
    space_id: String,
    list_ref: String,
}

#[derive(Clone, Copy, Deserialize, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
enum RefKind {
    List,
    Text,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ScopedRef {
    space_id: String,
    kind: RefKind,
    table: String,
    row_id: i64,
    column: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TextCreatePayload {
    space_id: String,
    table: String,
    row_id: i64,
    column: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TextEditPayload {
    space_id: String,
    text_ref: String,
    position: usize,
    delete_count: usize,
    insert: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TextReadPayload {
    space_id: String,
    text_ref: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FilePutPayload {
    space_id: String,
    bytes_base64: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct FileGetPayload {
    space_id: String,
    digest: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct JoinPayload {
    invite: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MemberPayload {
    space_id: String,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RemovePayload {
    space_id: String,
    member_id: i64,
}

#[derive(Serialize)]
struct CreateResult<'a> {
    space_id: String,
    schema_sha256: &'a str,
}

#[derive(Serialize)]
struct SnapshotResult {
    space_id: String,
    snapshot: Value,
}

#[derive(Serialize)]
struct RestoreResult {
    space_id: String,
    restored: bool,
}

#[derive(Serialize)]
struct SyncResult {
    space_id: String,
    synced: bool,
}

#[derive(Serialize)]
struct TableInsertResult {
    row_id: i64,
}

#[derive(Serialize)]
struct TableSelectResult {
    rows: Vec<Value>,
}

#[derive(Serialize)]
struct ListCreateResult {
    space_id: String,
    table: String,
    row_id: i64,
    column: String,
    list_ref: String,
}

#[derive(Serialize)]
struct ListAppendResult {
    space_id: String,
    list_ref: String,
    item_ref: String,
}

#[derive(Serialize)]
struct ListItemResult {
    item_ref: String,
    position: u64,
    value: Value,
}

#[derive(Serialize)]
struct ListReadResult {
    space_id: String,
    list_ref: String,
    items: Vec<ListItemResult>,
}

#[derive(Serialize)]
struct TextCreateResult {
    space_id: String,
    table: String,
    row_id: i64,
    column: String,
    text_ref: String,
}

#[derive(Serialize)]
struct TextEditResult {
    space_id: String,
    text_ref: String,
    edited: bool,
}

#[derive(Serialize)]
struct TextReadResult {
    space_id: String,
    text_ref: String,
    text: String,
}

#[derive(Serialize)]
struct FilePutResult {
    space_id: String,
    digest: String,
}

#[derive(Serialize)]
struct FileGetResult {
    space_id: String,
    digest: String,
    bytes_base64: String,
}

#[derive(Serialize)]
struct InviteResult {
    space_id: String,
    member_id: i64,
    invite: String,
}

#[derive(Serialize)]
struct JoinResult {
    space_id: String,
    member_id: i64,
    joined: bool,
}

#[derive(Serialize)]
struct RemoveResult {
    space_id: String,
    member_id: i64,
    removed: bool,
}

#[derive(Serialize)]
struct CloseResult {
    closed: bool,
}

#[derive(Serialize)]
struct ShutdownResult {
    shutting_down: bool,
}

impl Runtime {
    pub fn from_env() -> io::Result<Self> {
        let client_label = required_env(CLIENT_LABEL_ENV)?;
        let schema_path = required_env(SCHEMA_PATH_ENV)?;
        let schema = fs::read(&schema_path)?;
        let schema_sha256 = format!("{:x}", Sha256::digest(&schema));
        let executor = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(io::Error::other)?;
        let data_commitment_bytes = executor
            .block_on(async {
                let transport = LocalTransport::from_schema_file(&schema_path).await?;
                transport.get_root_hash().await
            })
            .map_err(io::Error::other)?;
        let data_commitment = data_commitment_bytes
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        let backend_url = std::env::var(BACKEND_URL_ENV)
            .ok()
            .filter(|value| !value.is_empty())
            .unwrap_or_else(|| DEFAULT_BACKEND_URL.to_owned());
        let request_timeout = request_timeout_from_env()?;

        Ok(Self {
            executor,
            process: ProcessConfig {
                client_label,
                schema_sha256,
                schema,
                data_commitment_bytes,
                data_commitment,
                ff_guest_image_id: encrypted_spaces_ffproof::EXTEND_FF_ID,
                backend_url,
                request_timeout,
            },
            space: None,
            shutdown_requested: false,
        })
    }

    pub fn dispatch(&mut self, request: Request) -> Response {
        match request.operation {
            Operation::Hello => Response::success(
                request.request_id,
                HelloResult {
                    protocol_version: PROTOCOL_VERSION,
                    client_label: &self.process.client_label,
                    schema_sha256: &self.process.schema_sha256,
                    data_commitment: &self.process.data_commitment,
                    ff_guest_image_id: self.process.ff_guest_image_id,
                },
            ),
            Operation::Version => Response::success(
                request.request_id,
                VersionResult {
                    version: env!("CARGO_PKG_VERSION"),
                    protocol_version: PROTOCOL_VERSION,
                },
            ),
            Operation::Create => self.create(request.request_id, request.payload),
            Operation::Snapshot => self.snapshot(request.request_id, request.payload),
            Operation::Restore => self.restore(request.request_id, request.payload),
            Operation::Sync => self.sync(request.request_id, request.payload),
            Operation::TableInsert => self.table_insert(request.request_id, request.payload),
            Operation::TableSelect => self.table_select(request.request_id, request.payload),
            Operation::ListCreate => self.list_create(request.request_id, request.payload),
            Operation::ListAppend => self.list_append(request.request_id, request.payload),
            Operation::ListRead => self.list_read(request.request_id, request.payload),
            Operation::TextCreate => self.text_create(request.request_id, request.payload),
            Operation::TextEdit => self.text_edit(request.request_id, request.payload),
            Operation::TextRead => self.text_read(request.request_id, request.payload),
            Operation::FilePut => self.file_put(request.request_id, request.payload),
            Operation::FileGet => self.file_get(request.request_id, request.payload),
            Operation::MemberInvite => self.member_invite(request.request_id, request.payload),
            Operation::MemberJoin | Operation::Join => {
                self.member_join(request.request_id, request.payload)
            }
            Operation::MemberRemove => self.member_remove(request.request_id, request.payload),
            Operation::Close => self.close(request.request_id, request.payload),
            Operation::Shutdown => self.shutdown(request.request_id, request.payload),
            operation => {
                let _operation_name = operation.name();
                let _ = request.payload;
                Response::not_implemented(request.request_id)
            }
        }
    }

    fn create(&mut self, request_id: String, payload: Value) -> Response {
        if let Err(response) = parse_payload::<EmptyPayload>(&request_id, payload) {
            return response;
        }
        if self.space.is_some() {
            return invalid_state(request_id);
        }
        let backend_url = self.process.backend_url.clone();
        let request_timeout = self.process.request_timeout;
        let schema = self.application_schema();
        let space = match self.executor.block_on(async move {
            let transport =
                WebSocketTransport::new_with_request_timeout(&backend_url, request_timeout).await?;
            Space::create(transport, schema).await
        }) {
            Ok(space) => space,
            Err(_) => return sdk_error(request_id),
        };
        let space_id = space.id().to_string();
        self.space = Some(space);
        Response::success(
            request_id,
            CreateResult {
                space_id,
                schema_sha256: &self.process.schema_sha256,
            },
        )
    }

    fn snapshot(&mut self, request_id: String, payload: Value) -> Response {
        let payload = match parse_payload::<SpacePayload>(&request_id, payload) {
            Ok(payload) => payload,
            Err(response) => return response,
        };
        let Some(space) = self.space.as_ref() else {
            return invalid_state(request_id);
        };
        let space_id = space.id().to_string();
        if payload.space_id != space_id {
            return invalid_state(request_id);
        }
        if let Err(response) = self.sync_before_access(&request_id) {
            return response;
        }
        match self.executor.block_on(space.snapshot()) {
            Ok(snapshot) => Response::success(request_id, SnapshotResult { space_id, snapshot }),
            Err(error) => sdk_operation_error(request_id, &error),
        }
    }

    fn restore(&mut self, request_id: String, payload: Value) -> Response {
        let payload = match parse_payload::<RestorePayload>(&request_id, payload) {
            Ok(payload) => payload,
            Err(response) => return response,
        };
        if self.space.is_some() {
            return invalid_state(request_id);
        }
        let backend_url = self.process.backend_url.clone();
        let request_timeout = self.process.request_timeout;
        let schema = self.application_schema();
        let space = match self.executor.block_on(async move {
            let transport =
                WebSocketTransport::new_with_request_timeout(&backend_url, request_timeout).await?;
            Space::restore_trusted(transport, payload.snapshot, schema).await
        }) {
            Ok(space) => space,
            Err(SdkErrorType::ValidationError(message))
                if message == "snapshot trust bundle mismatch" =>
            {
                return trust_mismatch(request_id);
            }
            Err(_) => return sdk_error(request_id),
        };
        let space_id = space.id().to_string();
        self.space = Some(space);
        Response::success(
            request_id,
            RestoreResult {
                space_id,
                restored: true,
            },
        )
    }

    fn sync(&mut self, request_id: String, payload: Value) -> Response {
        let payload = match parse_payload::<SyncPayload>(&request_id, payload) {
            Ok(payload) => payload,
            Err(response) => return response,
        };
        if payload.wait_for_change_ms.is_some() {
            return Response::not_implemented(request_id);
        }
        let Some(space) = self.space.as_ref() else {
            return invalid_state(request_id);
        };
        let space_id = space.id().to_string();
        if payload.space_id != space_id {
            return invalid_state(request_id);
        }
        match self.executor.block_on(space.sync()) {
            Ok(()) => Response::success(
                request_id,
                SyncResult {
                    space_id,
                    synced: true,
                },
            ),
            Err(error) => sdk_operation_error(request_id, &error),
        }
    }

    pub fn subscribe_updates(
        &self,
        space_id: &str,
    ) -> Option<tokio::sync::broadcast::Receiver<encrypted_spaces_sdk::BroadcastEvent>> {
        let space = self.space.as_ref()?;
        (space_id == space.id().to_string()).then(|| space.subscribe_updates())
    }

    pub fn poll_background(&self) {
        self.executor.block_on(tokio::task::yield_now());
    }

    fn sync_before_access(&self, request_id: &str) -> Result<(), Response> {
        let Some(space) = self.space.as_ref() else {
            return Err(invalid_state(request_id.to_owned()));
        };
        self.executor
            .block_on(space.sync())
            .map_err(|error| sdk_operation_error(request_id.to_owned(), &error))
    }

    fn table_insert(&mut self, request_id: String, payload: Value) -> Response {
        let payload = match parse_payload::<TableInsertPayload>(&request_id, payload) {
            Ok(payload) if payload.row.is_object() => payload,
            Ok(_) => return invalid_request(request_id),
            Err(response) => return response,
        };
        let Some(space) = self.space.as_ref() else {
            return invalid_state(request_id);
        };
        if payload.space_id != space.id().to_string() {
            return invalid_state(request_id);
        }
        if let Err(response) = self.sync_before_access(&request_id) {
            return response;
        }
        let table = space.table::<Value>(&payload.table);
        match self.executor.block_on(table.insert(&payload.row).execute()) {
            Ok(row_id) => Response::success(request_id, TableInsertResult { row_id }),
            Err(error) => sdk_operation_error(request_id, &error),
        }
    }

    fn table_select(&mut self, request_id: String, payload: Value) -> Response {
        let payload = match parse_payload::<TableSelectPayload>(&request_id, payload) {
            Ok(payload) => payload,
            Err(response) => return response,
        };
        let Some(space) = self.space.as_ref() else {
            return invalid_state(request_id);
        };
        if payload.space_id != space.id().to_string() {
            return invalid_state(request_id);
        }
        if payload.where_clause.len() != 1 {
            return invalid_request(request_id);
        }
        let (column, value) = payload
            .where_clause
            .into_iter()
            .next()
            .expect("one predicate");
        let Some(schema) = space.get_table_schema(&payload.table) else {
            return invalid_request(request_id);
        };
        if column != "id"
            && !schema
                .indexed_columns()
                .iter()
                .any(|indexed| indexed == &column)
        {
            return invalid_request(request_id);
        }
        if let Err(response) = self.sync_before_access(&request_id) {
            return response;
        }
        let table = space.table::<Value>(&payload.table);
        match self
            .executor
            .block_on(table.select().where_eq(&column, value).all())
        {
            Ok(rows) => Response::success(request_id, TableSelectResult { rows }),
            Err(error) => sdk_operation_error(request_id, &error),
        }
    }

    fn list_create(&mut self, request_id: String, payload: Value) -> Response {
        let payload = match parse_payload::<ListCreatePayload>(&request_id, payload) {
            Ok(payload)
                if !payload.table.is_empty()
                    && payload.row_id > 0
                    && !payload.column.is_empty() =>
            {
                payload
            }
            Ok(_) => return invalid_request(request_id),
            Err(response) => return response,
        };
        let Some(space) = self.space.as_ref() else {
            return invalid_state(request_id);
        };
        let space_id = space.id().to_string();
        if payload.space_id != space_id {
            return invalid_state(request_id);
        }
        if let Err(response) = self.sync_before_access(&request_id) {
            return response;
        }
        let list = space.list::<Value>(&payload.table, payload.row_id, &payload.column);
        if self.executor.block_on(list.get_all()).is_err() {
            return sdk_error(request_id);
        }
        let list_ref = match encode_opaque_ref(&ScopedRef {
            space_id: space_id.clone(),
            kind: RefKind::List,
            table: payload.table.clone(),
            row_id: payload.row_id,
            column: payload.column.clone(),
        }) {
            Ok(list_ref) => list_ref,
            Err(()) => return internal_error(request_id),
        };
        Response::success(
            request_id,
            ListCreateResult {
                space_id,
                table: payload.table,
                row_id: payload.row_id,
                column: payload.column,
                list_ref,
            },
        )
    }

    fn list_append(&mut self, request_id: String, payload: Value) -> Response {
        let payload = match parse_payload::<ListAppendPayload>(&request_id, payload) {
            Ok(payload) => payload,
            Err(response) => return response,
        };
        let list_ref = match decode_opaque_ref::<ScopedRef>(&payload.list_ref) {
            Ok(list_ref) if list_ref.kind == RefKind::List => list_ref,
            Ok(_) | Err(()) => return invalid_request(request_id),
        };
        let Some(space) = self.space.as_ref() else {
            return invalid_state(request_id);
        };
        let space_id = space.id().to_string();
        if payload.space_id != space_id || list_ref.space_id != space_id {
            return invalid_state(request_id);
        }
        if let Err(response) = self.sync_before_access(&request_id) {
            return response;
        }
        let list = space.list::<Value>(&list_ref.table, list_ref.row_id, &list_ref.column);
        match self.executor.block_on(list.append(&payload.value)) {
            Ok(item_ref) => Response::success(
                request_id,
                ListAppendResult {
                    space_id,
                    list_ref: payload.list_ref,
                    item_ref: URL_SAFE_NO_PAD.encode(item_ref),
                },
            ),
            Err(_) => sdk_error(request_id),
        }
    }

    fn list_read(&mut self, request_id: String, payload: Value) -> Response {
        let payload = match parse_payload::<ListReadPayload>(&request_id, payload) {
            Ok(payload) => payload,
            Err(response) => return response,
        };
        let list_ref = match decode_opaque_ref::<ScopedRef>(&payload.list_ref) {
            Ok(list_ref) if list_ref.kind == RefKind::List => list_ref,
            Ok(_) | Err(()) => return invalid_request(request_id),
        };
        let Some(space) = self.space.as_ref() else {
            return invalid_state(request_id);
        };
        let space_id = space.id().to_string();
        if payload.space_id != space_id || list_ref.space_id != space_id {
            return invalid_state(request_id);
        }
        if let Err(response) = self.sync_before_access(&request_id) {
            return response;
        }
        let list = space.list::<Value>(&list_ref.table, list_ref.row_id, &list_ref.column);
        match self.executor.block_on(list.get_all()) {
            Ok(entries) => Response::success(
                request_id,
                ListReadResult {
                    space_id,
                    list_ref: payload.list_ref,
                    items: entries
                        .into_iter()
                        .map(|entry| ListItemResult {
                            item_ref: URL_SAFE_NO_PAD.encode(entry.key),
                            position: entry.position,
                            value: entry.value,
                        })
                        .collect(),
                },
            ),
            Err(_) => sdk_error(request_id),
        }
    }

    fn text_create(&mut self, request_id: String, payload: Value) -> Response {
        let payload = match parse_payload::<TextCreatePayload>(&request_id, payload) {
            Ok(payload)
                if !payload.table.is_empty()
                    && payload.row_id > 0
                    && !payload.column.is_empty() =>
            {
                payload
            }
            Ok(_) => return invalid_request(request_id),
            Err(response) => return response,
        };
        let Some(space) = self.space.as_ref() else {
            return invalid_state(request_id);
        };
        let space_id = space.id().to_string();
        if payload.space_id != space_id {
            return invalid_state(request_id);
        }
        if let Err(response) = self.sync_before_access(&request_id) {
            return response;
        }
        let text = space.textarea(&payload.table, payload.row_id, &payload.column);
        if self.executor.block_on(text.sync()).is_err() {
            return sdk_error(request_id);
        }
        let text_ref = match encode_opaque_ref(&ScopedRef {
            space_id: space_id.clone(),
            kind: RefKind::Text,
            table: payload.table.clone(),
            row_id: payload.row_id,
            column: payload.column.clone(),
        }) {
            Ok(text_ref) => text_ref,
            Err(()) => return internal_error(request_id),
        };
        Response::success(
            request_id,
            TextCreateResult {
                space_id,
                table: payload.table,
                row_id: payload.row_id,
                column: payload.column,
                text_ref,
            },
        )
    }

    fn text_edit(&mut self, request_id: String, payload: Value) -> Response {
        let payload = match parse_payload::<TextEditPayload>(&request_id, payload) {
            Ok(payload) => payload,
            Err(response) => return response,
        };
        let text_ref = match decode_opaque_ref::<ScopedRef>(&payload.text_ref) {
            Ok(text_ref) if text_ref.kind == RefKind::Text => text_ref,
            Ok(_) | Err(()) => return invalid_request(request_id),
        };
        let Some(delete_end) = payload.position.checked_add(payload.delete_count) else {
            return invalid_request(request_id);
        };
        let Some(space) = self.space.as_ref() else {
            return invalid_state(request_id);
        };
        let space_id = space.id().to_string();
        if payload.space_id != space_id || text_ref.space_id != space_id {
            return invalid_state(request_id);
        }
        if let Err(response) = self.sync_before_access(&request_id) {
            return response;
        }
        let text = space.textarea(&text_ref.table, text_ref.row_id, &text_ref.column);
        let text_len = match self.executor.block_on(async {
            text.sync().await?;
            text.len().await
        }) {
            Ok(text_len) => text_len,
            Err(error) => return sdk_operation_error(request_id, &error),
        };
        if payload.position > text_len || delete_end > text_len {
            return invalid_request(request_id);
        }
        let mutation = self.executor.block_on(apply_text_mutation(
            request_id.clone(),
            payload.delete_count > 0,
            text.delete_range(payload.position, delete_end),
            !payload.insert.is_empty(),
            text.insert_string(payload.position, &payload.insert),
        ));
        match mutation {
            Ok(()) => Response::success(
                request_id,
                TextEditResult {
                    space_id,
                    text_ref: payload.text_ref,
                    edited: true,
                },
            ),
            Err(response) => response,
        }
    }

    fn text_read(&mut self, request_id: String, payload: Value) -> Response {
        let payload = match parse_payload::<TextReadPayload>(&request_id, payload) {
            Ok(payload) => payload,
            Err(response) => return response,
        };
        let text_ref = match decode_opaque_ref::<ScopedRef>(&payload.text_ref) {
            Ok(text_ref) if text_ref.kind == RefKind::Text => text_ref,
            Ok(_) | Err(()) => return invalid_request(request_id),
        };
        let Some(space) = self.space.as_ref() else {
            return invalid_state(request_id);
        };
        let space_id = space.id().to_string();
        if payload.space_id != space_id || text_ref.space_id != space_id {
            return invalid_state(request_id);
        }
        if let Err(response) = self.sync_before_access(&request_id) {
            return response;
        }
        let text = space.textarea(&text_ref.table, text_ref.row_id, &text_ref.column);
        match self.executor.block_on(text.snapshot()) {
            Ok(text) => Response::success(
                request_id,
                TextReadResult {
                    space_id,
                    text_ref: payload.text_ref,
                    text,
                },
            ),
            Err(_) => sdk_error(request_id),
        }
    }

    fn file_put(&mut self, request_id: String, payload: Value) -> Response {
        let payload = match parse_payload::<FilePutPayload>(&request_id, payload) {
            Ok(payload) => payload,
            Err(response) => return response,
        };
        let bytes = match STANDARD.decode(&payload.bytes_base64) {
            Ok(bytes) => bytes,
            Err(_) => return invalid_request(request_id),
        };
        let Some(space) = self.space.as_ref() else {
            return invalid_state(request_id);
        };
        let space_id = space.id().to_string();
        if payload.space_id != space_id {
            return invalid_state(request_id);
        }
        if let Err(response) = self.sync_before_access(&request_id) {
            return response;
        }
        match self
            .executor
            .block_on(space.file().upload(File::from_data(bytes)))
        {
            Ok(file) => match file.hash() {
                Ok(digest) => Response::success(
                    request_id,
                    FilePutResult {
                        space_id,
                        digest: digest.to_owned(),
                    },
                ),
                Err(_) => internal_error(request_id),
            },
            Err(_) => sdk_error(request_id),
        }
    }

    fn file_get(&mut self, request_id: String, payload: Value) -> Response {
        let payload = match parse_payload::<FileGetPayload>(&request_id, payload) {
            Ok(payload) if valid_digest(&payload.digest) => payload,
            Ok(_) => return invalid_request(request_id),
            Err(response) => return response,
        };
        let Some(space) = self.space.as_ref() else {
            return invalid_state(request_id);
        };
        let space_id = space.id().to_string();
        if payload.space_id != space_id {
            return invalid_state(request_id);
        }
        if let Err(response) = self.sync_before_access(&request_id) {
            return response;
        }
        match self.executor.block_on(
            space
                .file()
                .download(&File::from_hash(payload.digest.clone())),
        ) {
            Ok(file) => match file.into_data() {
                Ok(bytes) => Response::success(
                    request_id,
                    FileGetResult {
                        space_id,
                        digest: payload.digest,
                        bytes_base64: STANDARD.encode(bytes),
                    },
                ),
                Err(_) => internal_error(request_id),
            },
            Err(_) => sdk_error(request_id),
        }
    }

    fn member_invite(&mut self, request_id: String, payload: Value) -> Response {
        let payload = match parse_payload::<MemberPayload>(&request_id, payload) {
            Ok(payload) => payload,
            Err(response) => return response,
        };
        let Some(space) = self.space.as_ref() else {
            return invalid_state(request_id);
        };
        let space_id = space.id().to_string();
        if payload.space_id != space_id {
            return invalid_state(request_id);
        }
        if let Err(response) = self.sync_before_access(&request_id) {
            return response;
        }
        match self.executor.block_on(space.invite_user()) {
            Ok(invite) => {
                let Some(member_id) = invite.id() else {
                    return internal_error(request_id);
                };
                match encode_opaque_ref(&invite) {
                    Ok(invite) => Response::success(
                        request_id,
                        InviteResult {
                            space_id,
                            member_id,
                            invite,
                        },
                    ),
                    Err(()) => internal_error(request_id),
                }
            }
            Err(error) => sdk_operation_error(request_id, &error),
        }
    }

    fn member_join(&mut self, request_id: String, payload: Value) -> Response {
        let payload = match parse_payload::<JoinPayload>(&request_id, payload) {
            Ok(payload) => payload,
            Err(response) => return response,
        };
        if self.space.is_some() {
            return invalid_state(request_id);
        }
        let invite = match decode_opaque_ref::<SpaceInvite>(&payload.invite) {
            Ok(invite) => invite,
            Err(()) => return invalid_request(request_id),
        };
        let backend_url = self.process.backend_url.clone();
        let request_timeout = self.process.request_timeout;
        let schema = self.application_schema();
        let space = match self.executor.block_on(async move {
            let transport =
                WebSocketTransport::new_with_request_timeout(&backend_url, request_timeout).await?;
            Space::join(transport, invite, schema).await
        }) {
            Ok(space) => space,
            Err(error) => return sdk_operation_error(request_id, &error),
        };
        let space_id = space.id().to_string();
        let Some(member_id) = space.uid().map(i64::from) else {
            return internal_error(request_id);
        };
        self.space = Some(space);
        Response::success(
            request_id,
            JoinResult {
                space_id,
                member_id,
                joined: true,
            },
        )
    }

    fn member_remove(&mut self, request_id: String, payload: Value) -> Response {
        let payload = match parse_payload::<RemovePayload>(&request_id, payload) {
            Ok(payload) if payload.member_id > 0 => payload,
            Ok(_) => return invalid_request(request_id),
            Err(response) => return response,
        };
        let Some(space) = self.space.as_ref() else {
            return invalid_state(request_id);
        };
        let space_id = space.id().to_string();
        if payload.space_id != space_id {
            return invalid_state(request_id);
        }
        if let Err(response) = self.sync_before_access(&request_id) {
            return response;
        }
        match self.executor.block_on(space.remove_user(payload.member_id)) {
            Ok(()) => Response::success(
                request_id,
                RemoveResult {
                    space_id,
                    member_id: payload.member_id,
                    removed: true,
                },
            ),
            Err(error) => sdk_operation_error(request_id, &error),
        }
    }

    fn close(&mut self, request_id: String, payload: Value) -> Response {
        if let Err(response) = parse_payload::<EmptyPayload>(&request_id, payload) {
            return response;
        }
        self.space.take();
        Response::success(request_id, CloseResult { closed: true })
    }

    fn shutdown(&mut self, request_id: String, payload: Value) -> Response {
        if let Err(response) = parse_payload::<EmptyPayload>(&request_id, payload) {
            return response;
        }
        self.space.take();
        self.shutdown_requested = true;
        Response::success(
            request_id,
            ShutdownResult {
                shutting_down: true,
            },
        )
    }

    pub fn should_shutdown(&self) -> bool {
        self.shutdown_requested
    }

    fn application_schema(&self) -> ApplicationSchema {
        ApplicationSchema::FromOwnedBytes(
            self.process.schema.clone(),
            self.process.data_commitment_bytes,
            self.process.ff_guest_image_id,
        )
    }
}

fn parse_payload<T: DeserializeOwned>(request_id: &str, payload: Value) -> Result<T, Response> {
    serde_json::from_value(payload).map_err(|_| {
        Response::error(
            Some(request_id.to_owned()),
            "INVALID_REQUEST",
            "invalid bridge request",
        )
    })
}

fn invalid_state(request_id: String) -> Response {
    Response::error(
        Some(request_id),
        "INVALID_STATE",
        "operation is not valid in the current bridge state",
    )
}

fn invalid_request(request_id: String) -> Response {
    Response::error(
        Some(request_id),
        "INVALID_REQUEST",
        "invalid bridge request",
    )
}

fn sdk_error(request_id: String) -> Response {
    Response::error(
        Some(request_id),
        "SDK_ERROR",
        "encrypted spaces operation failed",
    )
}

fn sdk_operation_error(request_id: String, error: &SdkErrorType) -> Response {
    if matches!(error, SdkErrorType::CommitOutcomeUnknown(_)) {
        return Response::error(
            Some(request_id),
            "COMMIT_UNKNOWN",
            "operation outcome is unknown; synchronize before retrying",
        );
    }
    let removed_member = matches!(
        error,
        SdkErrorType::ValidationError(message)
            if message == "no GK delivery slot available for current user"
    ) || matches!(
        error,
        SdkErrorType::DecryptionError(message) if message == "missing key for current key_id"
    );
    if matches!(error, SdkErrorType::AccessDenied(_))
        || removed_member
        || error
            .to_string()
            .to_ascii_lowercase()
            .contains("access denied")
    {
        return Response::error(
            Some(request_id),
            "ACCESS_DENIED",
            "encrypted spaces access denied",
        );
    }
    sdk_error(request_id)
}

fn internal_error(request_id: String) -> Response {
    Response::error(
        Some(request_id),
        "INTERNAL_ERROR",
        "bridge operation failed",
    )
}

fn text_mutation_error(request_id: String) -> Response {
    Response::error(
        Some(request_id),
        "PARTIAL_COMMIT",
        "text edit may have partially committed; synchronize before continuing",
    )
}

async fn apply_text_mutation<D, I>(
    request_id: String,
    run_delete: bool,
    delete: D,
    run_insert: bool,
    insert: I,
) -> Result<(), Response>
where
    D: Future<Output = Result<(), SdkErrorType>>,
    I: Future<Output = Result<(), SdkErrorType>>,
{
    if run_delete {
        if let Err(error) = delete.await {
            return Err(sdk_operation_error(request_id, &error));
        }
    }
    if run_insert {
        if let Err(error) = insert.await {
            return Err(if run_delete {
                text_mutation_error(request_id)
            } else {
                sdk_operation_error(request_id, &error)
            });
        }
    }
    Ok(())
}

fn trust_mismatch(request_id: String) -> Response {
    Response::error(
        Some(request_id),
        "TRUST_MISMATCH",
        "snapshot trust bundle does not match process configuration",
    )
}

fn encode_opaque_ref(value: &impl Serialize) -> Result<String, ()> {
    serde_json::to_vec(value)
        .map(|bytes| URL_SAFE_NO_PAD.encode(bytes))
        .map_err(|_| ())
}

fn decode_opaque_ref<T: DeserializeOwned>(value: &str) -> Result<T, ()> {
    let bytes = URL_SAFE_NO_PAD.decode(value).map_err(|_| ())?;
    serde_json::from_slice(&bytes).map_err(|_| ())
}

fn valid_digest(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn required_env(name: &str) -> io::Result<String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, format!("{name} is required")))
}

fn request_timeout_from_env() -> io::Result<Duration> {
    match std::env::var(REQUEST_TIMEOUT_MS_ENV) {
        Ok(value) => parse_request_timeout_ms(Some(&value)),
        Err(std::env::VarError::NotPresent) => parse_request_timeout_ms(None),
        Err(error) => Err(io::Error::new(io::ErrorKind::InvalidInput, error)),
    }
}

fn parse_request_timeout_ms(value: Option<&str>) -> io::Result<Duration> {
    let milliseconds = match value {
        Some(value) => value.parse::<u64>().map_err(|_| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("{REQUEST_TIMEOUT_MS_ENV} must be an integer"),
            )
        })?,
        None => DEFAULT_REQUEST_TIMEOUT_MS,
    };
    if !(1..=MAX_REQUEST_TIMEOUT_MS).contains(&milliseconds) {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("{REQUEST_TIMEOUT_MS_ENV} must be between 1 and {MAX_REQUEST_TIMEOUT_MS}"),
        ));
    }
    Ok(Duration::from_millis(milliseconds))
}

#[cfg(test)]
mod tests {
    use super::*;
    use encrypted_spaces_sdk::{ColumnType, SchemaBuilder};
    use serde_json::json;

    #[test]
    fn text_edit_failure_after_mutation_is_reported_as_partial_commit() {
        #[derive(Serialize)]
        struct TextRow {
            id: Option<i64>,
            body: encrypted_spaces_sdk::TextArea,
        }

        let executor = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime");
        executor.block_on(async {
            let schema = SchemaBuilder::new("text_records")
                .column("id", ColumnType::Integer)
                .plaintext_primary_key()
                .column("body", ColumnType::List)
                .expect("body column")
                .build()
                .expect("text schema");
            let transport = LocalTransport::new(std::slice::from_ref(&schema), None, Some(1024))
                .await
                .expect("local transport");
            let data_commitment = transport.get_root_hash().await.expect("initial root");
            let space = Space::create(
                transport,
                ApplicationSchema::WithDataCommitment(
                    vec![schema],
                    data_commitment,
                    encrypted_spaces_ffproof::EXTEND_FF_ID,
                ),
            )
            .await
            .expect("text space");
            let row_id = space
                .table::<TextRow>("text_records")
                .insert(&TextRow {
                    id: None,
                    body: encrypted_spaces_sdk::TextArea::empty(),
                })
                .execute()
                .await
                .expect("text parent row");
            let text = space.textarea("text_records", row_id, "body");
            text.insert_string(0, "abcdef").await.expect("initial text");

            let result = apply_text_mutation(
                "partial-text-edit".to_owned(),
                true,
                text.delete_range(1, 3),
                true,
                std::future::ready(encrypted_spaces_sdk::SdkResult::Err(SdkErrorType::NotFound)),
            )
            .await;
            let response = result.expect_err("insert failure after delete must be partial commit");

            assert!(!response.ok);
            assert_eq!(
                response.error.as_ref().map(|error| error.code),
                Some("PARTIAL_COMMIT")
            );
            assert_eq!(
                text.snapshot().await.expect("partially committed text"),
                "adef"
            );
        });
    }

    #[test]
    fn text_edit_first_stage_access_denial_is_preserved() {
        let executor = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime");
        let result = executor.block_on(apply_text_mutation(
            "denied-text-edit".to_owned(),
            true,
            std::future::ready(encrypted_spaces_sdk::SdkResult::Err(
                SdkErrorType::AccessDenied("revoked".to_owned()),
            )),
            true,
            std::future::ready(encrypted_spaces_sdk::SdkResult::Ok(())),
        ));
        let response = result.expect_err("access denial must fail the edit");

        assert_eq!(
            response.error.as_ref().map(|error| error.code),
            Some("ACCESS_DENIED")
        );
    }

    #[test]
    fn sdk_commit_outcome_unknown_is_preserved() {
        let response = sdk_operation_error(
            "unknown-commit".to_owned(),
            &SdkErrorType::CommitOutcomeUnknown("deadline after send".to_owned()),
        );

        assert_eq!(
            response.error.as_ref().map(|error| error.code),
            Some("COMMIT_UNKNOWN")
        );
        assert!(
            response
                .error
                .as_ref()
                .is_some_and(|error| error.message.contains("synchronize")),
            "unknown commit response omitted reconciliation guidance"
        );
    }

    #[test]
    fn request_timeout_bounds_are_enforced() {
        assert_eq!(
            parse_request_timeout_ms(None).expect("default timeout"),
            Duration::from_millis(DEFAULT_REQUEST_TIMEOUT_MS)
        );
        assert_eq!(
            parse_request_timeout_ms(Some("1")).expect("minimum timeout"),
            Duration::from_millis(1)
        );
        assert_eq!(
            parse_request_timeout_ms(Some("3600000")).expect("maximum timeout"),
            Duration::from_millis(MAX_REQUEST_TIMEOUT_MS)
        );
        for invalid in ["0", "3600001", "not-a-number"] {
            assert!(
                parse_request_timeout_ms(Some(invalid)).is_err(),
                "accepted invalid timeout {invalid}"
            );
        }
    }

    fn revoked_member_runtime() -> (Runtime, String, i64) {
        let executor = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("test runtime");
        let (member, space_id, row_id, data_commitment_bytes) = executor.block_on(async {
            let schema = SchemaBuilder::new("records")
                .column("id", ColumnType::Integer)
                .plaintext_primary_key()
                .column("label", ColumnType::Text)
                .expect("label column")
                .build()
                .expect("records schema");
            let transport = LocalTransport::new(std::slice::from_ref(&schema), None, Some(1024))
                .await
                .expect("local transport");
            let data_commitment = transport.get_root_hash().await.expect("initial root");
            let schema_bundle = || {
                ApplicationSchema::WithDataCommitment(
                    vec![schema.clone()],
                    data_commitment,
                    encrypted_spaces_ffproof::EXTEND_FF_ID,
                )
            };
            let owner = Space::create(transport.clone(), schema_bundle())
                .await
                .expect("owner space");
            let invite = owner.invite_user().await.expect("member invite");
            let member = Space::join(transport.clone(), invite, schema_bundle())
                .await
                .expect("member join");
            let member_id = member.uid().expect("member ID") as i64;

            owner.sync().await.expect("owner join sync");
            let row_id = owner
                .table::<Value>("records")
                .insert(&json!({"label": "cached-before-revocation"}))
                .execute()
                .await
                .expect("owner insert");
            member.sync().await.expect("member row sync");
            let cached = member
                .table::<Value>("records")
                .select()
                .where_eq("id", row_id)
                .all()
                .await
                .expect("warm member cache");
            assert_eq!(cached.len(), 1, "member cache fixture");

            owner.remove_user(member_id).await.expect("remove member");
            (member, owner.id().to_string(), row_id, data_commitment)
        });
        let data_commitment = data_commitment_bytes
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();
        let runtime = Runtime {
            executor,
            process: ProcessConfig {
                client_label: "revoked-member".to_owned(),
                schema_sha256: "test-schema".to_owned(),
                schema: Vec::new(),
                data_commitment_bytes,
                data_commitment,
                ff_guest_image_id: encrypted_spaces_ffproof::EXTEND_FF_ID,
                backend_url: "local-test".to_owned(),
                request_timeout: Duration::from_millis(DEFAULT_REQUEST_TIMEOUT_MS),
            },
            space: Some(member),
            shutdown_requested: false,
        };

        (runtime, space_id, row_id)
    }

    #[test]
    fn table_select_syncs_revocation_before_cache_access() {
        let (mut runtime, space_id, row_id) = revoked_member_runtime();

        let response = runtime.table_select(
            "revoked-select".to_owned(),
            json!({
                "space_id": space_id,
                "table": "records",
                "where": {"id": row_id},
            }),
        );

        assert!(!response.ok, "revoked member read stale cached row");
        assert_eq!(
            response.error.as_ref().map(|error| error.code),
            Some("ACCESS_DENIED")
        );
    }

    #[test]
    fn table_select_rejects_other_active_space_as_invalid_state() {
        let (mut runtime, space_id, row_id) = revoked_member_runtime();

        let response = runtime.table_select(
            "wrong-space-select".to_owned(),
            json!({
                "space_id": format!("different-{space_id}"),
                "table": "records",
                "where": {"id": row_id},
            }),
        );

        assert!(
            !response.ok,
            "table.select accepted a different active Space"
        );
        assert_eq!(
            response.error.as_ref().map(|error| error.code),
            Some("INVALID_STATE")
        );
    }

    #[test]
    fn table_insert_syncs_revocation_before_write() {
        let (mut runtime, space_id, _) = revoked_member_runtime();

        let response = runtime.table_insert(
            "revoked-insert".to_owned(),
            json!({
                "space_id": space_id,
                "table": "records",
                "row": {"label": "write-after-revocation"},
            }),
        );

        assert!(!response.ok, "revoked member wrote after removal");
        assert_eq!(
            response.error.as_ref().map(|error| error.code),
            Some("ACCESS_DENIED")
        );
    }

    #[test]
    fn every_protected_operation_syncs_revocation_before_sdk_access() {
        let operations = [
            "snapshot",
            "list.create",
            "list.append",
            "list.read",
            "text.create",
            "text.edit",
            "text.read",
            "file.put",
            "file.get",
            "member.invite",
            "member.remove",
        ];

        for operation in operations {
            let (mut runtime, space_id, row_id) = revoked_member_runtime();
            let list_ref = encode_opaque_ref(&ScopedRef {
                space_id: space_id.clone(),
                kind: RefKind::List,
                table: "records".to_owned(),
                row_id,
                column: "label".to_owned(),
            })
            .expect("list reference");
            let text_ref = encode_opaque_ref(&ScopedRef {
                space_id: space_id.clone(),
                kind: RefKind::Text,
                table: "records".to_owned(),
                row_id,
                column: "label".to_owned(),
            })
            .expect("text reference");
            let response = match operation {
                "snapshot" => runtime.snapshot(
                    "revoked-snapshot".to_owned(),
                    json!({"space_id": space_id}),
                ),
                "list.create" => runtime.list_create(
                    "revoked-list-create".to_owned(),
                    json!({"space_id": space_id, "table": "records", "row_id": row_id, "column": "label"}),
                ),
                "list.append" => runtime.list_append(
                    "revoked-list-append".to_owned(),
                    json!({"space_id": space_id, "list_ref": list_ref, "value": "denied"}),
                ),
                "list.read" => runtime.list_read(
                    "revoked-list-read".to_owned(),
                    json!({"space_id": space_id, "list_ref": list_ref}),
                ),
                "text.create" => runtime.text_create(
                    "revoked-text-create".to_owned(),
                    json!({"space_id": space_id, "table": "records", "row_id": row_id, "column": "label"}),
                ),
                "text.edit" => runtime.text_edit(
                    "revoked-text-edit".to_owned(),
                    json!({"space_id": space_id, "text_ref": text_ref, "position": 0, "delete_count": 0, "insert": "denied"}),
                ),
                "text.read" => runtime.text_read(
                    "revoked-text-read".to_owned(),
                    json!({"space_id": space_id, "text_ref": text_ref}),
                ),
                "file.put" => runtime.file_put(
                    "revoked-file-put".to_owned(),
                    json!({"space_id": space_id, "bytes_base64": "ZGVuaWVk"}),
                ),
                "file.get" => runtime.file_get(
                    "revoked-file-get".to_owned(),
                    json!({"space_id": space_id, "digest": "0".repeat(64)}),
                ),
                "member.invite" => runtime.member_invite(
                    "revoked-member-invite".to_owned(),
                    json!({"space_id": space_id}),
                ),
                "member.remove" => runtime.member_remove(
                    "revoked-member-remove".to_owned(),
                    json!({"space_id": space_id, "member_id": 1}),
                ),
                _ => unreachable!(),
            };

            assert_eq!(
                response.error.as_ref().map(|error| error.code),
                Some("ACCESS_DENIED"),
                "{operation} did not observe revocation before SDK access"
            );
        }
    }
}
