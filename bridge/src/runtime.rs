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
use std::io;

const ACTOR_ID_ENV: &str = "ENCRYPTED_SPACES_ACTOR_ID";
const SCHEMA_PATH_ENV: &str = "ENCRYPTED_SPACES_SCHEMA_PATH";
const BACKEND_URL_ENV: &str = "ENCRYPTED_SPACES_BACKEND_URL";
const DEFAULT_BACKEND_URL: &str = "ws://127.0.0.1:8080/ws";

pub struct Runtime {
    executor: tokio::runtime::Runtime,
    process: ProcessConfig,
    space: Option<Space>,
}

struct ProcessConfig {
    actor_id: String,
    schema_sha256: String,
    schema: Vec<u8>,
    data_commitment_bytes: [u8; 32],
    data_commitment: String,
    ff_guest_image_id: [u32; 8],
    backend_url: String,
}

#[derive(Serialize)]
struct HelloResult<'a> {
    protocol_version: u16,
    actor_id: &'a str,
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

impl Runtime {
    pub fn from_env() -> io::Result<Self> {
        let actor_id = required_env(ACTOR_ID_ENV)?;
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

        Ok(Self {
            executor,
            process: ProcessConfig {
                actor_id,
                schema_sha256,
                schema,
                data_commitment_bytes,
                data_commitment,
                ff_guest_image_id: encrypted_spaces_ffproof::EXTEND_FF_ID,
                backend_url,
            },
            space: None,
        })
    }

    pub fn dispatch(&mut self, request: Request) -> Response {
        let _ = &self.executor;
        match request.operation {
            Operation::Hello => Response::success(
                request.request_id,
                HelloResult {
                    protocol_version: PROTOCOL_VERSION,
                    actor_id: &self.process.actor_id,
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
        let schema = self.application_schema();
        let space = match self.executor.block_on(async move {
            let transport = WebSocketTransport::new(&backend_url).await?;
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
        let space = match self.executor.block_on(async move {
            let transport = WebSocketTransport::new(&backend_url).await?;
            Space::restore(transport, payload.snapshot).await
        }) {
            Ok(space) => space,
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
        if payload.space_id != space.id().to_string() || payload.where_clause.len() != 1 {
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
        let list = space.list::<Value>(&payload.table, payload.row_id, &payload.column);
        if self.executor.block_on(list.get_all()).is_err() {
            return sdk_error(request_id);
        }
        let list_ref = match encode_opaque_ref(&ScopedRef {
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
        if payload.space_id != space_id {
            return invalid_state(request_id);
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
        if payload.space_id != space_id {
            return invalid_state(request_id);
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
        let text = space.textarea(&payload.table, payload.row_id, &payload.column);
        if self.executor.block_on(text.sync()).is_err() {
            return sdk_error(request_id);
        }
        let text_ref = match encode_opaque_ref(&ScopedRef {
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
        if payload.space_id != space_id {
            return invalid_state(request_id);
        }
        let text = space.textarea(&text_ref.table, text_ref.row_id, &text_ref.column);
        let result = self.executor.block_on(async {
            text.sync().await?;
            if payload.delete_count > 0 {
                text.delete_range(payload.position, delete_end).await?;
            }
            if !payload.insert.is_empty() {
                text.insert_string(payload.position, &payload.insert)
                    .await?;
            }
            encrypted_spaces_sdk::SdkResult::Ok(())
        });
        match result {
            Ok(()) => Response::success(
                request_id,
                TextEditResult {
                    space_id,
                    text_ref: payload.text_ref,
                    edited: true,
                },
            ),
            Err(_) => sdk_error(request_id),
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
        if payload.space_id != space_id {
            return invalid_state(request_id);
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
        let schema = self.application_schema();
        let space = match self.executor.block_on(async move {
            let transport = WebSocketTransport::new(&backend_url).await?;
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
