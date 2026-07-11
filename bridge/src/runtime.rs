use crate::protocol::Response;
use crate::schema::{Operation, Request, PROTOCOL_VERSION};
use encrypted_spaces_sdk::{ApplicationSchema, LocalTransport, Space, WebSocketTransport};
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
            Err(_) => sdk_error(request_id),
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

fn sdk_error(request_id: String) -> Response {
    Response::error(
        Some(request_id),
        "SDK_ERROR",
        "encrypted spaces operation failed",
    )
}

fn required_env(name: &str) -> io::Result<String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, format!("{name} is required")))
}
