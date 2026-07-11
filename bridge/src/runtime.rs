use crate::protocol::Response;
use crate::schema::{Operation, Request, PROTOCOL_VERSION};
use encrypted_spaces_sdk::LocalTransport;
use serde::Serialize;
use sha2::{Digest, Sha256};
use std::fs;
use std::io;

const ACTOR_ID_ENV: &str = "ENCRYPTED_SPACES_ACTOR_ID";
const SCHEMA_PATH_ENV: &str = "ENCRYPTED_SPACES_SCHEMA_PATH";

pub struct Runtime {
    executor: tokio::runtime::Runtime,
    process: ProcessConfig,
}

struct ProcessConfig {
    actor_id: String,
    schema_sha256: String,
    data_commitment: String,
    ff_guest_image_id: [u32; 8],
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
        let data_commitment = executor
            .block_on(async {
                let transport = LocalTransport::from_schema_file(&schema_path).await?;
                transport.get_root_hash().await
            })
            .map_err(io::Error::other)?
            .iter()
            .map(|byte| format!("{byte:02x}"))
            .collect();

        Ok(Self {
            executor,
            process: ProcessConfig {
                actor_id,
                schema_sha256,
                data_commitment,
                ff_guest_image_id: encrypted_spaces_ffproof::EXTEND_FF_ID,
            },
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
            operation => {
                let _operation_name = operation.name();
                let _ = request.payload;
                Response::not_implemented(request.request_id)
            }
        }
    }
}

fn required_env(name: &str) -> io::Result<String> {
    std::env::var(name)
        .ok()
        .filter(|value| !value.is_empty())
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, format!("{name} is required")))
}
