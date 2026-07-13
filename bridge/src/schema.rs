use serde::{Deserialize, Serialize};
use serde_json::Value;

pub const PROTOCOL_VERSION: u16 = 1;
pub const MAX_REQUEST_ID_BYTES: usize = 256;

#[derive(Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Request {
    pub version: u16,
    pub request_id: String,
    pub operation: Operation,
    #[serde(default)]
    pub payload: Value,
}

impl Request {
    pub fn validate(&self) -> Result<(), &'static str> {
        if self.version != PROTOCOL_VERSION {
            return Err("unsupported protocol version");
        }
        if self.request_id.is_empty() || self.request_id.len() > MAX_REQUEST_ID_BYTES {
            return Err("request id exceeds maximum size");
        }
        Ok(())
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Operation {
    Hello,
    Version,
    Cancel,
    #[serde(rename = "space.create")]
    Create,
    #[serde(rename = "space.join")]
    Join,
    #[serde(rename = "space.snapshot")]
    Snapshot,
    #[serde(rename = "space.restore")]
    Restore,
    #[serde(rename = "space.sync")]
    Sync,
    #[serde(rename = "table.insert")]
    TableInsert,
    #[serde(rename = "table.select")]
    TableSelect,
    #[serde(rename = "list.create")]
    ListCreate,
    #[serde(rename = "list.append")]
    ListAppend,
    #[serde(rename = "list.read")]
    ListRead,
    #[serde(rename = "text.create")]
    TextCreate,
    #[serde(rename = "text.edit")]
    TextEdit,
    #[serde(rename = "text.read")]
    TextRead,
    #[serde(rename = "file.put")]
    FilePut,
    #[serde(rename = "file.get")]
    FileGet,
    #[serde(rename = "member.invite")]
    MemberInvite,
    #[serde(rename = "member.join")]
    MemberJoin,
    #[serde(rename = "member.remove")]
    MemberRemove,
    Close,
    Shutdown,
}

impl Operation {
    pub const fn name(&self) -> &'static str {
        match self {
            Self::Hello => "hello",
            Self::Version => "version",
            Self::Cancel => "cancel",
            Self::Create => "space.create",
            Self::Join => "space.join",
            Self::Snapshot => "space.snapshot",
            Self::Restore => "space.restore",
            Self::Sync => "space.sync",
            Self::TableInsert => "table.insert",
            Self::TableSelect => "table.select",
            Self::ListCreate => "list.create",
            Self::ListAppend => "list.append",
            Self::ListRead => "list.read",
            Self::TextCreate => "text.create",
            Self::TextEdit => "text.edit",
            Self::TextRead => "text.read",
            Self::FilePut => "file.put",
            Self::FileGet => "file.get",
            Self::MemberInvite => "member.invite",
            Self::MemberJoin => "member.join",
            Self::MemberRemove => "member.remove",
            Self::Close => "close",
            Self::Shutdown => "shutdown",
        }
    }
}
