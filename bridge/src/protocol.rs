use crate::runtime;
use crate::schema::{Request, PROTOCOL_VERSION};
use serde::Serialize;
use std::io::{self, BufRead, Read, Write};

pub const MAX_FRAME_BYTES: usize = 64 * 1024;

#[derive(Debug, Serialize)]
pub struct Response {
    pub version: u16,
    pub request_id: Option<String>,
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ErrorBody>,
}

#[derive(Debug, Serialize)]
pub struct ErrorBody {
    pub code: &'static str,
    pub message: &'static str,
}

impl Response {
    pub fn error(request_id: Option<String>, code: &'static str, message: &'static str) -> Self {
        Self {
            version: PROTOCOL_VERSION,
            request_id,
            ok: false,
            error: Some(ErrorBody { code, message }),
        }
    }

    pub fn not_implemented(request_id: String) -> Self {
        Self::error(
            Some(request_id),
            "NOT_IMPLEMENTED",
            "runtime operation is not implemented",
        )
    }
}

enum FrameError {
    TooLarge,
    Io(io::Error),
}

pub fn run<R: Read, W: Write>(reader: R, mut writer: W) -> io::Result<()> {
    let mut reader = io::BufReader::new(reader);
    loop {
        let frame = match read_frame(&mut reader) {
            Ok(Some(frame)) => frame,
            Ok(None) => return Ok(()),
            Err(FrameError::TooLarge) => {
                write_response(
                    &mut writer,
                    Response::error(None, "FRAME_TOO_LARGE", "JSONL frame exceeds maximum size"),
                )?;
                continue;
            }
            Err(FrameError::Io(error)) => return Err(error),
        };

        let response = match serde_json::from_slice::<Request>(&frame) {
            Ok(request) => match request.validate() {
                Ok(()) => runtime::dispatch(request),
                Err(_) => Response::error(None, "INVALID_REQUEST", "invalid bridge request"),
            },
            Err(_) => Response::error(None, "INVALID_JSON", "malformed JSONL frame"),
        };
        write_response(&mut writer, response)?;
    }
}

fn read_frame<R: BufRead>(reader: &mut R) -> Result<Option<Vec<u8>>, FrameError> {
    let mut frame = Vec::with_capacity(1024);
    let mut byte = [0u8; 1];
    loop {
        let read = reader.read(&mut byte).map_err(FrameError::Io)?;
        if read == 0 {
            return if frame.is_empty() {
                Ok(None)
            } else {
                Ok(Some(frame))
            };
        }
        if byte[0] == b'\n' {
            return Ok(Some(frame));
        }
        if frame.len() == MAX_FRAME_BYTES {
            loop {
                let read = reader.read(&mut byte).map_err(FrameError::Io)?;
                if read == 0 || byte[0] == b'\n' {
                    break;
                }
            }
            return Err(FrameError::TooLarge);
        }
        frame.push(byte[0]);
    }
}

fn write_response<W: Write>(writer: &mut W, response: Response) -> io::Result<()> {
    serde_json::to_writer(&mut *writer, &response).map_err(io::Error::other)?;
    writer.write_all(b"\n")?;
    writer.flush()
}
