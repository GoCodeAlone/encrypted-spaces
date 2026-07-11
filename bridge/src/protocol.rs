use crate::runtime;
use crate::schema::{Operation, Request, MAX_REQUEST_ID_BYTES, PROTOCOL_VERSION};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashMap;
use std::io::{self, BufRead, Read, Write};
use std::sync::{
    atomic::{AtomicU8, Ordering},
    Arc, Mutex,
};
use std::thread;
use std::time::Duration;

pub const MAX_FRAME_BYTES: usize = 64 * 1024;
const MAX_PENDING_REQUESTS: usize = 64;
const MAX_WAIT_FOR_CHANGE_MS: u64 = 60_000;
const PENDING: u8 = 0;
const CANCELED: u8 = 1;
const COMPLETING: u8 = 2;

#[derive(Debug, Serialize)]
pub struct Response {
    pub version: u16,
    pub request_id: Option<String>,
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
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
            result: None,
            error: Some(ErrorBody { code, message }),
        }
    }

    pub fn success(request_id: String, result: impl Serialize) -> Self {
        match serde_json::to_value(result) {
            Ok(result) => Self {
                version: PROTOCOL_VERSION,
                request_id: Some(request_id),
                ok: true,
                result: Some(result),
                error: None,
            },
            Err(_) => Self::error(
                Some(request_id),
                "INTERNAL_ERROR",
                "bridge response serialization failed",
            ),
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

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WaitSyncPayload {
    space_id: String,
    wait_for_change_ms: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct CancelPayload {
    request_id: String,
}

#[derive(Serialize)]
struct CancelResult {
    canceled: bool,
}

pub fn run<R: Read, W: Write + Send + 'static>(reader: R, writer: W) -> io::Result<()> {
    let runtime = Arc::new(Mutex::new(runtime::Runtime::from_env()?));
    let writer = Arc::new(Mutex::new(writer));
    let mut pending = HashMap::<String, Arc<AtomicU8>>::new();
    let mut reader = io::BufReader::new(reader);
    loop {
        let frame = match read_frame(&mut reader) {
            Ok(Some(frame)) => frame,
            Ok(None) => return Ok(()),
            Err(FrameError::TooLarge) => {
                write_shared_response(
                    &writer,
                    Response::error(None, "FRAME_TOO_LARGE", "JSONL frame exceeds maximum size"),
                )?;
                return Ok(());
            }
            Err(FrameError::Io(error)) => return Err(error),
        };
        pending.retain(|_, state| state.load(Ordering::Acquire) == PENDING);

        let request = match decode_request(&frame) {
            Ok(request) => request,
            Err(response) => {
                write_shared_response(&writer, response)?;
                continue;
            }
        };

        if matches!(request.operation, Operation::Cancel) {
            cancel_pending(request, &mut pending, &writer)?;
            continue;
        }
        if matches!(request.operation, Operation::Sync)
            && request.payload.get("wait_for_change_ms").is_some()
        {
            schedule_wait_sync(
                request,
                &mut pending,
                Arc::clone(&runtime),
                Arc::clone(&writer),
            )?;
            continue;
        }

        let (response, shutdown) = {
            let mut runtime = runtime
                .lock()
                .map_err(|_| io::Error::other("runtime lock poisoned"))?;
            let response = runtime.dispatch(request);
            (response, runtime.should_shutdown())
        };
        write_shared_response(&writer, response)?;
        if shutdown {
            return Ok(());
        }
    }
}

fn schedule_wait_sync<W: Write + Send + 'static>(
    request: Request,
    pending: &mut HashMap<String, Arc<AtomicU8>>,
    runtime: Arc<Mutex<runtime::Runtime>>,
    writer: Arc<Mutex<W>>,
) -> io::Result<()> {
    let request_id = request.request_id;
    let payload = match serde_json::from_value::<WaitSyncPayload>(request.payload) {
        Ok(payload)
            if payload.wait_for_change_ms > 0
                && payload.wait_for_change_ms <= MAX_WAIT_FOR_CHANGE_MS =>
        {
            payload
        }
        _ => {
            return write_shared_response(
                &writer,
                Response::error(
                    Some(request_id),
                    "INVALID_REQUEST",
                    "invalid bridge request",
                ),
            );
        }
    };
    if pending.len() >= MAX_PENDING_REQUESTS || pending.contains_key(&request_id) {
        return write_shared_response(
            &writer,
            Response::error(
                Some(request_id),
                "TOO_MANY_PENDING",
                "pending bridge request limit reached",
            ),
        );
    }

    let state = Arc::new(AtomicU8::new(PENDING));
    pending.insert(request_id.clone(), Arc::clone(&state));
    thread::spawn(move || {
        thread::sleep(Duration::from_millis(payload.wait_for_change_ms));
        if state
            .compare_exchange(PENDING, COMPLETING, Ordering::AcqRel, Ordering::Acquire)
            .is_err()
        {
            return;
        }
        let response = match runtime.lock() {
            Ok(mut runtime) => runtime.dispatch(Request {
                version: PROTOCOL_VERSION,
                request_id,
                operation: Operation::Sync,
                payload: json!({"space_id": payload.space_id}),
            }),
            Err(_) => return,
        };
        let _ = write_shared_response(&writer, response);
    });
    Ok(())
}

fn cancel_pending<W: Write>(
    request: Request,
    pending: &mut HashMap<String, Arc<AtomicU8>>,
    writer: &Arc<Mutex<W>>,
) -> io::Result<()> {
    let payload = match serde_json::from_value::<CancelPayload>(request.payload) {
        Ok(payload) if !payload.request_id.is_empty() => payload,
        _ => {
            return write_shared_response(
                writer,
                Response::error(
                    Some(request.request_id),
                    "INVALID_REQUEST",
                    "invalid bridge request",
                ),
            );
        }
    };
    let canceled = pending.remove(&payload.request_id).is_some_and(|state| {
        state
            .compare_exchange(PENDING, CANCELED, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    });
    let mut writer = writer
        .lock()
        .map_err(|_| io::Error::other("writer lock poisoned"))?;
    if canceled {
        write_response(
            &mut *writer,
            Response::error(
                Some(payload.request_id),
                "CANCELED",
                "bridge request canceled",
            ),
        )?;
    }
    write_response(
        &mut *writer,
        Response::success(request.request_id, CancelResult { canceled }),
    )
}

fn write_shared_response<W: Write>(writer: &Arc<Mutex<W>>, response: Response) -> io::Result<()> {
    let mut writer = writer
        .lock()
        .map_err(|_| io::Error::other("writer lock poisoned"))?;
    write_response(&mut *writer, response)
}

fn decode_request(frame: &[u8]) -> Result<Request, Response> {
    let value = match serde_json::from_slice::<Value>(frame) {
        Ok(value) => value,
        Err(_) => {
            return Err(Response::error(
                None,
                "INVALID_JSON",
                "malformed JSONL frame",
            ));
        }
    };
    let request_id = parsed_request_id(&value);
    if has_unknown_operation(&value) {
        return Err(Response::error(
            request_id,
            "UNKNOWN_OPERATION",
            "unknown bridge operation",
        ));
    }

    let request = match serde_json::from_value::<Request>(value) {
        Ok(request) => request,
        Err(_) => {
            return Err(Response::error(
                request_id,
                "INVALID_REQUEST",
                "invalid bridge request",
            ));
        }
    };
    match request.validate() {
        Ok(()) => Ok(request),
        Err(_) => Err(Response::error(
            request_id,
            "INVALID_REQUEST",
            "invalid bridge request",
        )),
    }
}

fn has_unknown_operation(value: &Value) -> bool {
    value
        .get("operation")
        .and_then(Value::as_str)
        .is_some_and(|operation| {
            serde_json::from_value::<Operation>(Value::String(operation.to_owned())).is_err()
        })
}

fn parsed_request_id(value: &Value) -> Option<String> {
    value
        .get("request_id")
        .and_then(Value::as_str)
        .filter(|request_id| !request_id.is_empty() && request_id.len() <= MAX_REQUEST_ID_BYTES)
        .map(str::to_owned)
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
            return Err(FrameError::TooLarge);
        }
        frame.push(byte[0]);
    }
}

fn write_response<W: Write>(writer: &mut W, response: Response) -> io::Result<()> {
    writer.write_all(&encode_response(&response)?)?;
    writer.flush()
}

fn encode_response(response: &Response) -> io::Result<Vec<u8>> {
    let mut frame = serde_json::to_vec(response).map_err(io::Error::other)?;
    frame.push(b'\n');
    Ok(frame)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocol_decode_accepts_valid_and_correlates_invalid_requests() {
        let valid = br#"{"version":1,"request_id":"decode-ok","operation":"hello","payload":{}}"#;
        let request = decode_request(valid).expect("valid request");
        assert_eq!(request.request_id, "decode-ok");
        assert!(matches!(request.operation, Operation::Hello));

        let invalid =
            br#"{"version":2,"request_id":"decode-invalid","operation":"hello","payload":{}}"#;
        let response = decode_request(invalid).expect_err("invalid version");
        assert_eq!(response.request_id.as_deref(), Some("decode-invalid"));
        assert_eq!(response.error.expect("error body").code, "INVALID_REQUEST");
    }

    #[test]
    fn protocol_encode_emits_one_redacted_jsonl_frame() {
        let response = Response::error(
            Some("encode-request".to_owned()),
            "INVALID_REQUEST",
            "invalid bridge request",
        );
        let encoded = encode_response(&response).expect("encode response");
        assert_eq!(
            encoded,
            b"{\"version\":1,\"request_id\":\"encode-request\",\"ok\":false,\"error\":{\"code\":\"INVALID_REQUEST\",\"message\":\"invalid bridge request\"}}\n"
        );
    }

    #[test]
    fn protocol_frame_limit_accepts_exact_and_rejects_oversize() {
        let mut exact = io::Cursor::new(vec![b'x'; MAX_FRAME_BYTES]);
        let frame = match read_frame(&mut exact) {
            Ok(Some(frame)) => frame,
            _ => panic!("exact-size frame must be accepted"),
        };
        assert_eq!(frame.len(), MAX_FRAME_BYTES);

        let mut oversize = io::Cursor::new(vec![b'x'; MAX_FRAME_BYTES + 1]);
        assert!(matches!(
            read_frame(&mut oversize),
            Err(FrameError::TooLarge)
        ));
    }
}
