use crate::runtime;
use crate::schema::{Operation, Request, MAX_REQUEST_ID_BYTES, PROTOCOL_VERSION};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::HashMap;
use std::io::{self, BufRead, Read, Write};
use std::sync::{
    atomic::{AtomicU8, Ordering},
    mpsc, Arc, Mutex,
};
use std::thread;
use std::time::Duration;

pub const MAX_FRAME_BYTES: usize = 64 * 1024;
const MAX_PENDING_REQUESTS: usize = 64;
const MAX_WAIT_FOR_CHANGE_MS: u64 = 60_000;
const PENDING: u8 = 0;
const CANCELED: u8 = 1;
const COMPLETING: u8 = 2;
const DONE: u8 = 3;
const WAIT_POLL_INTERVAL: Duration = Duration::from_millis(10);

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

struct PendingRequest {
    state: Arc<AtomicU8>,
    cancelable: bool,
}

enum RuntimeJob {
    Dispatch {
        request: Request,
        state: Arc<AtomicU8>,
        wait_trigger: Option<&'static str>,
    },
    RegisterWait {
        request_id: String,
        payload: WaitSyncPayload,
        state: Arc<AtomicU8>,
    },
}

enum CoordinatorEvent {
    Frame(Vec<u8>),
    FrameTooLarge,
    InputClosed,
    ReadError(io::Error),
    Completed {
        request_id: String,
        state: Arc<AtomicU8>,
        response: Response,
        shutdown: bool,
    },
}

pub fn run<R: Read + Send + 'static, W: Write>(reader: R, mut writer: W) -> io::Result<()> {
    let runtime = Arc::new(Mutex::new(runtime::Runtime::from_env()?));
    let (event_tx, event_rx) = mpsc::sync_channel(MAX_PENDING_REQUESTS * 2);
    let (job_tx, job_rx) = mpsc::sync_channel(MAX_PENDING_REQUESTS);
    spawn_reader(reader, event_tx.clone());
    spawn_runtime_worker(Arc::clone(&runtime), job_rx, job_tx.clone(), event_tx);

    let mut pending = HashMap::<String, PendingRequest>::new();
    let mut input_closed = false;
    let mut shutdown_queued = false;
    loop {
        match event_rx
            .recv()
            .map_err(|_| io::Error::other("bridge coordinator stopped"))?
        {
            CoordinatorEvent::Frame(frame) => {
                if shutdown_queued {
                    let request_id = serde_json::from_slice::<Value>(&frame)
                        .ok()
                        .as_ref()
                        .and_then(parsed_request_id);
                    let response = if request_id
                        .as_ref()
                        .is_some_and(|request_id| pending.contains_key(request_id))
                    {
                        Response::error(
                            None,
                            "DUPLICATE_REQUEST_ID",
                            "request ID is already pending",
                        )
                    } else {
                        invalid_state_response_optional(request_id)
                    };
                    write_response(&mut writer, response)?;
                    continue;
                }
                let request = match decode_request(&frame) {
                    Ok(request) => request,
                    Err(mut response) => {
                        if response
                            .request_id
                            .as_ref()
                            .is_some_and(|request_id| pending.contains_key(request_id))
                        {
                            response.request_id = None;
                        }
                        write_response(&mut writer, response)?;
                        continue;
                    }
                };

                if matches!(request.operation, Operation::Cancel) {
                    cancel_pending(request, &mut pending, &mut writer)?;
                    continue;
                }
                if pending.contains_key(&request.request_id) {
                    write_response(
                        &mut writer,
                        Response::error(
                            None,
                            "DUPLICATE_REQUEST_ID",
                            "request ID is already pending",
                        ),
                    )?;
                    continue;
                }
                if pending.len() >= MAX_PENDING_REQUESTS {
                    write_response(
                        &mut writer,
                        Response::error(
                            Some(request.request_id),
                            "TOO_MANY_PENDING",
                            "pending bridge request limit reached",
                        ),
                    )?;
                    continue;
                }

                if matches!(request.operation, Operation::Shutdown) {
                    cancel_all_waits(&mut pending, &mut writer)?;
                    shutdown_queued = true;
                }

                let request_id = request.request_id.clone();
                if matches!(request.operation, Operation::Sync)
                    && request.payload.get("wait_for_change_ms").is_some()
                {
                    let payload = match parse_wait_sync(&request_id, request.payload) {
                        Ok(payload) => payload,
                        Err(response) => {
                            write_response(&mut writer, response)?;
                            continue;
                        }
                    };
                    let state = Arc::new(AtomicU8::new(PENDING));
                    pending.insert(
                        request_id.clone(),
                        PendingRequest {
                            state: Arc::clone(&state),
                            cancelable: true,
                        },
                    );
                    if job_tx
                        .send(RuntimeJob::RegisterWait {
                            request_id: request_id.clone(),
                            payload,
                            state,
                        })
                        .is_err()
                    {
                        pending.remove(&request_id);
                        return Err(io::Error::other("runtime worker stopped"));
                    }
                    continue;
                }

                let state = Arc::new(AtomicU8::new(COMPLETING));
                pending.insert(
                    request_id.clone(),
                    PendingRequest {
                        state: Arc::clone(&state),
                        cancelable: false,
                    },
                );
                if job_tx
                    .send(RuntimeJob::Dispatch {
                        request,
                        state,
                        wait_trigger: None,
                    })
                    .is_err()
                {
                    pending.remove(&request_id);
                    return Err(io::Error::other("runtime worker stopped"));
                }
            }
            CoordinatorEvent::Completed {
                request_id,
                state,
                response,
                shutdown,
            } => {
                if pending
                    .get(&request_id)
                    .is_some_and(|pending| Arc::ptr_eq(&pending.state, &state))
                {
                    pending.remove(&request_id);
                }
                write_response(&mut writer, response)?;
                if shutdown {
                    cancel_all_requests(&mut pending, &mut writer)?;
                    return Ok(());
                }
                if input_closed && pending.is_empty() {
                    return Ok(());
                }
            }
            CoordinatorEvent::FrameTooLarge => {
                write_response(
                    &mut writer,
                    Response::error(None, "FRAME_TOO_LARGE", "JSONL frame exceeds maximum size"),
                )?;
                return Ok(());
            }
            CoordinatorEvent::InputClosed => {
                input_closed = true;
                cancel_all_waits(&mut pending, &mut writer)?;
                if pending.is_empty() {
                    return Ok(());
                }
            }
            CoordinatorEvent::ReadError(error) => return Err(error),
        }
    }
}

fn spawn_reader<R: Read + Send + 'static>(reader: R, events: mpsc::SyncSender<CoordinatorEvent>) {
    thread::spawn(move || {
        let mut reader = io::BufReader::new(reader);
        loop {
            let event = match read_frame(&mut reader) {
                Ok(Some(frame)) => CoordinatorEvent::Frame(frame),
                Ok(None) => CoordinatorEvent::InputClosed,
                Err(FrameError::TooLarge) => CoordinatorEvent::FrameTooLarge,
                Err(FrameError::Io(error)) => CoordinatorEvent::ReadError(error),
            };
            let terminal = !matches!(event, CoordinatorEvent::Frame(_));
            if events.send(event).is_err() || terminal {
                return;
            }
        }
    });
}

fn spawn_runtime_worker(
    runtime: Arc<Mutex<runtime::Runtime>>,
    jobs: mpsc::Receiver<RuntimeJob>,
    job_sender: mpsc::SyncSender<RuntimeJob>,
    events: mpsc::SyncSender<CoordinatorEvent>,
) {
    thread::spawn(move || {
        while let Ok(job) = jobs.recv() {
            match job {
                RuntimeJob::Dispatch {
                    request,
                    state,
                    wait_trigger,
                } => {
                    if wait_trigger.is_some()
                        && state
                            .compare_exchange(
                                PENDING,
                                COMPLETING,
                                Ordering::AcqRel,
                                Ordering::Acquire,
                            )
                            .is_err()
                    {
                        continue;
                    }
                    let request_id = request.request_id.clone();
                    let (mut response, shutdown) = match runtime.lock() {
                        Ok(mut runtime) => {
                            let response = runtime.dispatch(request);
                            (response, runtime.should_shutdown())
                        }
                        Err(_) => (
                            Response::error(
                                Some(request_id.clone()),
                                "INTERNAL_ERROR",
                                "bridge runtime unavailable",
                            ),
                            false,
                        ),
                    };
                    if response.ok {
                        if let (Some(trigger), Some(Value::Object(result))) =
                            (wait_trigger, response.result.as_mut())
                        {
                            result.insert("wait_trigger".to_owned(), serde_json::json!(trigger));
                        }
                    }
                    state.store(DONE, Ordering::Release);
                    if events
                        .send(CoordinatorEvent::Completed {
                            request_id,
                            state,
                            response,
                            shutdown,
                        })
                        .is_err()
                        || shutdown
                    {
                        return;
                    }
                }
                RuntimeJob::RegisterWait {
                    request_id,
                    payload,
                    state,
                } => {
                    if state.load(Ordering::Acquire) != PENDING {
                        continue;
                    }
                    let updates = match runtime.lock() {
                        Ok(runtime) => runtime.subscribe_updates(&payload.space_id),
                        Err(_) => None,
                    };
                    let Some(updates) = updates else {
                        if state
                            .compare_exchange(PENDING, DONE, Ordering::AcqRel, Ordering::Acquire)
                            .is_ok()
                        {
                            let _ = events.send(CoordinatorEvent::Completed {
                                request_id: request_id.clone(),
                                state,
                                response: invalid_state_response(request_id),
                                shutdown: false,
                            });
                        }
                        continue;
                    };
                    spawn_waiter(
                        request_id,
                        payload,
                        updates,
                        Arc::clone(&runtime),
                        job_sender.clone(),
                        Arc::clone(&state),
                    );
                }
            }
        }
    });
}

fn parse_wait_sync(request_id: &str, payload: Value) -> Result<WaitSyncPayload, Response> {
    match serde_json::from_value::<WaitSyncPayload>(payload) {
        Ok(payload)
            if payload.wait_for_change_ms > 0
                && payload.wait_for_change_ms <= MAX_WAIT_FOR_CHANGE_MS =>
        {
            Ok(payload)
        }
        _ => Err(Response::error(
            Some(request_id.to_owned()),
            "INVALID_REQUEST",
            "invalid bridge request",
        )),
    }
}

fn spawn_waiter(
    request_id: String,
    payload: WaitSyncPayload,
    mut updates: tokio::sync::broadcast::Receiver<encrypted_spaces_sdk::BroadcastEvent>,
    runtime: Arc<Mutex<runtime::Runtime>>,
    jobs: mpsc::SyncSender<RuntimeJob>,
    state: Arc<AtomicU8>,
) {
    thread::spawn(move || {
        let deadline =
            std::time::Instant::now() + Duration::from_millis(payload.wait_for_change_ms);
        let trigger = loop {
            if state.load(Ordering::Acquire) != PENDING {
                return;
            }
            if let Ok(runtime) = runtime.try_lock() {
                runtime.poll_background();
            }
            match updates.try_recv() {
                Ok(_) | Err(tokio::sync::broadcast::error::TryRecvError::Lagged(_)) => {
                    break "change";
                }
                Err(tokio::sync::broadcast::error::TryRecvError::Closed) => break "closed",
                Err(tokio::sync::broadcast::error::TryRecvError::Empty) => {}
            }
            let now = std::time::Instant::now();
            if now >= deadline {
                break "timeout";
            }
            thread::sleep(WAIT_POLL_INTERVAL.min(deadline.saturating_duration_since(now)));
        };
        if state.load(Ordering::Acquire) != PENDING {
            return;
        }
        let _ = jobs.send(RuntimeJob::Dispatch {
            request: Request {
                version: PROTOCOL_VERSION,
                request_id,
                operation: Operation::Sync,
                payload: serde_json::json!({"space_id": payload.space_id}),
            },
            state,
            wait_trigger: Some(trigger),
        });
    });
}

fn cancel_pending<W: Write>(
    request: Request,
    pending: &mut HashMap<String, PendingRequest>,
    writer: &mut W,
) -> io::Result<()> {
    if pending.contains_key(&request.request_id) {
        return write_response(
            writer,
            Response::error(
                None,
                "DUPLICATE_REQUEST_ID",
                "request ID is already pending",
            ),
        );
    }
    let payload = match serde_json::from_value::<CancelPayload>(request.payload) {
        Ok(payload) if !payload.request_id.is_empty() => payload,
        _ => {
            return write_response(
                writer,
                Response::error(
                    Some(request.request_id),
                    "INVALID_REQUEST",
                    "invalid bridge request",
                ),
            );
        }
    };
    if request.request_id == payload.request_id {
        return write_response(
            writer,
            Response::error(
                Some(request.request_id),
                "INVALID_REQUEST",
                "invalid bridge request",
            ),
        );
    }
    let canceled = pending.get(&payload.request_id).is_some_and(|pending| {
        pending.cancelable
            && pending
                .state
                .compare_exchange(PENDING, CANCELED, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
    });
    if canceled {
        pending.remove(&payload.request_id);
    }
    if canceled {
        write_response(
            writer,
            Response::error(
                Some(payload.request_id),
                "CANCELED",
                "bridge request canceled",
            ),
        )?;
    }
    write_response(
        writer,
        Response::success(request.request_id, CancelResult { canceled }),
    )
}

fn cancel_all_waits<W: Write>(
    pending: &mut HashMap<String, PendingRequest>,
    writer: &mut W,
) -> io::Result<()> {
    let request_ids: Vec<_> = pending
        .iter()
        .filter(|(_, pending)| {
            pending.cancelable
                && pending
                    .state
                    .compare_exchange(PENDING, CANCELED, Ordering::AcqRel, Ordering::Acquire)
                    .is_ok()
        })
        .map(|(request_id, _)| request_id.clone())
        .collect();
    for request_id in request_ids {
        pending.remove(&request_id);
        write_response(
            writer,
            Response::error(Some(request_id), "CANCELED", "bridge request canceled"),
        )?;
    }
    Ok(())
}

fn cancel_all_requests<W: Write>(
    pending: &mut HashMap<String, PendingRequest>,
    writer: &mut W,
) -> io::Result<()> {
    for (request_id, pending_request) in pending.drain() {
        pending_request.state.store(CANCELED, Ordering::Release);
        write_response(
            writer,
            Response::error(Some(request_id), "CANCELED", "bridge request canceled"),
        )?;
    }
    Ok(())
}

fn invalid_state_response(request_id: String) -> Response {
    Response::error(
        Some(request_id),
        "INVALID_STATE",
        "operation is invalid for current bridge state",
    )
}

fn invalid_state_response_optional(request_id: Option<String>) -> Response {
    Response::error(
        request_id,
        "INVALID_STATE",
        "operation is invalid for current bridge state",
    )
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
    if frame.len() > MAX_FRAME_BYTES {
        frame = serde_json::to_vec(&Response::error(
            response.request_id.clone(),
            "RESPONSE_TOO_LARGE",
            "bridge response exceeds maximum size",
        ))
        .map_err(io::Error::other)?;
    }
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
    fn protocol_encode_replaces_oversized_response_with_bounded_error() {
        let response =
            Response::success("oversized-response".to_owned(), "x".repeat(MAX_FRAME_BYTES));

        let encoded = encode_response(&response).expect("encode bounded response");
        assert!(encoded.len() <= MAX_FRAME_BYTES + 1);
        let value: Value = serde_json::from_slice(&encoded).expect("bounded response JSON");
        assert_eq!(value["request_id"], "oversized-response");
        assert_eq!(value["ok"], false);
        assert_eq!(value["error"]["code"], "RESPONSE_TOO_LARGE");
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
