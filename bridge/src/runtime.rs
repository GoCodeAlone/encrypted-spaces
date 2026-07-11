use crate::protocol::Response;
use crate::schema::Request;

/// Boundary only: SDK/backend runtime integration is pending.
pub fn dispatch(request: Request) -> Response {
    let _operation_name = request.operation.name();
    let _ = request.payload;
    Response::not_implemented(request.request_id)
}
