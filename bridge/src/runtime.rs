use crate::protocol::Response;
use crate::schema::Request;

/// RED boundary only: Task 4 owns the real SDK/backend runtime integration.
pub fn dispatch(request: Request) -> Response {
    let _operation_name = request.operation.name();
    let _ = request.payload;
    Response::not_implemented(request.request_id)
}
