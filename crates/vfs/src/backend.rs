//! The VFS's only route to the server.
//!
//! [`crate::GrpcBackend`] sends each request over gRPC; the simulator executes
//! them against an in-process store. Requests and responses are the protocol's
//! own messages, so both carry identical semantics.

use grpsqlite_proto as proto;
use tonic::{Code, Status};

/// Executes protocol requests synchronously.
pub trait Backend: Send + Sync + 'static {
    /// Opens a session. For a read-write open, the backend keeps the lease
    /// alive until [`Backend::close`].
    fn open(&self, req: proto::OpenRequest) -> Result<proto::OpenResponse, Status>;
    fn close(&self, req: proto::CloseRequest) -> Result<(), Status>;
    fn read(&self, req: proto::ReadRequest) -> Result<proto::ReadResponse, Status>;
    fn write(&self, req: proto::WriteRequest) -> Result<(), Status>;
    fn commit(&self, req: proto::CommitRequest) -> Result<proto::CommitResponse, Status>;
    fn get_file_size(
        &self,
        req: proto::GetFileSizeRequest,
    ) -> Result<proto::GetFileSizeResponse, Status>;
    fn truncate(&self, req: proto::TruncateRequest) -> Result<(), Status>;
    fn delete(&self, req: proto::DeleteRequest) -> Result<(), Status>;
    fn pragma(&self, req: proto::PragmaRequest) -> Result<proto::PragmaResponse, Status>;
    /// Whether the session's lease is still safe to use: the server has not
    /// rejected a heartbeat, and the last accepted heartbeat was sent less
    /// than the lease's TTL ago.
    fn lease_valid(&self, session: &str) -> bool;
}

/// The server rejected the request's lease.
pub(crate) fn is_lease_error(status: &Status) -> bool {
    status.code() == Code::FailedPrecondition
}
