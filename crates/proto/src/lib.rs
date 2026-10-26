//! Generated client and server types for the gRPSQLite protocol, compiled from
//! `proto/grpsqlite.proto`.

// The crate holds only generated code, which newer clippy releases flag in
// ways its generators do not track: prost copies proto comments verbatim,
// and tonic marks its async-trait methods `#[must_use]`.
#![allow(clippy::all)]

tonic::include_proto!("grpsqlite.v2");
