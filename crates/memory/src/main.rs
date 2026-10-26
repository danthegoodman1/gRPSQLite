//! Runs the in-memory gRPSQLite server.
//!
//! ```text
//! memory-server [--addr 127.0.0.1:50051] [--no-atomic] [--no-pit]
//!               [--lease-ttl-ms 5000] [--latency-ms 0]
//! ```

use std::sync::Arc;
use std::time::Duration;

use grpsqlite_memory::{Options, Service, Store};
use parking_lot::Mutex;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut addr = "127.0.0.1:50051".to_string();
    let mut opts = Options::default();
    let mut latency = Duration::ZERO;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--addr" => addr = args.next().ok_or("--addr needs a value")?,
            "--no-atomic" => opts.atomic_commit = false,
            "--no-pit" => opts.point_in_time_reads = false,
            "--lease-ttl-ms" => {
                opts.lease_ttl_ms = args.next().ok_or("--lease-ttl-ms needs a value")?.parse()?;
                opts.heartbeat_interval_ms = (opts.lease_ttl_ms / 5).max(1);
            }
            "--latency-ms" => {
                latency =
                    Duration::from_millis(args.next().ok_or("--latency-ms needs a value")?.parse()?)
            }
            _ => return Err(format!("unknown argument {arg}").into()),
        }
    }
    let listener = tokio::net::TcpListener::bind(&addr).await?;
    println!(
        "memory-server on {} (atomic_commit={}, point_in_time_reads={})",
        listener.local_addr()?,
        opts.atomic_commit,
        opts.point_in_time_reads
    );
    let service = Service::new(Arc::new(Mutex::new(Store::new(opts)))).with_latency(latency);
    grpsqlite_memory::serve(listener, service).await?;
    Ok(())
}
