fn main() -> Result<(), Box<dyn std::error::Error>> {
    let proto_dir = "../../proto";
    println!("cargo:rerun-if-changed={proto_dir}/grpsqlite.proto");
    let fds = protox::compile([format!("{proto_dir}/grpsqlite.proto")], [proto_dir])?;
    // `bytes` fields become `bytes::Bytes`, so a commit and the cache can share
    // page buffers without copying them.
    tonic_prost_build::configure().bytes(".").compile_fds(fds)?;
    Ok(())
}
