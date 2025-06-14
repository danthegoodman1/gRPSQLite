pub fn add(left: u64, right: u64) -> u64 {
    left + right
}

tonic::include_proto!("grpc_vfs");

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn it_works() {
        let result = add(2, 2);
        assert_eq!(result, 4);
    }
}
