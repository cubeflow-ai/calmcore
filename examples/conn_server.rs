use proto::calmserver::{server_client::ServerClient, LoadEngineRequest};

#[tokio::main]
pub async fn main() {
    let mut client = ServerClient::connect("http://127.0.0.1:6000")
        .await
        .unwrap();
    let vv = client
        .load_space(LoadEngineRequest {
            name: "tig".to_string(),
        })
        .await
        .unwrap();
    println!("{:?}", vv.into_inner());
}
