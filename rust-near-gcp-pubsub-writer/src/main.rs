use serde_json;

use futures::StreamExt;
use tracing_subscriber::EnvFilter;

use google_cloud_pubsub::client::{ClientConfig, Client};
use google_cloud_googleapis::pubsub::v1::PubsubMessage;

use near_lake_framework::LakeConfigBuilder;

#[tokio::main]
async fn main() -> Result<(), tokio::io::Error> {
    init_tracing();

    // Start reading from NEAR Lake S3 files from a block height e.g. 121471200
    let block_height: u64 = std::env::var("BLOCK_HEIGHT")
        .expect("Invalid env var BLOCK_HEIGHT")
        .parse().unwrap();        

    let lake_config = LakeConfigBuilder::default()
        .mainnet()
        .start_block_height(block_height)
        .build()
        .expect("Failed to build LakeConfig");

    let (_, stream) = near_lake_framework::streamer(lake_config);

    // GCP Topic name. e.g. projects/pagoda-data-platform/topics/near-lakehouse-mainnet
    let gcp_pubsub_topic: String = std::env::var("GCP_PUBSUB_TOPIC")
    .expect("Invalid env var GCP_PUBSUB_TOPIC")
    .parse().unwrap();

    
    let pub_sub_config = ClientConfig::default().with_auth().await.unwrap();
    let client = Client::new(pub_sub_config).await.unwrap();
    let topic = client.topic(&gcp_pubsub_topic);
    let publisher = topic.new_publisher(None);

    let mut handlers = tokio_stream::wrappers::ReceiverStream::new(stream)
        .map(|msg| handle_streamer_message(msg, &publisher))
        .buffer_unordered(1usize);

    while let Some(_handle_message) = handlers.next().await {}

    Ok(())
}

async fn handle_streamer_message(
    streamer_message: near_lake_framework::near_indexer_primitives::StreamerMessage,
    publisher: &google_cloud_pubsub::publisher::Publisher,
) {
    let streamer_message_json = serde_json::to_string(&streamer_message).unwrap();

    let msg = PubsubMessage {
        data: streamer_message_json.clone().into_bytes(),
        ..Default::default()
    };

    let awaiter = publisher.publish(msg).await;

    let _ = awaiter.get().await;

    eprintln!(
        "{}",
        streamer_message.block.header.height
    );
}

fn init_tracing() {
    let mut env_filter = EnvFilter::new("near_lake_framework=info");

    if let Ok(rust_log) = std::env::var("RUST_LOG") {
        if !rust_log.is_empty() {
            for directive in rust_log.split(',').filter_map(|s| match s.parse() {
                Ok(directive) => Some(directive),
                Err(err) => {
                    eprintln!("Ignoring directive `{}`: {}", s, err);
                    None
                }
            }) {
                env_filter = env_filter.add_directive(directive);
            }
        }
    }

    tracing_subscriber::fmt::Subscriber::builder()
        .with_env_filter(env_filter)
        .with_writer(std::io::stderr)
        .init();
}