use clickhouse::Client;
use futures::StreamExt;
use tokio_stream::wrappers::ReceiverStream;

mod events;

use near_lake_framework::near_indexer_primitives::StreamerMessage;
use near_lake_framework::LakeConfig;

use crate::types::{self};

pub async fn handle_stream(
    config: LakeConfig,
    client: Client,
    receipts_cache_arc: types::ReceiptsCacheArc,
) {
    let (_, stream) = near_lake_framework::streamer(config);

    let mut handlers = ReceiverStream::new(stream)
        .map(|message| handle_streamer_message(message, &client, receipts_cache_arc.clone()))
        .buffer_unordered(1);

    while let Some(_) = handlers.next().await {}
}

async fn handle_streamer_message(
    message: StreamerMessage,
    client: &Client,
    receipts_cache_arc: types::ReceiptsCacheArc,
) {
    println!("Block: {}", message.block.header.height);

    if let Err(err) = events::handle_events(&message, client).await {
        eprintln!("Error handling events: {}", err);
    }
}
