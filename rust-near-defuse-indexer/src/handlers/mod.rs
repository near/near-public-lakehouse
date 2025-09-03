use clickhouse::Client;
use futures::StreamExt;
use tokio_stream::wrappers::ReceiverStream;

mod events;
mod execution_outcomes;
mod receipts;
mod transactions;

use near_lake_framework::near_indexer_primitives::StreamerMessage;

use crate::types::{self};

pub async fn handle_stream<T: Into<near_lake_framework::FastNearConfig>>(
    config: T,
    client: Client,
    receipts_cache_arc: types::ReceiptsCacheArc,
) {
    let (_, stream) = near_lake_framework::streamer(config.into());

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
    // TODO: remove it after tests
    if message.block.header.height > 162389776 {
        std::process::exit(0);
    }

    let transactions_future =
        transactions::handle_transactions(&message, client, receipts_cache_arc.clone());
    let execution_outcomes_future =
        execution_outcomes::handle_execution_outcomes(&message, client, receipts_cache_arc.clone());
    let receipts_future = receipts::handle_receipts(&message, client, receipts_cache_arc.clone());
    let events_future = events::handle_events(&message, client);

    if let Err(e) = futures::try_join!(
        transactions_future,
        execution_outcomes_future,
        receipts_future,
        events_future
    ) {
        eprintln!("Error handling streamer message: {:?}", e);
    }
}
