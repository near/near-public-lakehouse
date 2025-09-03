use std::env;

use cached::SizedCache;
use tokio::sync::Mutex;

use crate::config::init_tracing;
use crate::database::{get_last_height, init_clickhouse_client};

mod config;
mod database;
mod handlers;
mod types;

#[tokio::main]
async fn main() -> Result<(), tokio::io::Error> {
    dotenv::dotenv().ok();
    init_tracing();

    let client = init_clickhouse_client();

    let block_height: u64 = env::var("BLOCK_HEIGHT")
        .expect("Invalid env var BLOCK_HEIGHT")
        .parse()
        .expect("Failed to parse BLOCK_HEIGHT");

    let last_height = get_last_height(&client).await.unwrap_or(0);
    let start_block = block_height.max(last_height + 1);

    println!("Starting indexer at block height: {}", start_block);

    // TODO: enable this after development and testing
    // let lake_config = near_lake_framework::LakeConfigBuilder::default()
    //     .mainnet()
    //     .start_block_height(start_block)
    //     .build()
    //     .expect("Error creating NEAR Lake framework config");

    let lake_config = near_lake_framework::FastNearConfigBuilder::default()
        .mainnet()
        .start_block_height(start_block)
        .build()
        .expect("Error creating NEAR Lake framework config");

    let receipts_cache_arc: types::ReceiptsCacheArc =
        std::sync::Arc::new(Mutex::new(SizedCache::with_size(100_000)));

    handlers::handle_stream(lake_config, client, receipts_cache_arc).await;

    Ok(())
}
