mod config;
mod database;
mod event_handler;
mod types;

use crate::config::init_tracing;
use crate::database::{get_last_height, init_clickhouse_client};
use crate::event_handler::handle_stream;

use near_lake_framework::LakeConfigBuilder;
use std::env;

#[tokio::main]
async fn main() -> Result<(), tokio::io::Error> {
    init_tracing();

    let client = init_clickhouse_client();

    let block_height: u64 = env::var("BLOCK_HEIGHT")
        .expect("Invalid env var BLOCK_HEIGHT")
        .parse()
        .expect("Failed to parse BLOCK_HEIGHT");

    let last_height = get_last_height(&client).await.unwrap_or(0);
    let start_block = block_height.max(last_height + 1);
    
    println!("Starting indexer at block height: {}", start_block);

    let lake_config = LakeConfigBuilder::default()
        .mainnet()
        .start_block_height(start_block)
        .build()
        .expect("Error creating NEAR Lake framework config");

    handle_stream(lake_config, client).await;

    Ok(())
}