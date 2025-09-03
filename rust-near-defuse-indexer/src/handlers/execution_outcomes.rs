use std::str::FromStr;

use cached::Cached;

use near_lake_framework::near_indexer_primitives::{self, near_primitives};

use crate::{handlers::events::parse_status, types};

const EXECUTION_OUTCOMES_CLICKHOUSE_TABLE: &str = "execution_outcomes";

pub async fn handle_execution_outcomes(
    message: &near_indexer_primitives::StreamerMessage,
    client: &clickhouse::Client,
    receipts_cache_arc: types::ReceiptsCacheArc,
) -> anyhow::Result<()> {
    let execution_outcomes =
        extract_execution_outcomes(message, receipts_cache_arc.clone()).await?;

    if let Err(err) = crate::database::insert_rows(
        client,
        EXECUTION_OUTCOMES_CLICKHOUSE_TABLE,
        &execution_outcomes,
    )
    .await
    {
        eprintln!("Error inserting rows into Clickhouse: {}", err);
        anyhow::bail!("Failed to insert rows into Clickhouse: {}", err)
    }

    Ok(())
}

async fn extract_execution_outcomes(
    message: &near_indexer_primitives::StreamerMessage,
    receipts_cache_arc: types::ReceiptsCacheArc,
) -> anyhow::Result<Vec<types::ExecutionOutcomeRow>> {
    let mut receipts_cache_lock = receipts_cache_arc.lock().await;
    let block_height = message.block.header.height;
    let block_timestamp = message.block.header.timestamp;
    let block_hash = message.block.header.hash.to_string();
    let execution_outcomes = message
        .shards
        .iter()
        .flat_map(|shard| shard.receipt_execution_outcomes.iter())
        .flat_map(|outcome: &near_indexer_primitives::IndexerExecutionOutcomeWithReceipt| {
            if let Some(parent_tx_hash) = receipts_cache_lock.cache_get(&types::ReceiptOrDataId::ReceiptId(outcome.receipt.receipt_id)) {
                Some(types::ExecutionOutcomeRow {
                    block_height: block_height,
                    block_timestamp: block_timestamp,
                    block_hash: block_hash.clone(),
                    parent_transaction_hash: parent_tx_hash.clone(),
                    executor_id: outcome.execution_outcome.outcome.executor_id.to_string(),
                    status: parse_status(outcome.execution_outcome.outcome.status.clone()),
                    logs: serde_json::to_string(&outcome.execution_outcome.outcome.logs).unwrap(),
                    tokens_burnt: outcome.execution_outcome.outcome.tokens_burnt.to_string(),
                    gas_burnt: outcome.execution_outcome.outcome.gas_burnt,
                    receipt_ids: outcome.execution_outcome.outcome.receipt_ids.iter().map(|id| id.to_string()).collect(),
                })
            } else {
                println!("We don't watch for the this execution outcome but it is related to the account ids of interest, {}", outcome.receipt.receipt_id);
                None
            }
        })
        .collect::<Vec<_>>();

    // Add receipts that are going to be created by ExecutionOutcome to the cache (receipt-transaction matcher)
    execution_outcomes.iter().for_each(|outcome| {
        for receipt_id in &outcome.receipt_ids {
            receipts_cache_lock.cache_set(
                types::ReceiptOrDataId::ReceiptId(
                    near_primitives::hash::CryptoHash::from_str(receipt_id).unwrap(),
                ),
                outcome.parent_transaction_hash.clone(),
            );
        }
    });
    drop(receipts_cache_lock);
    Ok(execution_outcomes)
}
