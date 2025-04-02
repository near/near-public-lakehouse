use crate::types::{EventJson, EventRow, ReceiptsCache, ReceiptsCacheArc, ReceiptOrDataId};
use clickhouse::Client;
use futures::StreamExt;
use near_lake_framework::LakeConfig;
use near_lake_framework::near_indexer_primitives::CryptoHash;
use near_lake_framework::near_indexer_primitives::{self, StreamerMessage, views::ExecutionStatusView};
use serde_json::from_str;
use tokio_stream::wrappers::ReceiverStream;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::Mutex;

const EVENT_JSON_PREFIX: &str = "EVENT_JSON:";
const CACHE_SIZE: usize = 10000;

pub async fn handle_stream(config: LakeConfig, client: Client) {
    let (_, stream) = near_lake_framework::streamer(config);
    let receipts_cache = Arc::new(Mutex::new(ReceiptsCache::new(CACHE_SIZE)));

    let mut handlers = ReceiverStream::new(stream)
        .map(|message| handle_streamer_message(message, &client, receipts_cache.clone()))
        .buffer_unordered(1);

    while let Some(_) = handlers.next().await {}
}

async fn handle_streamer_message(message: StreamerMessage, client: &Client, receipts_cache: ReceiptsCacheArc) {
    println!("Block: {}", message.block.header.height);

    let rows: Vec<EventRow> = message
        .shards
        .iter()
        .flat_map(|shard| {
            let chunk = Option::as_ref(&shard.chunk).unwrap();
            let header = message.block.header.clone();
            
            let receipts_cache_clone = receipts_cache.clone();
            shard.receipt_execution_outcomes.iter().flat_map(move |outcome| {
                let mut cache_lock = receipts_cache_clone.blocking_lock();
                let receipt_id = ReceiptOrDataId::ReceiptId(outcome.receipt.receipt_id);
                
                let tx_hash = if let Some(cached_hash) = cache_lock.cache_get(&receipt_id).cloned() {
                    cached_hash
                } else {
                    let hash = chunk
                        .transactions
                        .iter()
                        .find(|tx| {
                            tx.outcome
                                .execution_outcome
                                .outcome
                                .receipt_ids
                                .contains(&outcome.receipt.receipt_id)
                        })
                        .map(|tx| tx.transaction.hash);

                    match hash {
                        Some(h) => {
                            let hash_str = h.to_string();
                            cache_lock.cache_set(receipt_id, hash_str.clone());
                            hash_str
                        }
                        None => {
                            let hash_str = outcome.execution_outcome.id.to_string();
                            cache_lock.cache_set(receipt_id, hash_str.clone());
                            hash_str
                        }
                    }
                };
                
                drop(cache_lock);
                let header = header.clone();

                outcome
                    .execution_outcome
                    .outcome
                    .logs
                    .iter()
                    .enumerate()
                    .filter_map(move |(index_in_log, log)| 
                        parse_event(
                            index_in_log,
                            log,
                            CryptoHash::from_str(&tx_hash).unwrap(),
                            outcome,
                            &header
                        )
                    )
            })
        })
        .collect();

    if let Err(err) = crate::database::insert_rows(client, &rows).await {
        eprintln!("Error inserting rows into Clickhouse: {}", err);
    }
}

fn parse_event(
    index_in_log: usize,
    log: &str,
    tx_hash: CryptoHash,
    outcome: &near_indexer_primitives::IndexerExecutionOutcomeWithReceipt,
    header: &near_indexer_primitives::views::BlockHeaderView,
) -> Option<EventRow> {
    let log_trimmed = log.trim();

    if log_trimmed.starts_with(EVENT_JSON_PREFIX) {
        if let Ok(event) = from_str::<EventJson>(&log_trimmed[EVENT_JSON_PREFIX.len()..]) {
            let contract_id = &outcome.execution_outcome.outcome.executor_id.to_string();
            if contract_id == "intents.near" || contract_id == "defuse-alpha.near" {
                if log_trimmed.contains("dip4") || log_trimmed.contains("nep245") {
                    println!("Event: {}", log_trimmed);
                    return Some(EventRow {
                        block_height: header.height,
                        block_timestamp: header.timestamp,
                        block_hash: header.hash.to_string(),
                        contract_id: contract_id.to_string(),
                        execution_status: parse_status(outcome.execution_outcome.outcome.status.clone()),
                        version: event.version,
                        standard: event.standard,
                        index_in_log: index_in_log as u64,
                        event: event.event,
                        data:  event.data.to_string(),
                        related_receipt_id: outcome.receipt.receipt_id.to_string(),
                        related_receipt_receiver_id: outcome.receipt.receiver_id.to_string(),
                        related_receipt_predecessor_id: outcome.receipt.predecessor_id.to_string(),
                        tx_hash: tx_hash.to_string(),
                    });
                }
            }
        }
    }
    None
}

fn parse_status(status: ExecutionStatusView) -> String {
    match status {
        ExecutionStatusView::SuccessReceiptId(_) => "success_receipt_id".to_string(),
        ExecutionStatusView::SuccessValue(_) => "success_value".to_string(),
        ExecutionStatusView::Unknown => "unknown".to_string(),
        ExecutionStatusView::Failure(_) => "failure".to_string(),
    }
}
