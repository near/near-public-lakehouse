use crate::types::{EventJson, EventRow};
use clickhouse::Client;
use futures::StreamExt;
use near_lake_framework::LakeConfig;
use near_lake_framework::near_indexer_primitives::{self, StreamerMessage, views::ExecutionStatusView};
use serde_json::from_str;
use tokio_stream::wrappers::ReceiverStream;

const EVENT_JSON_PREFIX: &str = "EVENT_JSON:";

pub async fn handle_stream(config: LakeConfig, client: Client) {
    let (_, stream) = near_lake_framework::streamer(config);

    let mut handlers = ReceiverStream::new(stream)
        .map(|message| handle_streamer_message(message, &client))
        .buffer_unordered(1);

    while let Some(_) = handlers.next().await {}
}

async fn handle_streamer_message(message: StreamerMessage, client: &Client) {
    println!("Block: {}", message.block.header.height);

    let rows: Vec<EventRow> = message
        .shards
        .iter()
        .flat_map(|shard| {
            shard.receipt_execution_outcomes.iter().flat_map(|outcome| {
                outcome
                    .execution_outcome
                    .outcome
                    .logs
                    .iter()
                    .enumerate()
                    .filter_map(|(index_in_log, log)| parse_event(index_in_log, log, outcome, &message.block.header))
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
    outcome: &near_indexer_primitives::IndexerExecutionOutcomeWithReceipt,
    header: &near_indexer_primitives::views::BlockHeaderView,
) -> Option<EventRow> {
    let log_trimmed = log.trim();

    if log_trimmed.starts_with(EVENT_JSON_PREFIX) {
        if let Ok(event) = from_str::<EventJson>(&log_trimmed[EVENT_JSON_PREFIX.len()..]) {
            if log_trimmed.contains("dip4") || log_trimmed.contains("nep245") {
                println!("Event: {}", log_trimmed);
                return Some(EventRow {
                    block_height: header.height,
                    block_timestamp: header.timestamp,
                    block_hash: header.hash.to_string(),
                    contract_id: outcome.execution_outcome.outcome.executor_id.to_string(),
                    execution_status: parse_status(outcome.execution_outcome.outcome.status.clone()),
                    version: event.version,
                    standard: event.standard,
                    index_in_log: index_in_log as u64,
                    event: event.event,
                    data:  event.data.to_string(),
                    related_receipt_id: outcome.receipt.receipt_id.to_string(),
                    related_receipt_receiver_id: outcome.receipt.receiver_id.to_string(),
                    related_receipt_predecessor_id: outcome.receipt.predecessor_id.to_string(),
                });
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