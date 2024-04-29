
use actix_web::{get, post, web, App, HttpResponse, HttpServer, Responder, Result};

use anyhow::Context;
use tracing::info;

use near_jsonrpc_client::JsonRpcClient;

mod account_details;
mod lockup;
mod lockup_types;

struct AppState {
    rpc_client: JsonRpcClient,
}

const LOCKUPS: &str = "lockups";

#[get("/health")]
async fn health() -> impl Responder {
    HttpResponse::Ok().body("OK")
}

#[post("/lockup_amount")]
async fn lockup_amount(req: web::Json<lockup_types::RequestLockupAmount>, data: web::Data<AppState>) -> Result<impl Responder>  {
    let rpc_client = &data.rpc_client;

    let block_height = req.block_id;
    let block_timestamp = req.block_timestamp;
    let lockup_account_id: near_indexer_primitives::types::AccountId = req.lockup_account_id.parse().unwrap();

    let amount = compute_lockup_amount(&rpc_client, &block_height, block_timestamp, &lockup_account_id).await.unwrap();

    let response = lockup_types::ResponseLockupAmount {
        lockup_amount: amount,
    };

    info!(target: crate::LOCKUPS, "{:?} {:?} {:?} {:?}", req.block_id, req.lockup_account_id, block_timestamp, amount);

    Ok(web::Json(response))
}


#[actix_web::main]
async fn main() -> std::io::Result<()> {
    dotenv::dotenv().ok();

    HttpServer::new(|| {
        App::new()
            .app_data(web::Data::new(AppState {
                rpc_client: JsonRpcClient::connect(
                    std::env::var("RPC_URL").expect("RPC_URL must be set in either .env or environment"),
                ),
            }))
            .service(health)
            .service(lockup_amount)
    })
    .bind(("127.0.0.1", 8089))?
    .run()
    .await
}


async fn compute_lockup_amount(
    rpc_client: &JsonRpcClient,
    block_height: &u64,
    block_timestamp: u64,
    lockup_account_id: &near_indexer_primitives::types::AccountId,
) -> anyhow::Result<u128> {

    let state = lockup::get_lockup_contract_state(rpc_client, lockup_account_id, &block_height)
        .await
        .with_context(|| {
            format!(
                "Failed to get lockup contract details for {}",
                lockup_account_id
            )
        })?;
    let code_hash =
        account_details::get_contract_code_hash(rpc_client, lockup_account_id, &block_height)
            .await?;
    let is_lockup_with_bug = lockup::is_bug_inside_contract(&code_hash, lockup_account_id);
    let locked_amount = state
        .get_locked_amount(block_timestamp, is_lockup_with_bug)
        .0;

    Ok(locked_amount)
}
