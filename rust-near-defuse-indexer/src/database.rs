use clickhouse::{Client, Row};
use std::env;

// Initializes the Clickhouse client using environment variables.
/// Environment variables required:
/// - `CLICKHOUSE_URL`
/// - `CLICKHOUSE_USER`
/// - `CLICKHOUSE_PASSWORD`
/// - `CLICKHOUSE_DATABASE`
pub fn init_clickhouse_client() -> Client {
    let url = env::var("CLICKHOUSE_URL").expect("CLICKHOUSE_URL not set in environment");
    let user = env::var("CLICKHOUSE_USER").expect("CLICKHOUSE_USER not set in environment");
    let password =
        env::var("CLICKHOUSE_PASSWORD").expect("CLICKHOUSE_PASSWORD not set in environment");
    let database =
        env::var("CLICKHOUSE_DATABASE").expect("CLICKHOUSE_DATABASE not set in environment");

    Client::default()
        .with_url(&url)
        .with_user(&user)
        .with_password(&password)
        .with_database(&database)
}

pub async fn get_last_height(client: &Client) -> Result<u64, clickhouse::error::Error> {
    client
        .query("SELECT max(block_height) FROM events")
        .fetch_one::<u64>()
        .await
}

pub async fn insert_rows(
    client: &Client,
    table: &str,
    rows: &[impl Row + serde::Serialize],
) -> Result<(), Box<dyn std::error::Error>> {
    let mut insert = client.insert(table).unwrap();
    for row in rows {
        insert.write(row).await?;
    }
    insert.end().await?;
    Ok(())
}
