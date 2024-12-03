# NEAR Defuse Custom Indexer

This project is a Rust-based indexer that processes blockchain events from NEAR Protocol and inserts them into a Clickhouse database for efficient querying. It uses [NEAR Lake Framework](https://github.com/near/near-lake-framework) for streaming blockchain data and stores structured events in a Clickhouse database.

## Features

- Stream NEAR blockchain events starting from a specified block height.
- Filter and structure events based on specific standards.
- Store events in a Clickhouse database.

## Requirements

1. [Rust](https://www.rust-lang.org/tools/install) (1.76.0 version recommended)
2. [Clickhouse](https://clickhouse.com/docs/en/quick-start#self-managed-install) database server
3. Environment variables for configuration

### Build the project

```bash
cargo build --release
```

## Environment Configuration

Before running the indexer, ensure that all required environment variables are set, for example:

| Variable              | Description                     | Default Value            |
| --------------------- | ------------------------------- | ------------------------ |
| `CLICKHOUSE_URL`      | Clickhouse server URL           | `http://localhost:18123` |
| `CLICKHOUSE_USER`     | Username for Clickhouse         | `clickhouse`             |
| `CLICKHOUSE_PASSWORD` | Password for Clickhouse         | `clickhouse`             |
| `CLICKHOUSE_DB`       | Clickhouse database name        | `mainnet`                |
| `BLOCK_HEIGHT`        | Start block height for indexing | -                        |

Set the environment variables in a `.env` file or export them directly:

```bash
export CLICKHOUSE_URL="http://localhost:18123"
export CLICKHOUSE_USER="clickhouse"
export CLICKHOUSE_PASSWORD="clickhouse"
export CLICKHOUSE_DB="mainnet"
export BLOCK_HEIGHT="130636886"
```

## Usage

1. Start the Clickhouse server.
2. Run the application:

   ```bash
   cargo run --release
   ```

The application will:

- Connect to the Clickhouse server.
- Start reading blockchain events from the specified `BLOCK_HEIGHT` (or the last processed height).
- Filter and insert selected events into the Clickhouse database.

### Optional Command-line Execution

You can pass `BLOCK_HEIGHT` when running the program to override the default or previously set block height.

```bash
BLOCK_HEIGHT=130636886 cargo run --release
```

## Project Structure

The project is structured as follows:

- `main.rs`: Initializes the application, connects to Clickhouse, and starts the indexer.
- `database.rs`: Contains functions for connecting to Clickhouse and managing data.
- `event_handler.rs`: Processes streamed events and filters specific event standards.

## Clickhouse schema

Here is the Clickhouse schema to run the indexer:

```sql
CREATE TABLE IF NOT EXISTS events
    (
        block_height                     UInt64,
        block_timestamp                  DateTime64(9, 'UTC'),
        block_hash                       String,
        contract_id                      String,
        execution_status                 String,
        version                          String,
        standard                         String,
        event                        	  String,
        data                         	  String,
        related_receipt_id               String,
        related_receipt_receiver_id      String,
        related_receipt_predecessor_id   String,

        INDEX            block_timestamp_minmax_idx block_timestamp TYPE minmax GRANULARITY 1,
        INDEX            contract_id_bloom_index contract_id TYPE bloom_filter() GRANULARITY 1,
        INDEX            related_receipt_id_bloom_index related_receipt_id TYPE bloom_filter() GRANULARITY 1,
        INDEX            related_receipt_receiver_id_bloom_index related_receipt_receiver_id TYPE bloom_filter() GRANULARITY 1,
    ) ENGINE = ReplacingMergeTree
    PRIMARY KEY (block_height, related_receipt_id)
    ORDER BY (block_height, related_receipt_id);


CREATE TABLE IF NOT EXISTS decoded_mints
    (
        block_height                     UInt64,
        block_timestamp                  DateTime64(9, 'UTC'),
        block_hash                       String,
        contract_id                      String,
        execution_status                 String,
        version                        	 String,
        standard                         String,
        event                        	 String,
        data                         	 String,
        related_receipt_id               String,
        related_receipt_receiver_id      String,
        related_receipt_predecessor_id   String,
        memo                             Nullable(String),
        old_owner_id                     Nullable(String),
        new_owner_id                     Nullable(String),
        token_ids                        Nullable(String),
        amounts                          Nullable(String),

        INDEX            mints_block_timestamp_minmax_idx block_timestamp TYPE minmax GRANULARITY 1,
        INDEX            mints_contract_id_bloom_index contract_id TYPE bloom_filter() GRANULARITY 1,
        INDEX            mints_related_receipt_id_bloom_index related_receipt_id TYPE bloom_filter() GRANULARITY 1,
        INDEX            mints_related_receipt_receiver_id_bloom_index related_receipt_receiver_id TYPE bloom_filter() GRANULARITY 1,
    ) ENGINE = ReplacingMergeTree
    PRIMARY KEY (block_height, related_receipt_id)
    ORDER BY (block_height, related_receipt_id);

CREATE MATERIALIZED VIEW mv_decoded_mints TO decoded_mints AS
    WITH decoded_events AS (
        SELECT
            block_height
            , block_timestamp
            , block_hash
            , contract_id
            , execution_status
            , version
            , standard
            , event
            , related_receipt_id
            , related_receipt_predecessor_id
            , related_receipt_receiver_id
            , arrayJoin(JSONExtractArrayRaw(data)) data_row
        FROM events
        WHERE contract_id in ('defuse-alpha.near', 'intents.near')
    )
    SELECT
        *  EXCEPT (data_row)
        , COALESCE(JSON_VALUE(data_row, '$.memo'), '') memo
        , JSON_VALUE(data_row, '$.owner_id') old_owner_id
        , JSON_VALUE(data_row, '$.owner_id') new_owner_id
        , JSON_VALUE(data_row, '$.token_ids[*]') token_ids
        , JSON_VALUE(data_row, '$.amounts[*]') amounts
    FROM decoded_events
    WHERE event != 'mt_transfer' AND standard = 'nep245'
    settings function_json_value_return_type_allow_nullable=true;

```
