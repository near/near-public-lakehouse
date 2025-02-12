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

| Variable                | Description                     | Default Value            |
| ----------------------- | ------------------------------- | ------------------------ |
| `CLICKHOUSE_URL`        | Clickhouse server URL           | `http://localhost:18123` |
| `CLICKHOUSE_USER`       | Username for Clickhouse         | `clickhouse`             |
| `CLICKHOUSE_PASSWORD`   | Password for Clickhouse         | `clickhouse`             |
| `CLICKHOUSE_DB`         | Clickhouse database name        | `mainnet`                |
| `BLOCK_HEIGHT`          | Start block height for indexing | -                        |
| `AWS_ACCESS_KEY_ID`     | AWS access key ID               | -                        |
| `AWS_SECRET_ACCESS_KEY` | AWS secret access key           | -                        |

Set the environment variables in a `.env` file or export them directly:

```bash
export CLICKHOUSE_URL="http://localhost:18123"
export CLICKHOUSE_USER="clickhouse"
export CLICKHOUSE_PASSWORD="clickhouse"
export CLICKHOUSE_DB="mainnet"
export BLOCK_HEIGHT="130636886"
export AWS_ACCESS_KEY_ID="your AWS access key ID"
export AWS_SECRET_ACCESS_KEY="your AWS secret access key"
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
CREATE TABLE defuse_assets (
    blockchain          String COMMENT 'The blockchain',
    contract_address    String COMMENT 'The contract address',
    decimals            UInt64 COMMENT 'Decimals',
    defuse_asset_id     String COMMENT 'The asset ID',
    price               Float64 COMMENT 'The price',
    price_updated_at    DateTime64(9, 'UTC') COMMENT 'The price updated timestamp',
    symbol              String COMMENT 'The symbol'
) ENGINE = ReplacingMergeTree
PRIMARY KEY (defuse_asset_id, price_updated_at)
ORDER BY (defuse_asset_id, price_updated_at);

CREATE MATERIALIZED VIEW mv_defuse_assets
REFRESH EVERY 1 DAY APPEND TO defuse_assets AS (
    WITH json_rows AS (
        SELECT 
        arrayJoin(items) item
        FROM url('https://api-mng-console.chaindefuser.com/api/tokens/', JSONEachRow)
    )

    SELECT 
        item.blockchain blockchain
        , item.contract_address contract_address
        , item.decimals decimals
        , item.defuse_asset_id defuse_asset_id
        , item.price price
        , item.price_updated_at price_updated_at
        , item.symbol symbol
    FROM json_rows
);

CREATE TABLE IF NOT EXISTS events
    (
        block_height                     UInt64 COMMENT 'The height of the block',
        block_timestamp                  DateTime64(9, 'UTC') COMMENT 'The timestamp of the block in UTC',
        block_hash                       String COMMENT 'The hash of the block',
        contract_id                      String COMMENT 'The ID of the account on which the execution outcome happens',
        execution_status                 String COMMENT 'The execution outcome status',
        version                        	 String COMMENT 'The event version',
        standard                         String COMMENT 'The event standard',
        event                        	 String COMMENT 'The event type',
        data                         	 String COMMENT 'The event JSON data',
        related_receipt_id               String COMMENT 'The execution outcome receipt ID',
        related_receipt_receiver_id      String COMMENT 'The destination account ID',
        related_receipt_predecessor_id   String COMMENT 'The account ID which issued a receipt. In case of a gas or deposit refund, the account ID is system',

        INDEX            block_timestamp_minmax_idx block_timestamp TYPE minmax GRANULARITY 1,
        INDEX            contract_id_bloom_index contract_id TYPE bloom_filter() GRANULARITY 1,
        INDEX            related_receipt_id_bloom_index related_receipt_id TYPE bloom_filter() GRANULARITY 1,
        INDEX            related_receipt_receiver_id_bloom_index related_receipt_receiver_id TYPE bloom_filter() GRANULARITY 1,
    ) ENGINE = ReplacingMergeTree
    PRIMARY KEY (block_height, related_receipt_id)
    ORDER BY (block_height, related_receipt_id);

CREATE TABLE IF NOT EXISTS silver_nep_245_events
    (
        block_height                     UInt64 COMMENT 'The height of the block',
        block_timestamp                  DateTime64(9, 'UTC') COMMENT 'The timestamp of the block in UTC',
        block_hash                       String COMMENT 'The hash of the block',
        contract_id                      String COMMENT 'The ID of the account on which the execution outcome happens',
        execution_status                 String COMMENT 'The execution outcome status',
        version                        	 String COMMENT 'The event version',
        standard                         String COMMENT 'The event standard',
        event                        	 String COMMENT 'The event type',
        related_receipt_id               String COMMENT 'The execution outcome receipt ID',
        related_receipt_receiver_id      String COMMENT 'The destination account ID',
        related_receipt_predecessor_id   String COMMENT 'The account ID which issued a receipt. In case of a gas or deposit refund, the account ID is system',
        memo                             Nullable(String) COMMENT 'The event memo',
        old_owner_id                     Nullable(String) COMMENT 'The old owner account ID',
        new_owner_id                     Nullable(String) COMMENT 'The new owner account ID',
        token_id                         Nullable(String) COMMENT 'The token ID',
        amount                           Nullable(Float64) COMMENT 'The amount',

        INDEX            nep_245_block_timestamp_minmax_idx block_timestamp TYPE minmax GRANULARITY 1,
        INDEX            nep_245_contract_id_bloom_index contract_id TYPE bloom_filter() GRANULARITY 1,
        INDEX            nep_245_related_receipt_id_bloom_index related_receipt_id TYPE bloom_filter() GRANULARITY 1,
        INDEX            nep_245_related_receipt_receiver_id_bloom_index related_receipt_receiver_id TYPE bloom_filter() GRANULARITY 1,
    ) ENGINE = ReplacingMergeTree
    PRIMARY KEY (block_height, related_receipt_id, event, old_owner_id, new_owner_id)
    ORDER BY (block_height, related_receipt_id, event, old_owner_id, new_owner_id)
settings allow_nullable_key=true;

CREATE MATERIALIZED VIEW mv_silver_nep_245_events TO silver_nep_245_events AS
WITH decoded_events AS (
    SELECT *
        , arrayJoin(JSONExtractArrayRaw(data)) data_row
    FROM events
    WHERE standard = 'nep245'
    -- timestamp threshold for new records
    -- previous records backfill can be done via https://clickhouse.com/docs/en/data-modeling/backfilling#timestamp-or-monotonically-increasing-column-available 
    AND block_timestamp >= '2025-02-12 22:10:00'
), tokens AS (
    SELECT * 
        , COALESCE(JSON_VALUE(data_row, '$.memo'), '') memo
        , if( event = 'mt_transfer', JSON_VALUE(data_row, '$.old_owner_id'), JSON_VALUE(data_row, '$.owner_id')) old_owner_id
        , if( event = 'mt_transfer', JSON_VALUE(data_row, '$.new_owner_id'), JSON_VALUE(data_row, '$.owner_id')) new_owner_id
        , JSONExtractArrayRaw(data_row, 'token_ids') token_ids
        , JSONExtractArrayRaw(data_row, 'amounts') amounts
    FROM decoded_events
), tokens_flattened AS (
    SELECT *
        , (arrayJoin(arrayZip(token_ids, amounts)) AS t).1 token_id, t.2 as amount
    FROM tokens
)
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
    , related_receipt_receiver_id
    , related_receipt_predecessor_id
    , memo
    , old_owner_id
    , new_owner_id
    , replaceAll(token_id, '"', '')  token_id 
    , replaceAll(amount, '"', '')::Float64 amount 
FROM tokens_flattened
settings function_json_value_return_type_allow_nullable=true;

CREATE TABLE IF NOT EXISTS silver_dip4_token_diff
    (
        block_height                     UInt64 COMMENT 'The height of the block',
        block_timestamp                  DateTime64(9, 'UTC') COMMENT 'The timestamp of the block in UTC',
        block_hash                       String COMMENT 'The hash of the block',
        contract_id                      String COMMENT 'The ID of the account on which the execution outcome happens',
        execution_status                 String COMMENT 'The execution outcome status',
        version                        	 String COMMENT 'The event version',
        standard                         String COMMENT 'The event standard',
        event                        	 String COMMENT 'The event type',
        related_receipt_id               String COMMENT 'The execution outcome receipt ID',
        related_receipt_receiver_id      String COMMENT 'The destination account ID',
        related_receipt_predecessor_id   String COMMENT 'The account ID which issued a receipt. In case of a gas or deposit refund, the account ID is system',
        account_id                       Nullable(String) COMMENT 'The token differential account ID',
        diff                             Nullable(String) COMMENT 'The token differentials',
        intent_hash                      String COMMENT 'The hash of the intent',
        referral                         Nullable(String) COMMENT 'The referral of the intent',

        INDEX            dif4_diff_block_timestamp_minmax_idx block_timestamp TYPE minmax GRANULARITY 1,
        INDEX            dif4_diff_contract_id_bloom_index contract_id TYPE bloom_filter() GRANULARITY 1,
        INDEX            dif4_diff_related_receipt_id_bloom_index related_receipt_id TYPE bloom_filter() GRANULARITY 1,
        INDEX            dif4_diff_related_receipt_receiver_id_bloom_index related_receipt_receiver_id TYPE bloom_filter() GRANULARITY 1,
    ) ENGINE = ReplacingMergeTree
    PRIMARY KEY (block_height, related_receipt_id, intent_hash)
    ORDER BY (block_height, related_receipt_id, intent_hash);

CREATE MATERIALIZED VIEW mv_silver_dip4_token_diff TO silver_dip4_token_diff AS
    WITH decoded_events AS (
        SELECT *
            , arrayJoin(JSONExtractArrayRaw(data)) data_row
        FROM events
        WHERE contract_id in ('defuse-alpha.near', 'intents.near')
        AND standard = 'dip4' 
        AND event = 'token_diff'
        -- timestamp threshold for new records
        -- previous records backfill can be done via https://clickhouse.com/docs/en/data-modeling/backfilling#timestamp-or-monotonically-increasing-column-available 
        AND block_timestamp >= '2025-02-12 22:50:00'
    )

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
        , COALESCE(JSON_VALUE(data_row, '$.account_id'), '') account_id
        , COALESCE(JSON_VALUE(data_row, '$.diff'), '') diff
        , COALESCE(JSON_VALUE(data_row, '$.intent_hash'), '') intent_hash
        , COALESCE(JSON_VALUE(data_row, '$.referral'), '') referral
    FROM decoded_events
    settings function_json_value_return_type_allow_nullable=true, function_json_value_return_type_allow_complex=true;


CREATE TABLE IF NOT EXISTS silver_dip4_public_keys
    (
        block_height                     UInt64 COMMENT 'The height of the block',
        block_timestamp                  DateTime64(9, 'UTC') COMMENT 'The timestamp of the block in UTC',
        block_hash                       String COMMENT 'The hash of the block',
        contract_id                      String COMMENT 'The ID of the account on which the execution outcome happens',
        execution_status                 String COMMENT 'The execution outcome status',
        version                        	 String COMMENT 'The event version',
        standard                         String COMMENT 'The event standard',
        event                        	 String COMMENT 'The event type',
        related_receipt_id               String COMMENT 'The execution outcome receipt ID',
        related_receipt_receiver_id      String COMMENT 'The destination account ID',
        related_receipt_predecessor_id   String COMMENT 'The account ID which issued a receipt. In case of a gas or deposit refund, the account ID is system',
        account_id                       String COMMENT 'The public key account ID',
        public_key                       String COMMENT 'The public key',

        INDEX            dip4_public_keys_block_timestamp_minmax_idx block_timestamp TYPE minmax GRANULARITY 1,
        INDEX            dip4_public_keys_contract_id_bloom_index contract_id TYPE bloom_filter() GRANULARITY 1,
        INDEX            dip4_public_keys_related_receipt_id_bloom_index related_receipt_id TYPE bloom_filter() GRANULARITY 1,
        INDEX            dip4_public_keys_related_receipt_receiver_id_bloom_index related_receipt_receiver_id TYPE bloom_filter() GRANULARITY 1,
    ) ENGINE = ReplacingMergeTree
    PRIMARY KEY (block_height, related_receipt_id, account_id)
    ORDER BY (block_height, related_receipt_id, account_id);

CREATE MATERIALIZED VIEW mv_silver_dip4_public_keys TO silver_dip4_public_keys AS
WITH decoded_events AS (
    SELECT
        *
        , data data_row
    FROM events
    WHERE contract_id in ('defuse-alpha.near', 'intents.near')
    AND standard = 'dip4' 
    AND event in ('public_key_added', 'public_key_removed')
    -- timestamp threshold for new records
    -- previous records backfill can be done via https://clickhouse.com/docs/en/data-modeling/backfilling#timestamp-or-monotonically-increasing-column-available 
    AND block_timestamp >= '2025-02-12 23:35:00'
)

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
    , COALESCE(JSON_VALUE(data_row, '$.account_id'), '') account_id
    , COALESCE(JSON_VALUE(data_row, '$.public_key'), '') public_key
FROM decoded_events
settings function_json_value_return_type_allow_nullable=true, function_json_value_return_type_allow_complex=true;

CREATE TABLE IF NOT EXISTS silver_dip4_intents_executed
    (
        block_height                     UInt64 COMMENT 'The height of the block',
        block_timestamp                  DateTime64(9, 'UTC') COMMENT 'The timestamp of the block in UTC',
        block_hash                       String COMMENT 'The hash of the block',
        contract_id                      String COMMENT 'The ID of the account on which the execution outcome happens',
        execution_status                 String COMMENT 'The execution outcome status',
        version                        	 String COMMENT 'The event version',
        standard                         String COMMENT 'The event standard',
        event                        	 String COMMENT 'The event type',
        related_receipt_id               String COMMENT 'The execution outcome receipt ID',
        related_receipt_receiver_id      String COMMENT 'The destination account ID',
        related_receipt_predecessor_id   String COMMENT 'The account ID which issued a receipt. In case of a gas or deposit refund, the account ID is system',
        account_id                       String COMMENT 'The intent executed account ID',
        intent_hash                      String COMMENT 'The intent executed hash',

        INDEX            dip4_intents_executed_block_timestamp_minmax_idx block_timestamp TYPE minmax GRANULARITY 1,
        INDEX            dip4_intents_executed_contract_id_bloom_index contract_id TYPE bloom_filter() GRANULARITY 1,
        INDEX            dip4_intents_executed_related_receipt_id_bloom_index related_receipt_id TYPE bloom_filter() GRANULARITY 1,
        INDEX            dip4_intents_executed_related_receipt_receiver_id_bloom_index related_receipt_receiver_id TYPE bloom_filter() GRANULARITY 1,
    ) ENGINE = ReplacingMergeTree
    PRIMARY KEY (block_height, related_receipt_id, intent_hash)
    ORDER BY (block_height, related_receipt_id, intent_hash);

CREATE MATERIALIZED VIEW mv_silver_dip4_intents_executed TO silver_dip4_intents_executed AS
    WITH decoded_events AS (
        SELECT *
            , arrayJoin(JSONExtractArrayRaw(data)) data_row
        FROM events
        WHERE contract_id in ('defuse-alpha.near', 'intents.near')
        AND standard = 'dip4' and event = 'intents_executed'
        -- timestamp threshold for new records
        -- previous records backfill can be done via https://clickhouse.com/docs/en/data-modeling/backfilling#timestamp-or-monotonically-increasing-column-available 
        AND block_timestamp >= '2025-02-12 23:45:00'
    )

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
        , COALESCE(JSON_VALUE(data_row, '$.account_id'), '') account_id
        , COALESCE(JSON_VALUE(data_row, '$.intent_hash'), '') intent_hash
    FROM decoded_events
    settings function_json_value_return_type_allow_nullable=true, function_json_value_return_type_allow_complex=true;


CREATE TABLE IF NOT EXISTS silver_dip4_fee_changed
    (
        block_height                     UInt64 COMMENT 'The height of the block',
        block_timestamp                  DateTime64(9, 'UTC') COMMENT 'The timestamp of the block in UTC',
        block_hash                       String COMMENT 'The hash of the block',
        contract_id                      String COMMENT 'The ID of the account on which the execution outcome happens',
        execution_status                 String COMMENT 'The execution outcome status',
        version                        	 String COMMENT 'The event version',
        standard                         String COMMENT 'The event standard',
        event                        	 String COMMENT 'The event type',
        related_receipt_id               String COMMENT 'The execution outcome receipt ID',
        related_receipt_receiver_id      String COMMENT 'The destination account ID',
        related_receipt_predecessor_id   String COMMENT 'The account ID which issued a receipt. In case of a gas or deposit refund, the account ID is system',
        old_fee                          String COMMENT 'The old fee',
        new_fee                          String COMMENT 'The new fee',

        INDEX            dip4_fee_changed_block_timestamp_minmax_idx block_timestamp TYPE minmax GRANULARITY 1,
        INDEX            dip4_fee_changed_contract_id_bloom_index contract_id TYPE bloom_filter() GRANULARITY 1,
        INDEX            dip4_fee_changed_related_receipt_id_bloom_index related_receipt_id TYPE bloom_filter() GRANULARITY 1,
        INDEX            dip4_fee_changed_related_receipt_receiver_id_bloom_index related_receipt_receiver_id TYPE bloom_filter() GRANULARITY 1,
    ) ENGINE = ReplacingMergeTree
    PRIMARY KEY (block_height, related_receipt_id)
    ORDER BY (block_height, related_receipt_id);

CREATE MATERIALIZED VIEW silver_mv_dip4_fee_changed TO silver_dip4_fee_changed AS
    WITH decoded_events AS (
        SELECT *
            , data data_row
        FROM events
        WHERE contract_id in ('defuse-alpha.near', 'intents.near')
        AND standard = 'dip4' and event = 'fee_changed'
        -- timestamp threshold for new records
        -- previous records backfill can be done via https://clickhouse.com/docs/en/data-modeling/backfilling#timestamp-or-monotonically-increasing-column-available 
        AND block_timestamp >= '2025-02-12 23:50:00'
    )

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
        , COALESCE(JSON_VALUE(data_row, '$.old_fee'), '') old_fee
        , COALESCE(JSON_VALUE(data_row, '$.new_fee'), '') new_fee
    FROM decoded_events
    settings function_json_value_return_type_allow_nullable=true, function_json_value_return_type_allow_complex=true;
```
