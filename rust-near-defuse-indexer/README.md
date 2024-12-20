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

| Variable             | Description                          | Default Value        |
|----------------------|--------------------------------------|----------------------|
| `CLICKHOUSE_URL`     | Clickhouse server URL               | `http://localhost:18123` |
| `CLICKHOUSE_USER`    | Username for Clickhouse             | `clickhouse`         |
| `CLICKHOUSE_PASSWORD`| Password for Clickhouse             | `clickhouse`         |
| `CLICKHOUSE_DB`      | Clickhouse database name            | `mainnet`            |
| `BLOCK_HEIGHT`       | Start block height for indexing     | -                    |

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
