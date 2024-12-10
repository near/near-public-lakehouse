# Databricks notebook source
# MAGIC %md
# MAGIC # Aggregated Circulating Supply Pipeline
# MAGIC
# MAGIC There are lockup contracts that contains a bug in the lockup amount calculation method, for this reason we can't rely entirely on RPC function calls.
# MAGIC Indexer for explorer handled this issue. We created a RUST API that implement the Indexer for explorer logic and deployed in GCP Cloud Run to use in this pipeline.
# MAGIC - Code: https://github.com/near/near-public-lakehouse/tree/main/rust-extract-apis/lockups
# MAGIC - RUST API endpoint: https://near-public-lakehouse-24ktefolwq-uc.a.run.app/lockup_amount
# MAGIC
# MAGIC The API returns the lockup amount per account that will give us better data granularity.
# MAGIC
# MAGIC
# MAGIC References:
# MAGIC - Indexer for Explorer: https://github.com/near/near-indexer-for-explorer/blob/master/circulating-supply/src/main.rs#L160
# MAGIC - Lockup Contract: https://github.com/near/core-contracts/blob/master/lockup/src/getters.rs#L65

# COMMAND ----------

!pip install httpx[http2]=="0.25.2" httpcore[asyncio]

# COMMAND ----------

# MAGIC %md
# MAGIC # 1) Create gold_aggregated_circulating_supply record for the date

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_gold_aggregated_circulating_supply AS  
# MAGIC     WITH blocks_daily AS (
# MAGIC       SELECT 
# MAGIC           b.block_date, 
# MAGIC           max(b.block_timestamp) AS computed_at_block_timestamp
# MAGIC       FROM mainnet.silver_blocks b
# MAGIC       GROUP BY 1
# MAGIC       ORDER BY 2 DESC
# MAGIC     )
# MAGIC
# MAGIC     SELECT 
# MAGIC       bd.*,
# MAGIC       b.block_hash as computed_at_block_hash,
# MAGIC       b.block_height as computed_at_block_height,
# MAGIC       CAST(0 AS DOUBLE) AS circulating_tokens_supply,
# MAGIC       CAST(b.total_supply AS DOUBLE) AS total_tokens_supply,
# MAGIC       CAST(0 AS INT) AS total_lockup_contracts_count,
# MAGIC       CAST(0 AS INT) AS unfinished_lockup_contracts_count,
# MAGIC       CAST(0 AS DOUBLE) AS foundation_locked_tokens,
# MAGIC       CAST(0 AS DOUBLE) AS lockups_locked_tokens
# MAGIC     FROM blocks_daily bd
# MAGIC     JOIN mainnet.silver_blocks b ON bd.block_date = b.block_date AND bd.computed_at_block_timestamp = b.block_timestamp
# MAGIC     ORDER BY bd.block_date DESC

# COMMAND ----------

# Ensure the table exists
spark.sql("""
    CREATE TABLE IF NOT EXISTS mainnet.gold_aggregated_circulating_supply
    PARTITIONED BY (block_date)
    LOCATION '/pipelines/mainnet_silver_stream/tables/scheduled/gold_aggregated_circulating_supply_v3'
    COMMENT 'Gold aggregated circulating supply' AS
    SELECT *
    FROM temp_gold_aggregated_circulating_supply
    WHERE 1=2
""")

# COMMAND ----------

# MAGIC %md
# MAGIC -- A one time run to backfill the table with previous fivetran postgres explorer db records until 2024-03-20
# MAGIC ```sql
# MAGIC MERGE INTO mainnet.gold_aggregated_circulating_supply as t
# MAGIC USING (
# MAGIC   SELECT 
# MAGIC     CAST(to_timestamp(acs.computed_at_block_timestamp / 1000 / 1000 / 1000) AS DATE) as block_date,
# MAGIC     b.block_height as computed_at_block_height,
# MAGIC     acs.* EXCEPT (acs.`_fivetran_deleted`, acs.`_fivetran_synced`)
# MAGIC   FROM mainnet_explorer_public.aggregated__circulating_supply acs
# MAGIC   JOIN mainnet.silver_blocks b ON acs.computed_at_block_timestamp = b.block_timestamp
# MAGIC   ) s
# MAGIC ON s.block_date = t.block_date 
# MAGIC WHEN NOT MATCHED THEN INSERT *
# MAGIC ```

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO mainnet.gold_aggregated_circulating_supply as t
# MAGIC USING (
# MAGIC     SELECT * FROM temp_gold_aggregated_circulating_supply 
# MAGIC     -- WHERE block_date > '2024-04-22' AND block_date <= '2024-04-28'
# MAGIC     WHERE block_date = (current_date() - INTERVAL '1' DAY) 
# MAGIC ) s
# MAGIC ON s.block_date = t.block_date 
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # 2) Update silver_aggregated_lockups with the created and deleted block heights 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE mainnet.silver_aggregated_lockups AS 
# MAGIC SELECT
# MAGIC   receipt_receiver_account_id as account_id,
# MAGIC   MIN(CASE WHEN action_kind IN ('CREATE_ACCOUNT', 'TRANSFER') THEN block_height ELSE NULL END) AS creation_block_height,
# MAGIC   MIN(CASE WHEN action_kind = 'DELETE_ACCOUNT' THEN block_height ELSE NULL END) AS deletion_block_height
# MAGIC FROM mainnet.silver_action_receipt_actions
# MAGIC WHERE 
# MAGIC   receipt_receiver_account_id LIKE '%.lockup.near'
# MAGIC   AND action_kind IN ('CREATE_ACCOUNT', 'DELETE_ACCOUNT', 'TRANSFER')
# MAGIC GROUP BY account_id
# MAGIC ORDER BY account_id

# COMMAND ----------

# MAGIC %md
# MAGIC # 3) Update the silver_lockup_amount_account_daily using a RUST API

# COMMAND ----------

import asyncio
import httpx
import requests
import time

import base64 
import json
import re
import random

from datetime import datetime, timedelta

from delta.tables import *
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, BooleanType, BinaryType

YESTERDAY = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

# Read RPC URL 
READ_RPC_URL = "https://archival-rpc.mainnet.near.org"

ALL_RPC_SERVERS_AND_HISTORICAL_DATA = [
    "https://rpc.mainnet.near.org",
    READ_RPC_URL,
    "https://1rpc.io/near"
]

CUSTOM_RUST_API = "https://near-public-lakehouse-24ktefolwq-uc.a.run.app/lockup_amount"

# COMMAND ----------

async def fetch_lockup_amount(client, account_id, block_height, block_timestamp):

    response = await client.post(
        url = CUSTOM_RUST_API,
        timeout=httpx.Timeout(20.0, read=None),
        json={
                "block_id": block_height,
                "block_timestamp": block_timestamp,
                "lockup_account_id": account_id,
            }
    )
    print(response.text)
    if response.status_code != 200 or "lockup_amount" not in response.text:
        raise Exception(f"ERROR: on acct {account_id}, block {block_height}, response {response.text}")
    else:
        return json.loads(response.text)

# COMMAND ----------

async def rust_api_get_lockup_amount(account_id, block_height, block_timestamp):
    locked_amount = "0"
    try:
        async with httpx.AsyncClient(http2=False, transport=httpx.AsyncHTTPTransport(retries=0)) as client:

            response = await fetch_lockup_amount(client, account_id, block_height, block_timestamp)

            locked_amount = response["lockup_amount"]
    except Exception as e:
        locked_amount = e
        print(e)

    return locked_amount

# Wrapper function to handle the asynchronous call
def rust_api_get_lockup_amount_wrapper(account_id, block_height, block_timestamp):
    # wait 1.5 sec to avoid RPC rate limit errors
    # time.sleep(1.5)
    return asyncio.run(rust_api_get_lockup_amount(account_id, block_height, block_timestamp))


# Register the function as a UDF
rust_api_get_lockup_amount = spark.udf.register("rust_api_get_lockup_amount", rust_api_get_lockup_amount_wrapper)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_lockup_amount_account_daily AS  
# MAGIC     SELECT
# MAGIC       ds.block_date,
# MAGIC       ds.computed_at_block_height,
# MAGIC       ds.computed_at_block_timestamp,
# MAGIC       al.account_id,
# MAGIC       "0" AS lockup_amount
# MAGIC     FROM mainnet.silver_aggregated_lockups al
# MAGIC     JOIN mainnet.gold_aggregated_circulating_supply ds ON (al.creation_block_height IS NULL OR al.creation_block_height <= ds.computed_at_block_height)
# MAGIC     AND (al.deletion_block_height IS NULL OR al.deletion_block_height >= ds.computed_at_block_height)

# COMMAND ----------

# Ensure the table exists
spark.sql("""
    CREATE TABLE IF NOT EXISTS mainnet.silver_lockup_amount_account_daily
    PARTITIONED BY (block_date)
    LOCATION '/pipelines/mainnet_silver_stream/tables/scheduled/silver_lockup_amount_account_daily_v7'
    COMMENT 'Silver lockup amount per account daily' AS
    SELECT *
    FROM temp_lockup_amount_account_daily
    WHERE 1=2
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO mainnet.silver_lockup_amount_account_daily as t
# MAGIC USING (
# MAGIC     SELECT
# MAGIC       la.block_date,
# MAGIC       la.computed_at_block_height,
# MAGIC       la.computed_at_block_timestamp,
# MAGIC       la.account_id,
# MAGIC       rust_api_get_lockup_amount(la.account_id, la.computed_at_block_height, la.computed_at_block_timestamp) AS lockup_amount
# MAGIC     FROM (
# MAGIC       SELECT * FROM temp_lockup_amount_account_daily td 
# MAGIC       -- WHERE td.block_date >= '2024-09-23' AND td.block_date <= '2024-10-15' 
# MAGIC       WHERE td.block_date = (current_date() - INTERVAL '1' DAY)  
# MAGIC       ORDER BY td.block_date
# MAGIC     ) la
# MAGIC     ORDER BY la.block_date DESC
# MAGIC
# MAGIC ) s
# MAGIC ON s.block_date = t.block_date AND s.account_id = t.account_id
# MAGIC WHEN MATCHED THEN UPDATE SET t.lockup_amount = s.lockup_amount
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %md
# MAGIC # 4) Update the silver_lockup_amount_nf_daily using RPC calls

# COMMAND ----------

async def fetch_account(client, account_id, block_height):
    for rpc_url in ALL_RPC_SERVERS_AND_HISTORICAL_DATA:
        response = await client.post(
            url = rpc_url,
            timeout=httpx.Timeout(20.0, read=None),
            json={
                "jsonrpc": "2.0",
                "id": "dontcare",
                "method": "query",
                "params": {
                    "request_type": "view_account",
                    "block_id": block_height,
                    "account_id": account_id,
                    "args_base64": "",
                }
            }
        )
        if response.status_code != 200 or "result" not in response.text:
            continue
        else:
            return json.loads(response.text)
    
    raise Exception(f"ERROR: on acct {account_id}, block {block_height}, response {response.text}")

async def rpc_call_view_account(account_id, block_height):
    locked_amount = "0"
    try:
        async with httpx.AsyncClient(http2=False, transport=httpx.AsyncHTTPTransport(retries=1)) as client:
            response = await fetch_account(client, account_id, block_height)
            locked_amount = response["result"]["amount"]
    except Exception as e:
        locked_amount = e
        print(e)

    return locked_amount

# Wrapper function to handle the asynchronous call
def rpc_call_view_account_wrapper(account_id, block_height):
    return asyncio.run(rpc_call_view_account(account_id, block_height))


# Register the function as a UDF
rpc_call_view_account = spark.udf.register("rpc_call_view_account", rpc_call_view_account_wrapper)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_lockup_amount_nf_daily AS  
# MAGIC     SELECT
# MAGIC       ds.block_date,
# MAGIC       rpc_call_view_account('lockup.near', ds.computed_at_block_height) AS lockup_near_amount,
# MAGIC       rpc_call_view_account('contributors.near', ds.computed_at_block_height) AS contributors_near_amount
# MAGIC     FROM mainnet.gold_aggregated_circulating_supply ds

# COMMAND ----------

# Ensure the table exists
spark.sql("""
    CREATE TABLE IF NOT EXISTS mainnet.silver_lockup_amount_nf_daily
    PARTITIONED BY (block_date)
    LOCATION '/pipelines/mainnet_silver_stream/tables/scheduled/silver_lockup_amount_nf_daily_v2'
    COMMENT 'Silver NF and Contributors lockup amount daily' AS
    SELECT * FROM temp_lockup_amount_nf_daily
    WHERE 1=2
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO mainnet.silver_lockup_amount_nf_daily as t
# MAGIC USING (
# MAGIC     SELECT * FROM temp_lockup_amount_nf_daily 
# MAGIC     -- WHERE block_date >= '2024-09-23' AND block_date <= '2024-10-15'
# MAGIC     WHERE block_date = (current_date() - INTERVAL '1' DAY)
# MAGIC     ) s
# MAGIC ON s.block_date = t.block_date 
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %md
# MAGIC # 5) Update the gold_aggregated_circulating_supply using data above

# COMMAND ----------

df_dates = spark.sql("""
    SELECT block_date 
    FROM mainnet.gold_aggregated_circulating_supply 
    -- WHERE block_date >= '2024-09-23' AND block_date <= '2024-10-15'
    WHERE block_date = (current_date() - INTERVAL '1' DAY)
    ORDER BY block_date
    """)

for date_process in df_dates.collect():
    print(date_process["block_date"])
    df_supply = spark.sql(f"""
        MERGE INTO mainnet.gold_aggregated_circulating_supply as t
        USING (
        WITH supply AS (
            SELECT block_date, total_tokens_supply FROM mainnet.gold_aggregated_circulating_supply WHERE block_date = '{date_process["block_date"]}'
        ),
        accts AS (
            SELECT sum(CAST(lockup_amount AS DOUBLE)) AS lockups_locked_tokens
            FROM mainnet.silver_lockup_amount_account_daily 
            WHERE block_date = '{date_process["block_date"]}' AND lockup_amount <> 'error'
        ), nf AS (
            SELECT sum(lockup_near_amount + contributors_near_amount) AS foundation_locked_tokens
            FROM mainnet.silver_lockup_amount_nf_daily
            WHERE block_date = '{date_process["block_date"]}'
        ), counts AS (
            SELECT 
                count(account_id) AS total_lockup_contracts_count,
                count_if(lockup_amount <> "0") AS unfinished_lockup_contracts_count
            FROM mainnet.silver_lockup_amount_account_daily
            WHERE block_date = '{date_process["block_date"]}'
        )

        SELECT
            block_date,
            lockups_locked_tokens,
            foundation_locked_tokens,
            total_lockup_contracts_count,
            unfinished_lockup_contracts_count,
            total_tokens_supply - (lockups_locked_tokens + foundation_locked_tokens) AS circulating_tokens_supply
        FROM supply, nf, accts, counts
        ) s
        ON s.block_date = t.block_date 
        WHEN MATCHED THEN UPDATE SET 
            t.lockups_locked_tokens = s.lockups_locked_tokens,
            t.foundation_locked_tokens = s.foundation_locked_tokens,
            t.total_lockup_contracts_count = s.total_lockup_contracts_count,
            t.unfinished_lockup_contracts_count = s.unfinished_lockup_contracts_count,
            t.circulating_tokens_supply = s.circulating_tokens_supply
    """)
    display(df_supply)

# COMMAND ----------

