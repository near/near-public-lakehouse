# Databricks notebook source
# MAGIC %md
# MAGIC # Epochs and validators Silver Pipeline

# COMMAND ----------

# MAGIC %pip install httpx[http2]=="0.25.2"

# COMMAND ----------

import asyncio
import httpx
import requests

import base64 
import json
import re
import random

from datetime import datetime, timedelta

from delta.tables import *
from pyspark.sql.functions import udf, col
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, ArrayType, BooleanType

YESTERDAY = (datetime.now() - timedelta(days=1)).strftime("%Y-%m-%d")

# Read RPC URL 
# READ_RPC_URL = "https://archival-rpc.mainnet.near.org"
READ_RPC_URL = "https://v2.rpc.mainnet.near.org"

ALL_RPC_SERVERS_AND_HISTORICAL_DATA = [
    READ_RPC_URL,
    # "https://rpc.mainnet.near.org",
    # "https://1rpc.io/near",
    # "https://rpc.ankr.com/near",
    # "https://endpoints.omniatech.io/v1/near/mainnet/public",
]

# COMMAND ----------

# MAGIC %md
# MAGIC # Epochs table

# COMMAND ----------

# MAGIC %sql
# MAGIC    -- A temp view will make it easier to reuse the SQL in the CREATE TABLE and MERGE operations
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_view_silver_epochs AS 
# MAGIC   SELECT
# MAGIC       header.epoch_id,
# MAGIC       MIN(block_height) AS epoch_min_block_height,
# MAGIC       MAX(block_height) AS epoch_max_block_height,
# MAGIC       MIN(block_date) AS min_block_date,
# MAGIC       MAX(block_date) AS max_block_date,
# MAGIC       MIN(block_timestamp_utc) AS min_block_timestamp_utc,
# MAGIC       MAX(block_timestamp_utc) AS max_block_timestamp_utc,
# MAGIC       COUNT(DISTINCT block_height) AS count_block_height,
# MAGIC       collect_set(block_height) AS block_heights,
# MAGIC       COUNT(DISTINCT author_account_id) AS count_author_account_id,
# MAGIC       collect_set(author_account_id) AS author_account_ids,
# MAGIC       MIN(total_supply) AS min_total_supply,
# MAGIC       MAX(total_supply) AS max_total_supply
# MAGIC   FROM (mainnet.silver_blocks)
# MAGIC   GROUP BY 1

# COMMAND ----------

# Ensure the table exists
spark.sql("""
    CREATE TABLE IF NOT EXISTS mainnet.silver_epochs 
    PARTITIONED BY (max_block_date)
    LOCATION '/pipelines/mainnet_silver_stream/tables/scheduled/silver_epochs_v1'
    COMMENT 'Silver Epochs. 1 epoch has a duration of 43,200 blocks aproximately 12 hours' AS
    SELECT *
    FROM temp_view_silver_epochs
    WHERE 1=2
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Since we already backfilled this table, we can process only the more recent records 
# MAGIC MERGE INTO mainnet.silver_epochs as t
# MAGIC USING (SELECT * FROM temp_view_silver_epochs 
# MAGIC   WHERE max_block_date = (current_date() - INTERVAL '1' DAY)
# MAGIC ) s
# MAGIC ON s.epoch_id = t.epoch_id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE mainnet.silver_epochs ZORDER BY (epoch_max_block_height);
# MAGIC VACUUM mainnet.silver_epochs;

# COMMAND ----------

# MAGIC %md
# MAGIC # Epochs Validators

# COMMAND ----------

async def get_validators(epoch_id):
    transport = httpx.AsyncHTTPTransport(retries=1)
    client = httpx.AsyncClient(transport=transport)

    r = await client.post(
        url = READ_RPC_URL,
        timeout = 100,
        json = {
            "jsonrpc": "2.0",
            "id": "dontcare",
            "method": "validators",
            "params": {
                "epoch_id": epoch_id,
            }
        }
    )

    print(epoch_id)

    resp_json = json.loads(r.text)
    await client.aclose()
    
    if "result" in resp_json:
        return resp_json["result"]["current_validators"]
    
    return {}

# Wrapper function to handle the asynchronous call
def get_validators_wrapper(epoch_id):
    return asyncio.run(get_validators(epoch_id))

udf_schema_validators = ArrayType(
    StructType([
        StructField("account_id", StringType(), True),
        StructField("public_key", StringType(), True),
        StructField("is_slashed", BooleanType(), True),
        StructField("stake", StringType(), True),
        StructField("num_produced_blocks", IntegerType(), True),
        StructField("num_expected_blocks", IntegerType(), True),
        StructField("num_produced_chunks", IntegerType(), True),
        StructField("num_expected_chunks", IntegerType(), True)
    ])
)

# Register the function as a UDF
get_read_rpc_validators_udf = spark.udf.register("get_read_rpc_validators", get_validators_wrapper, udf_schema_validators)

# COMMAND ----------

# MAGIC %sql
# MAGIC    -- A temp view will make it easier to reuse the SQL in the CREATE TABLE and MERGE operations
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_view_silver_epochs_validators AS 
# MAGIC WITH cte_validators AS (
# MAGIC   SELECT 
# MAGIC     epoch_id,
# MAGIC     epoch_min_block_height,
# MAGIC     epoch_max_block_height,
# MAGIC     min_block_date,
# MAGIC     max_block_date,
# MAGIC     min_block_timestamp_utc,
# MAGIC     max_block_timestamp_utc,
# MAGIC     explode(get_read_rpc_validators(epoch_id)) AS validator 
# MAGIC   FROM mainnet.silver_epochs 
# MAGIC ),
# MAGIC
# MAGIC cte_validators_flat AS (
# MAGIC   SELECT * EXCEPT (validator), validator.* FROM cte_validators
# MAGIC )
# MAGIC
# MAGIC SELECT * EXCEPT (account_id), account_id AS validator_id FROM cte_validators_flat

# COMMAND ----------

# Ensure the table exists
spark.sql("""
    CREATE TABLE IF NOT EXISTS mainnet.silver_epochs_validators
    PARTITIONED BY (max_block_date)
    LOCATION '/pipelines/mainnet_silver_stream/tables/scheduled/silver_epochs_validators_v2'
    COMMENT 'Silver Epochs Validators' AS
    SELECT *
    FROM temp_view_silver_epochs_validators
    WHERE 1=2
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Since we already backfilled this table, we can process only the more recent records  
# MAGIC MERGE INTO mainnet.silver_epochs_validators as t
# MAGIC USING (SELECT * FROM temp_view_silver_epochs_validators 
# MAGIC WHERE max_block_date = (current_date() - INTERVAL '1' DAY)) s
# MAGIC ON s.epoch_id = t.epoch_id AND s.validator_id = t.validator_id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE mainnet.silver_epochs_validators ZORDER BY (epoch_max_block_height);
# MAGIC VACUUM mainnet.silver_epochs_validators;

# COMMAND ----------

# MAGIC %md
# MAGIC # Epochs Validator Delegators

# COMMAND ----------

async def fetch_data(client, account_id, block_height, method_name, additional_args):
    args = {"account_id": account_id, **additional_args}
    args64 = base64.b64encode(json.dumps(args).encode("utf-8")).decode("utf-8")

    try:
        for rpc_url in ALL_RPC_SERVERS_AND_HISTORICAL_DATA:
            response = await client.post(
                url = rpc_url,
                timeout=httpx.Timeout(30.0, read=None),
                json={
                    "jsonrpc": "2.0",
                    "id": "dontcare",
                    "method": "query",
                    "params": {
                        "request_type": "call_function",
                        "block_id": block_height,
                        "account_id": account_id,
                        "method_name": method_name,
                        "args_base64": args64,
                    }
                }
            )
            if response.status_code != 200 or "result" not in response.text:
                continue
            else:
                return json.loads(response.text)
    except Exception as e:
         return {"error:" f"ERROR: on acct {account_id}, block {block_height}, exception {e}"}
        
    return {"error:" f"ERROR: on acct {account_id}, block {block_height}, response {response.text}"}
    

async def rpc_call_function_get_accounts(account_id, block_height, limit):
    accounts_list = []
    async with httpx.AsyncClient(http2=False, transport=httpx.AsyncHTTPTransport(retries=1)) as client:
        response = await fetch_data(client, account_id, block_height, "get_number_of_accounts", {})
        if "result" in response:
            number_of_accounts = json.loads(bytearray(response["result"]["result"]).decode("utf-8"))
            print("number_of_accounts", number_of_accounts)
        else:
            number_of_accounts = 0

        from_index = 0
        while from_index < number_of_accounts:
            response = await fetch_data(client, account_id, block_height, "get_accounts", {"from_index": from_index, "limit": limit})
            if "result" in response:
                accounts_list += json.loads(bytearray(response["result"]["result"]).decode("utf-8"))
            if "error" in response:
                print(response)
            
            from_index += limit

    return accounts_list

# Wrapper function to handle the asynchronous call
def rpc_call_function_get_accounts_wrapper(account_id, block_height, limit=500):
    return asyncio.run(rpc_call_function_get_accounts(account_id, block_height, limit))

udf_schema_call_function_get_accounts = ArrayType(
    StructType([
        StructField("account_id", StringType(), True),
        StructField("unstaked_balance", StringType(), True),
        StructField("staked_balance", StringType(), True),
        StructField("can_withdraw", BooleanType(), True),
    ])
)

# Register the function as a UDF
rpc_call_function_get_accounts_udf = spark.udf.register("rpc_call_function_get_accounts", rpc_call_function_get_accounts_wrapper, udf_schema_call_function_get_accounts)

# COMMAND ----------

# MAGIC %sql
# MAGIC    -- A temp view will make it easier to reuse the SQL in the CREATE TABLE and MERGE operations
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_view_silver_epochs_validator_delegators AS 
# MAGIC WITH cte_delegators AS (
# MAGIC   SELECT 
# MAGIC     epoch_id,
# MAGIC     validator_id,
# MAGIC     epoch_min_block_height,
# MAGIC     epoch_max_block_height,
# MAGIC     min_block_date,
# MAGIC     max_block_date,
# MAGIC     min_block_timestamp_utc,
# MAGIC     max_block_timestamp_utc,
# MAGIC     0 AS reward,
# MAGIC     explode(rpc_call_function_get_accounts(validator_id, epoch_max_block_height, 500)) AS delegator
# MAGIC   FROM mainnet.silver_epochs_validators
# MAGIC   WHERE validator_id LIKE '%pool.near' OR validator_id LIKE '%poolv1.near'
# MAGIC ),
# MAGIC
# MAGIC cte_delegators_flat AS (
# MAGIC   SELECT * EXCEPT (delegator), delegator.* FROM cte_delegators
# MAGIC )
# MAGIC
# MAGIC SELECT * EXCEPT (account_id), account_id AS delegator_id FROM cte_delegators_flat

# COMMAND ----------

# Ensure the table exists
spark.sql("""
    CREATE TABLE IF NOT EXISTS mainnet.silver_epochs_validator_delegators
    PARTITIONED BY (max_block_date)
    LOCATION '/pipelines/mainnet_silver_stream/tables/scheduled/silver_epochs_validator_delegators'
    COMMENT 'Silver Epochs Validator Delegators' AS
    SELECT *
    FROM temp_view_silver_epochs_validator_delegators
    WHERE 1=2
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Since we already backfilled this table, we can process only the more recent records  
# MAGIC MERGE INTO mainnet.silver_epochs_validator_delegators as t
# MAGIC USING (SELECT * FROM temp_view_silver_epochs_validator_delegators 
# MAGIC WHERE max_block_date = (current_date() - INTERVAL '1' DAY)
# MAGIC ) s
# MAGIC ON s.epoch_id = t.epoch_id AND s.validator_id = t.validator_id AND s.delegator_id = t.delegator_id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %md
# MAGIC # Epochs Validator Delegators Rewards

# COMMAND ----------

# MAGIC %sql
# MAGIC    -- A temp view will make it easier to reuse the SQL in the CREATE TABLE and MERGE operations
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_view_delegators_rewards AS 
# MAGIC WITH rewards AS (
# MAGIC   SELECT 
# MAGIC     validator_id,
# MAGIC     delegator_id,
# MAGIC     epoch_id,
# MAGIC     max_block_date,
# MAGIC     coalesce(staked_balance::DOUBLE - lag(staked_balance::DOUBLE) OVER (PARTITION BY validator_id, delegator_id ORDER BY epoch_max_block_height), 0) reward
# MAGIC   FROM mainnet.silver_epochs_validator_delegators
# MAGIC   ORDER BY 1
# MAGIC )
# MAGIC SELECT 
# MAGIC   validator_id,
# MAGIC   delegator_id,
# MAGIC   epoch_id,
# MAGIC   max_block_date,
# MAGIC   if(reward > 0, reward, 0) reward
# MAGIC FROM rewards

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Now update the reward column 
# MAGIC MERGE INTO mainnet.silver_epochs_validator_delegators as t
# MAGIC USING (SELECT * FROM temp_view_delegators_rewards 
# MAGIC WHERE max_block_date = (current_date() - INTERVAL '1' DAY)) s
# MAGIC ON s.epoch_id = t.epoch_id AND s.validator_id = t.validator_id AND s.delegator_id = t.delegator_id
# MAGIC WHEN MATCHED THEN UPDATE SET t.reward = s.reward

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE mainnet.silver_epochs_validator_delegators ZORDER BY (epoch_max_block_height);
# MAGIC VACUUM mainnet.silver_epochs_validator_delegators;

# COMMAND ----------

# MAGIC %sql
# MAGIC    -- A temp view will make it easier to reuse the SQL in the CREATE TABLE and MERGE operations
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_view_delegators_rewards_daily AS 
# MAGIC WITH latest_row_per_date AS (
# MAGIC   SELECT 
# MAGIC     * EXCEPT (rn) 
# MAGIC   FROM (
# MAGIC     SELECT
# MAGIC       *,
# MAGIC       ROW_NUMBER() OVER (PARTITION BY validator_id, delegator_id, max_block_date ORDER BY epoch_max_block_height DESC) rn
# MAGIC     FROM mainnet.silver_epochs_validator_delegators
# MAGIC   ) WHERE rn = 1
# MAGIC )
# MAGIC
# MAGIC SELECT 
# MAGIC   max_block_date AS epoch_date,
# MAGIC   epoch_id,
# MAGIC   epoch_max_block_height AS epoch_block_height,
# MAGIC   validator_id, 
# MAGIC   delegator_id,
# MAGIC   round(unstaked_balance::double / pow(10,24), 2) AS unstaked, 
# MAGIC   round(staked_balance::double / pow(10,24), 2) AS staked,
# MAGIC   round(reward::double / pow(10,24), 2) AS reward
# MAGIC FROM latest_row_per_date

# COMMAND ----------

# Ensure the table exists
spark.sql("""
    CREATE TABLE IF NOT EXISTS mainnet.silver_daily_delegators_rewards
    PARTITIONED BY (epoch_date)
    LOCATION '/pipelines/mainnet_silver_stream/tables/scheduled/silver_daily_delegators_rewards'
    COMMENT 'Silver Daily Epochs Validator Delegators Rewards' AS
    SELECT *
    FROM temp_view_delegators_rewards_daily
    WHERE 1=2
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO mainnet.silver_daily_delegators_rewards as t
# MAGIC USING (SELECT * FROM temp_view_delegators_rewards_daily 
# MAGIC WHERE epoch_date = (current_date() - INTERVAL '1' DAY)) s
# MAGIC ON s.epoch_id = t.epoch_id AND s.validator_id = t.validator_id AND s.delegator_id = t.delegator_id
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE mainnet.silver_daily_delegators_rewards;
# MAGIC VACUUM mainnet.silver_daily_delegators_rewards;

# COMMAND ----------

# MAGIC %md
# MAGIC # Lockup

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_view_lockups AS  
# MAGIC
# MAGIC WITH lockups_unrolled AS (
# MAGIC   SELECT
# MAGIC     *,
# MAGIC     CASE WHEN action_kind = 'FUNCTION_CALL' THEN from_json(args, 'gas BIGINT, deposit STRING, args_json STRING, args_base64 STRING, method_name STRING')['method_name'] END AS method_name,
# MAGIC     CASE WHEN action_kind = 'FUNCTION_CALL' AND index_in_action_receipt = 3 THEN from_json(unbase64(from_json(args, 'gas BIGINT, deposit STRING, args_base64 STRING, method_name STRING')['args_base64'])::string, 'owner_account_id STRING, lockup_duration STRING, transfers_information STRING, vesting_schedule STRING, release_duration STRING, staking_pool_whitelist_account_id STRING, foundation_account_id STRING') END AS lockup_metadata,
# MAGIC     CASE WHEN action_kind = 'FUNCTION_CALL' AND index_in_action_receipt = 0 THEN from_json(unbase64(from_json(args, 'gas BIGINT, deposit STRING, args_base64 STRING, method_name STRING')['args_base64'])::string, 'lockup_account_id STRING, attached_deposit STRING, predecessor_account_id STRING') END AS funding_metadata
# MAGIC   FROM mainnet.silver_action_receipt_actions
# MAGIC   WHERE receipt_predecessor_account_id = 'lockup.near'
# MAGIC ), 
# MAGIC
# MAGIC lockups AS (
# MAGIC   SELECT 
# MAGIC     block_date AS lockup_create_date,
# MAGIC     block_timestamp_utc AS lockup_create_timestamp_utc,
# MAGIC     receipt_id AS account_created_by_receipt_id,
# MAGIC     receipt_receiver_account_id AS lockup_account_id
# MAGIC   FROM lockups_unrolled
# MAGIC   WHERE action_kind = 'CREATE_ACCOUNT'
# MAGIC ),
# MAGIC
# MAGIC lockup_metadata AS (
# MAGIC   SELECT
# MAGIC     receipt_receiver_account_id AS lockup_account_id,
# MAGIC     lockup_metadata['lockup_duration']::bigint / pow(10,9) AS lockup_duration,
# MAGIC     lockup_metadata['owner_account_id'] AS owner_account_id,
# MAGIC     lockup_metadata['release_duration'] AS release_duration,
# MAGIC     lockup_metadata['vesting_schedule'] AS vesting_schedule,
# MAGIC     lockup_metadata['foundation_account_id'] AS foundation_account_id,
# MAGIC     lockup_metadata['staking_pool_whitelist_account_id'] AS staking_pool_whitelist_account_id,
# MAGIC     cast(from_json(from_json(lockup_metadata['transfers_information'], 'TransfersEnabled STRING')['TransfersEnabled'], 'transfers_timestamp STRING')['transfers_timestamp']::bigint/pow(10,9) AS timestamp) AS transfer_start,
# MAGIC     cast((from_json(from_json(lockup_metadata['transfers_information'], 'TransfersEnabled STRING')['TransfersEnabled'], 'transfers_timestamp STRING')['transfers_timestamp']::bigint + lockup_metadata['release_duration']::bigint)/pow(10,9) AS timestamp) AS transfer_end
# MAGIC   FROM lockups_unrolled
# MAGIC   WHERE method_name = 'new'
# MAGIC ),
# MAGIC
# MAGIC funding_metadata AS (
# MAGIC   SELECT
# MAGIC     receipt_id AS funded_by_receipt_id,
# MAGIC     block_timestamp_utc AS funded_date,
# MAGIC     funding_metadata['lockup_account_id'] AS lockup_account_id,
# MAGIC     funding_metadata['predecessor_account_id'] AS funding_account,
# MAGIC     funding_metadata['attached_deposit']::float / pow(10, 24) AS attached_deposit
# MAGIC   FROM lockups_unrolled
# MAGIC   WHERE method_name = 'on_lockup_create'
# MAGIC ),
# MAGIC
# MAGIC transfers AS (
# MAGIC   SELECT
# MAGIC     receipt_receiver_account_id AS lockup_account_id,
# MAGIC     block_timestamp_utc AS transfer_date,
# MAGIC     from_json(args, 'deposit STRING')['deposit']::float / pow(10,24) AS transfer_amount
# MAGIC   FROM lockups_unrolled 
# MAGIC   WHERE action_kind = 'TRANSFER'
# MAGIC )
# MAGIC
# MAGIC SELECT 
# MAGIC 	l.*,
# MAGIC 	lm.lockup_duration,
# MAGIC 	lm.owner_account_id,
# MAGIC 	lm.release_duration,
# MAGIC 	lm.vesting_schedule,
# MAGIC 	lm.foundation_account_id,
# MAGIC 	lm.staking_pool_whitelist_account_id,
# MAGIC 	lm.transfer_start,
# MAGIC 	lm.transfer_end,
# MAGIC 	fm.funded_by_receipt_id,
# MAGIC 	fm.funded_date,
# MAGIC 	fm.funding_account,
# MAGIC 	fm.attached_deposit,
# MAGIC 	t.transfer_date,
# MAGIC 	t.transfer_amount
# MAGIC FROM lockups l
# MAGIC LEFT JOIN lockup_metadata lm ON l.lockup_account_id = lm.lockup_account_id 
# MAGIC LEFT JOIN funding_metadata fm ON l.lockup_account_id = fm.lockup_account_id 
# MAGIC LEFT JOIN transfers t ON l.lockup_account_id = t.lockup_account_id
# MAGIC ORDER BY l.lockup_create_timestamp_utc

# COMMAND ----------

# Ensure the table exists
spark.sql("""
    CREATE TABLE IF NOT EXISTS mainnet.silver_lockups
    PARTITIONED BY (lockup_create_date)
    LOCATION '/pipelines/mainnet_silver_stream/tables/scheduled/silver_lockups_v2'
    COMMENT 'Silver Lockups' AS
    SELECT *
    FROM temp_view_lockups
    WHERE 1=2
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO mainnet.silver_lockups as t
# MAGIC USING (SELECT * FROM temp_view_lockups 
# MAGIC   WHERE lockup_create_timestamp_utc = (current_date() - INTERVAL '1' DAY) OR transfer_date = (current_date() - INTERVAL '1' DAY)
# MAGIC ) s
# MAGIC ON s.lockup_account_id = t.lockup_account_id 
# MAGIC     AND s.owner_account_id = t.owner_account_id 
# MAGIC     AND s.lockup_create_timestamp_utc = t.lockup_create_timestamp_utc
# MAGIC     AND s.transfer_date = t.transfer_date
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE mainnet.silver_lockups;
# MAGIC VACUUM mainnet.silver_lockups;

# COMMAND ----------

# MAGIC %md
# MAGIC # Accounts with min balances to track

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_view_accounts_with_min_balances AS 
# MAGIC SELECT 
# MAGIC   ac.affected_account_id,
# MAGIC   l.lockup_account_id,
# MAGIC   min(ac.block_date) AS first_account_change_date
# MAGIC FROM mainnet.silver_account_changes ac
# MAGIC LEFT JOIN mainnet.silver_lockups l ON l.owner_account_id = ac.affected_account_id 
# MAGIC WHERE 
# MAGIC   (ac.affected_account_nonstaked_balance >= 10000 * pow(10, 24) -- minimum 10,000 NEAR Tokens
# MAGIC     AND NOT ac.affected_account_id LIKE '%.lockup.near')
# MAGIC   OR -- OR is in the NF list
# MAGIC   (ac.affected_account_id IN (SELECT account FROM nf_finance.nf_accounts))
# MAGIC GROUP BY 1, 2

# COMMAND ----------

# Ensure the table exists
spark.sql("""
    CREATE TABLE IF NOT EXISTS mainnet.silver_accounts_with_min_balances
    PARTITIONED BY (first_account_change_date)
    LOCATION '/pipelines/mainnet_silver_stream/tables/scheduled/silver_accounts_with_min_balances_v7'
    COMMENT 'Silver Accounts with min balances' AS
    SELECT *
    FROM temp_view_accounts_with_min_balances
    WHERE 1=2
""")

# COMMAND ----------

# MAGIC %sql
# MAGIC MERGE INTO mainnet.silver_accounts_with_min_balances as t
# MAGIC USING (SELECT * FROM temp_view_accounts_with_min_balances) s
# MAGIC ON s.affected_account_id = t.affected_account_id 
# MAGIC WHEN MATCHED THEN UPDATE SET *
# MAGIC WHEN NOT MATCHED THEN INSERT *

# COMMAND ----------

# MAGIC %sql
# MAGIC OPTIMIZE mainnet.silver_accounts_with_min_balances;
# MAGIC VACUUM mainnet.silver_accounts_with_min_balances;

# COMMAND ----------

# MAGIC %md
# MAGIC # Accounts daily FT balances

# COMMAND ----------

df_epochs = spark.sql("""
    SELECT 
        e.max_block_date, 
        max(e.epoch_max_block_height) AS epoch_max_block_height 
    FROM mainnet.silver_epochs e 
    WHERE max_block_date = (current_date() - INTERVAL '1' DAY)
    GROUP BY 1
    ORDER BY 1 DESC
    """)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW temp_view_accounts_daily_ft_balances AS 
# MAGIC SELECT 
# MAGIC     * EXCEPT(rn)
# MAGIC FROM (
# MAGIC     SELECT DISTINCT
# MAGIC         '2000-01-01' AS epoch_date,
# MAGIC         '0' AS epoch_block_height,
# MAGIC         ac.affected_account_id AS account_id ,
# MAGIC         round(ac.affected_account_nonstaked_balance::double / pow(10,24), 2) AS liquid,
# MAGIC         ac.affected_account_storage_usage AS storage_usage,
# MAGIC         COALESCE(dr.unstaked, 0) AS unstaked_not_liquid,
# MAGIC         COALESCE(dr.staked, 0) AS staked,
# MAGIC         COALESCE(dr.reward, 0) AS reward,
# MAGIC         amb.lockup_account_id,
# MAGIC         0 AS lockup_liquid,
# MAGIC         COALESCE(drl.unstaked, 0) AS lockup_unstaked_not_liquid,
# MAGIC         COALESCE(drl.staked, 0) AS lockup_staked,
# MAGIC         COALESCE(drl.reward, 0) AS lockup_reward,
# MAGIC         l.lockup_create_timestamp_utc,
# MAGIC         l.lockup_create_timestamp_utc AS lockup_end_timestamp_utc,
# MAGIC         0 AS lockup_release_duration_in_seconds,
# MAGIC         0 AS lockup_time_left_in_seconds,
# MAGIC         0 AS lockup_balance_unlocked,
# MAGIC         0 AS lockup_balance_locked,
# MAGIC         ROW_NUMBER() OVER (PARTITION BY ac.affected_account_id ORDER BY ac.block_height DESC) AS rn
# MAGIC     FROM mainnet.silver_accounts_with_min_balances amb
# MAGIC     JOIN mainnet.silver_account_changes ac ON ac.affected_account_id = amb.affected_account_id AND ac.block_date <='2000-01-01'
# MAGIC     LEFT JOIN mainnet.silver_daily_delegators_rewards dr ON dr.delegator_id = amb.affected_account_id AND dr.epoch_date = '2000-01-01'
# MAGIC     LEFT JOIN mainnet.silver_lockups l ON l.owner_account_id = amb.affected_account_id 
# MAGIC     LEFT JOIN mainnet.silver_daily_delegators_rewards drl ON dr.delegator_id = l.lockup_account_id AND dr.epoch_date = '2000-01-01'
# MAGIC     WHERE amb.first_account_change_date <= '2000-01-01'
# MAGIC ) accounts
# MAGIC WHERE rn = 1

# COMMAND ----------

# Ensure the table exists
spark.sql("""
    CREATE TABLE IF NOT EXISTS mainnet.silver_accounts_daily_ft_balances
    PARTITIONED BY (epoch_date)
    LOCATION '/pipelines/mainnet_silver_stream/tables/scheduled/silver_accounts_daily_ft_balances_v5'
    COMMENT 'Silver Accounts FT balances' AS
    SELECT *
    FROM temp_view_accounts_daily_ft_balances
    WHERE 1=2
""")

# COMMAND ----------

for epoch in df_epochs.collect():
    print(epoch["max_block_date"])
    df_accounts = spark.sql(f"""
        MERGE INTO mainnet.silver_accounts_daily_ft_balances as t
        USING (
            SELECT 
                * EXCEPT(rn)
            FROM (
                SELECT DISTINCT
                    '{epoch["max_block_date"]}' AS epoch_date,
                    '{epoch["epoch_max_block_height"]}' AS epoch_block_height,
                    ac.affected_account_id AS account_id ,
                    round(ac.affected_account_nonstaked_balance::double / pow(10,24), 2) AS liquid,
                    ac.affected_account_storage_usage AS storage_usage,
                    COALESCE(dr.unstaked, 0) AS unstaked_not_liquid,
                    COALESCE(dr.staked, 0) AS staked,
                    COALESCE(dr.reward, 0) AS reward,
                    amb.lockup_account_id,
                    0 AS lockup_liquid,
                    COALESCE(drl.unstaked, 0) AS lockup_unstaked_not_liquid,
                    COALESCE(drl.staked, 0) AS lockup_staked,
                    COALESCE(drl.reward, 0) AS lockup_reward,
                    l.lockup_create_timestamp_utc,
                    timestampadd(SECOND, l.release_duration/pow(10, 9), l.lockup_create_timestamp_utc) AS lockup_end_timestamp_utc,
                    l.release_duration / pow(10, 9) AS lockup_release_duration_in_seconds,
                    (l.release_duration / pow(10, 9)) - (unix_timestamp(to_timestamp('{epoch["max_block_date"]}')) - unix_timestamp(l.lockup_create_timestamp_utc)) AS lockup_time_left_in_seconds,
                    round(((unix_timestamp(to_timestamp('{epoch["max_block_date"]}')) - unix_timestamp(l.lockup_create_timestamp_utc)) / (l.release_duration / pow(10, 9))) * (drl.unstaked + drl.staked + drl.reward),2) AS lockup_balance_unlocked,
                    round((1 - (unix_timestamp(to_timestamp('{epoch["max_block_date"]}')) - unix_timestamp(l.lockup_create_timestamp_utc)) / (l.release_duration / pow(10, 9))) * (drl.unstaked + drl.staked + drl.reward), 2) AS lockup_balance_locked,
                    ROW_NUMBER() OVER (PARTITION BY ac.affected_account_id ORDER BY ac.block_height DESC) AS rn
                FROM mainnet.silver_accounts_with_min_balances amb
                JOIN mainnet.silver_account_changes ac ON ac.affected_account_id = amb.affected_account_id AND ac.block_date <= '{epoch["max_block_date"]}'
                LEFT JOIN mainnet.silver_daily_delegators_rewards dr ON dr.delegator_id = amb.affected_account_id AND dr.epoch_date = '{epoch["max_block_date"]}'
                LEFT JOIN mainnet.silver_lockups l ON l.lockup_account_id = amb.lockup_account_id
                LEFT JOIN mainnet.silver_daily_delegators_rewards drl ON drl.delegator_id = amb.lockup_account_id AND drl.epoch_date = '{epoch["max_block_date"]}'
                WHERE amb.first_account_change_date <= '{epoch["max_block_date"]}'
            ) accounts
            WHERE rn = 1) s
        ON s.epoch_date = t.epoch_date AND s.account_id = t.account_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)



# COMMAND ----------

for epoch in df_epochs.collect():
    print(epoch["max_block_date"])
    df_accounts = spark.sql(f"""
        MERGE INTO mainnet.silver_accounts_daily_ft_balances as t
        USING (
            SELECT 
                * EXCEPT(rn)
            FROM (
                SELECT DISTINCT
                    '{epoch["max_block_date"]}' AS epoch_date,
                    '{epoch["epoch_max_block_height"]}' AS epoch_block_height,
                    amb.lockup_account_id,
                    round(acl.affected_account_nonstaked_balance::double / pow(10,24), 2) AS lockup_liquid,
                    ROW_NUMBER() OVER (PARTITION BY acl.affected_account_id ORDER BY acl.block_height DESC) AS rn
                FROM mainnet.silver_accounts_with_min_balances amb
                LEFT JOIN mainnet.silver_account_changes acl ON acl.affected_account_id = amb.lockup_account_id AND acl.block_date <= '{epoch["max_block_date"]}'
                WHERE amb.first_account_change_date <= '{epoch["max_block_date"]}'
            ) accounts
            WHERE rn = 1) s
        ON s.epoch_date = t.epoch_date AND s.lockup_account_id = t.lockup_account_id
        WHEN MATCHED THEN UPDATE SET t.lockup_liquid = s.lockup_liquid
    """)

# COMMAND ----------

# MAGIC %sql
# MAGIC optimize mainnet.silver_accounts_daily_ft_balances;
# MAGIC vacuum mainnet.silver_accounts_daily_ft_balances;

# COMMAND ----------


