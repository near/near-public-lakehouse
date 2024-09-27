# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Lake Mainnet - Pipeline Prod FT and NFT events
# MAGIC
# MAGIC DLT allows SQL and Python but not Scala, so we are going to use Python for the UDF needs on DLT
# MAGIC
# MAGIC In this pipeline we also need to use WATERMARK to be able to use window function

# COMMAND ----------

import dlt
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType

# COMMAND ----------

def calculate_event_index(block_timestamp, shard_id, event_type_index, row_number):
    result = (block_timestamp * 100000000 * 100000000) + \
             (shard_id * 100000000) + \
             (event_type_index * 1000000) + \
             row_number
    return str(result)

udf_calculate_event_index = udf(calculate_event_index, StringType())
spark.udf.register("udf_calculate_event_index", udf_calculate_event_index)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Execution Outcomes FT Event Logs

# COMMAND ----------

@dlt.table(
  name="silver_execution_outcome_ft_event_logs",
  comment="Stream of parsed execution outcome fungible tokens event logs",
  table_properties={"quality" : "silver", "delta.autoOptimize.optimizeWrite" : "true", "delta.autoOptimize.autoCompact" : "true"},
  partition_cols=["block_date"],
  temporary=False)
def silver_execution_outcome_ft_event_logs():
    return spark.sql("""
        WITH near_log_events AS (
            SELECT
                *,
                substring(log,12) as event
            FROM STREAM(live.silver_execution_outcome_logs) WATERMARK block_timestamp_utc DELAY OF INTERVAL 30 SECONDS AS eo
            WHERE startswith(log, 'EVENT_JSON')
            AND status <> 'FAILURE'
        ), near_ev AS (
            SELECT 
                le.*, 
                le.event:event AS cause, 
                le.event:`version` AS `version`, 
                le.event:standard, 
                1 AS event_type_index,
                'near' AS token_id,
                posexplode(from_json(le.event:data, "ARRAY<STRUCT<amount: STRING, memo: STRING, owner_id: STRING, new_owner_id: STRING, old_owner_id: STRING>>")) AS (event_data_index,  data)
            FROM near_log_events le
            WHERE event:standard = "nep141"
        ), near_parsed_event AS (
            SELECT 
                near_ev.*, 
                cause, 
                inline(filter(array(
                    struct(CASE WHEN cause IN ('ft_mint', 'ft_transfer') THEN 1 ELSE 0 END keep, coalesce(near_ev.data.new_owner_id, near_ev.data.owner_id) x, near_ev.data.old_owner_id y, CAST(near_ev.data.amount AS DECIMAL(38,0)))
                    , struct(CASE WHEN cause='ft_burn' THEN 1 ELSE 0 END keep    , near_ev.data.owner_id x    , NULL y                , -CAST(near_ev.data.amount AS DECIMAL(38,0)))
                    , struct(CASE WHEN cause='ft_transfer' THEN 1 ELSE 0 END keep, near_ev.data.old_owner_id x, near_ev.data.new_owner_id y, -CAST(near_ev.data.amount AS DECIMAL(38,0)))
                ), s -> s.keep = 1)) AS (keep, affected_account_id, involved_account_id, delta_amount), 
                near_ev.data.memo, 
                (ROW_NUMBER() OVER (PARTITION BY near_ev.contract_id, near_ev.standard, near_ev.block_height ORDER BY near_ev.block_timestamp) -1) as rn
            FROM near_ev
        ), near_parsed_event_with_index AS (
            SELECT
                *,
                udf_calculate_event_index(pe.block_timestamp, pe.shard_id, pe.event_type_index, pe.rn) AS event_index
            FROM near_parsed_event pe
        ), near_tokens AS (
            SELECT 
                block_date,
                block_height,
                block_timestamp,
                block_timestamp_utc,
                block_hash,
                chunk_hash,
                shard_id,
                NOW() AS _dlt_synced_utc,
                BIGINT(NOW() - block_timestamp_utc) as _dlt_synced_lag_seconds,
                pei.standard, 
                pei.receipt_id, 
                pei.contract_id AS contract_account_id, 
                pei.affected_account_id, 
                involved_account_id, 
                pei.delta_amount, 
                pei.cause, 
                pei.status, 
                pei.memo AS event_memo,
                event_index,
                'near' as token_id
            FROM near_parsed_event_with_index pei
        )

        SELECT * FROM near_tokens
    """)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Execution Outcomes NFT Event Logs

# COMMAND ----------

@dlt.table(
  name="silver_execution_outcome_nft_event_logs",
  comment="Stream of parsed execution outcome NFT event logs",
  table_properties={"quality" : "silver", "delta.autoOptimize.optimizeWrite" : "true", "delta.autoOptimize.autoCompact" : "true"},
  partition_cols=["block_date"],
  temporary=False)
def silver_execution_outcome_nft_event_logs():
    return spark.sql("""
        WITH log_events AS (
            SELECT
                *,
                substring(log,12) as event
            FROM STREAM(live.silver_execution_outcome_logs) WATERMARK block_timestamp_utc DELAY OF INTERVAL 30 SECONDS AS eo
            WHERE startswith(log, 'EVENT_JSON')
            AND status <> 'FAILURE'
        ), ev_data AS (
            SELECT 
                le.*, 
                le.event:event AS cause, 
                le.event:`version` AS `version`, 
                le.event:standard, 
                2 AS event_type_index,
                posexplode(from_json(le.event:data, "ARRAY<STRUCT<token_ids: ARRAY<STRING>, memo: STRING, owner_id: STRING, new_owner_id: STRING, old_owner_id: STRING, authorized_id: STRING>>")) AS (event_data_index,  data)
            FROM log_events le
            WHERE event:standard = "nep171"
        ), ev_tokens AS (
            SELECT 
                ev_data.*, 
                posexplode(ev_data.data.token_ids) AS (token_index,  token_id)
            FROM ev_data
        ), ev_rn AS (
            SELECT 
                ev_tokens.*, 
                (ROW_NUMBER() OVER (PARTITION BY ev_tokens.contract_id, ev_tokens.standard, ev_tokens.block_height ORDER BY ev_tokens.block_timestamp) -1) as rn
            FROM ev_tokens 
        ), ev_with_index AS (
            SELECT
                *,
                udf_calculate_event_index(ev_rn.block_timestamp, ev_rn.shard_id, ev_rn.event_type_index, ev_rn.rn) AS event_index
            FROM
                ev_rn
        )

        SELECT 
            block_date,
            block_height,
            block_timestamp,
            block_timestamp_utc,
            block_hash,
            chunk_hash,
            shard_id,
            NOW() AS _dlt_synced_utc,
            BIGINT(NOW() - block_timestamp_utc) as _dlt_synced_lag_seconds,
            ev_with_index.standard, 
            ev_with_index.receipt_id, 
            ev_with_index.contract_id AS contract_account_id, 
            ev_with_index.token_id,
            ev_with_index.data.old_owner_id as old_owner_account_id, 
            COALESCE(ev_with_index.data.new_owner_id, ev_with_index.data.owner_id) as new_owner_account_id, 
            ev_with_index.data.authorized_id as authorized_account_id,
            ev_with_index.cause, 
            ev_with_index.status, 
            ev_with_index.data.memo AS event_memo,
            ev_with_index.event_index,
            ev_with_index.log
        FROM ev_with_index
    """)

# COMMAND ----------


