# Databricks notebook source
# MAGIC %md
# MAGIC # BQ Writer receipt_origin_transaction

# COMMAND ----------

def bq_reader(sql, temp_location):
    return spark.read.format("bigquery") \
        .option("materializationDataset", temp_location) \
        .option("query", sql) \
        .load() \
        .collect()

# COMMAND ----------

def bq_writer(table_name, df):
    return df.write \
        .format("bigquery") \
        .option("temporaryGcsBucket", 'databricks-bq-buffer-near-lakehouse') \
        .option("table", f"pagoda-data-platform.crypto_near_mainnet.{table_name}") \
        .option("createDisposition", "CREATE_IF_NEEDED") \
        .option("partitionField", "block_date") \
        .option("partitionType", "DAY") \
        .option("allowFieldAddition", "true") \
        .option("allowFieldRelaxation", "true") \
        .option("writeMethod", "indirect") \
        .mode("append").save()  

# COMMAND ----------

df_bq_last_update = bq_reader(""" 
    select max(_record_last_updated_utc) as last_update 
    from `pagoda-data-platform.crypto_near_mainnet.receipt_origin_transaction`
    """, "crypto_near_mainnet")

last_date_bq = df_bq_last_update[0]["last_update"]
print(last_date_bq)

# COMMAND ----------

df_receipt_origin_transaction = spark.sql(f"""
        select 
            block_date, 
            block_height, 
            receipt_id, 
            data_id, 
            receipt_kind, 
            originated_from_transaction_hash,
            _record_last_updated_utc 
        from mainnet.silver_receipt_originated_from_transaction
        where originated_from_transaction_hash IS NOT NULL AND originated_from_transaction_hash != ''
        and _record_last_updated_utc > '{last_date_bq}'
    """)

# COMMAND ----------

bq_writer("receipt_origin_transaction", df_receipt_origin_transaction)

# COMMAND ----------


