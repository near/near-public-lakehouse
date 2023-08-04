# Databricks notebook source
# MAGIC %md
# MAGIC # BQ Writer Stream

# COMMAND ----------

def bq_writer_stream(table_name, start_date):
    df = spark.readStream \
    .format("delta") \
    .table(f"hive_metastore.mainnet.silver_{table_name}") \
    .where(f"block_date > '{start_date}'")

    df.writeStream \
        .format("bigquery") \
        .option("temporaryGcsBucket", "databricks-bq-buffer-near-lakehouse") \
        .option("table", f"pagoda-data-platform.crypto_near_mainnet.{table_name}") \
        .option("createDisposition", "CREATE_IF_NEEDED") \
        .option("partitionField", "block_date") \
        .option("partitionType", "DAY") \
        .option("allowFieldAddition", "true") \
        .option("allowFieldRelaxation", "true") \
        .option("writeMethod", "indirect") \
        .option("checkpointLocation", f"/pipelines/checkpoints/bq_writer_stream_{table_name}") \
        .start()

# COMMAND ----------

bq_writer_stream("blocks", "2023-08-01")

# COMMAND ----------

bq_writer_stream("transactions", "2023-08-01")

# COMMAND ----------

bq_writer_stream("receipts", "2023-08-01")

# COMMAND ----------

bq_writer_stream("account_changes", "2023-08-01")

# COMMAND ----------

bq_writer_stream("chunks", "2023-08-01")

# COMMAND ----------


