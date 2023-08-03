# near-public-lakehouse

NEAR Public Lakehouse

This repository contains the source code for ingesting NEAR Protocol data stored as JSON files in AWS S3 by [near-lake-indexer](https://github.com/near/near-lake-indexer). The data is loaded in a streaming fashion using Databricks Autoloader into raw/bronze tables, transformed with Databricks Delta Live Tables streaming jobs into cleaned/enriched/silver tables.

The silver tables are also copied into the GCP BigQuery Public Dataset.

# Architecture
![Architecture](./docs/Architecture.png "Architecture")

# What is NEAR Protocol?
NEAR is a user-friendly, carbon-neutral blockchain, built from the ground up to be performant, secure, and infinitely scalable. It's a layer one, sharded, proof-of-stake blockchain designed with usability in mind. In simple terms, NEAR is blockchain for everyone.



