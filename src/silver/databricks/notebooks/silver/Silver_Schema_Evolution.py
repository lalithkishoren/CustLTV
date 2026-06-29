# Databricks notebook source
# =================================================================================
# SILVER LAYER SCHEMA EVOLUTION DETECTION
# =================================================================================
# CRITICAL AUTH POLICY: Authentication is handled entirely by Unity Catalog via 
# Managed Identity (Access Connector). NO fs.azure.* keys or secrets are set here.
# =================================================================================
# Note: Schema evolution is INTENTIONAL. This script detects drift and alerts,
# it does NOT blindly apply mergeSchema.
# =================================================================================

import json
from delta.tables import DeltaTable

dbutils.widgets.text("source_bronze_path", "")
dbutils.widgets.text("target_silver_path", "")
dbutils.widgets.text("storage_account", "")

source_bronze_path = dbutils.widgets.get("source_bronze_path")
target_silver_path = dbutils.widgets.get("target_silver_path")
storage_account = dbutils.widgets.get("storage_account")

bronze_uri = f"abfss://bronze@{storage_account}.dfs.core.windows.net/{source_bronze_path.replace('bronze/', '', 1)}"
silver_uri = f"abfss://silver@{storage_account}.dfs.core.windows.net/{target_silver_path.replace('silver/', '', 1)}"

if not DeltaTable.isDeltaTable(spark, silver_uri):
    dbutils.notebook.exit(json.dumps({"status": "skipped", "reason": "Silver table does not exist yet"}))

df_bronze = spark.read.format("delta").load(bronze_uri)
df_silver = spark.read.format("delta").load(silver_uri)

bronze_cols = set([c.lower() for c in df_bronze.columns])
silver_cols = set([c.lower() for c in df_silver.columns])

# Ignore metadata columns in Silver
metadata_cols = {"_pipeline_run_id", "_load_timestamp", "_is_deleted", "_is_current", "_valid_from", "_valid_to", "_hash_key", "_dq_warnings"}
silver_business_cols = silver_cols - metadata_cols

new_columns_in_bronze = list(bronze_cols - silver_business_cols)
missing_columns_in_bronze = list(silver_business_cols - bronze_cols)

drift_detected = len(new_columns_in_bronze) > 0 or len(missing_columns_in_bronze) > 0

result = {
    "drift_detected": drift_detected,
    "new_columns_in_source": new_columns_in_bronze,
    "missing_columns_in_source": missing_columns_in_bronze
}

print(json.dumps(result, indent=2))

if drift_detected:
    print("WARNING: Schema drift detected. Manual review required before altering Silver schema.")
    # In a strict production environment, we might raise an exception here to fail the pipeline
    # raise Exception(f"Schema drift detected: {result}")

dbutils.notebook.exit(json.dumps(result))