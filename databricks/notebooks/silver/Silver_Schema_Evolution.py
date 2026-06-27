# Databricks notebook source
import json

# COMMAND ----------
# 1. WIDGET DEFINITIONS
# COMMAND ----------
dbutils.widgets.text("source_bronze_table", "")
dbutils.widgets.text("target_silver_table", "")
dbutils.widgets.text("storage_account", "")
dbutils.widgets.text("storage_access_key", "")

source_bronze_table = dbutils.widgets.get("source_bronze_table")
target_silver_table = dbutils.widgets.get("target_silver_table")
storage_account = dbutils.widgets.get("storage_account")
storage_access_key = dbutils.widgets.get("storage_access_key")

# COMMAND ----------
# 2. STORAGE AUTHENTICATION
# COMMAND ----------
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    storage_access_key
)

bronze_path = f"abfss://datalake@{storage_account}.dfs.core.windows.net/bronze/{source_bronze_table}/"
silver_path = f"abfss://datalake@{storage_account}.dfs.core.windows.net/silver/{target_silver_table}/"

# COMMAND ----------
# 3. DETECT SCHEMA DRIFT
# COMMAND ----------
try:
    df_bronze = spark.read.format("delta").load(bronze_path)
    df_silver = spark.read.format("delta").load(silver_path)
    
    bronze_cols = set([f.name.lower() for f in df_bronze.schema.fields])
    silver_cols = set([f.name.lower() for f in df_silver.schema.fields])
    
    # Ignore metadata columns in comparison
    metadata_cols = {"_cdc_operation", "_cdc_version", "_ingest_timestamp", "_pipeline_run_id", "_load_timestamp", "_is_deleted", "_is_current", "_valid_from", "_valid_to", "_hash_key"}
    
    new_columns = (bronze_cols - silver_cols) - metadata_cols
    
    if new_columns:
        print(f"Schema drift detected. New columns in Bronze: {new_columns}")
        # Note: Intentional schema evolution policy dictates we do NOT automatically mergeSchema.
        # This script alerts on drift. A Data Engineer must explicitly add the column to the Silver transformation rules.
        dbutils.notebook.exit(json.dumps({"schema_drift_detected": True, "new_columns": list(new_columns)}))
    else:
        dbutils.notebook.exit(json.dumps({"schema_drift_detected": False}))
        
except Exception as e:
    dbutils.notebook.exit(json.dumps({"error": str(e)}))