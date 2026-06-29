# Databricks notebook source
# =================================================================================
# SILVER LAYER TRANSFORMATION NOTEBOOK
# =================================================================================
# (auth handled by Unity Catalog — see note above)
# NEVER storage account keys, NEVER OAuth client secrets, NEVER plaintext passwords/tokens
# Databricks -> ADLS is authorized by Unity Catalog (the Databricks Access Connector 
# backs a UC Storage Credential + External Locations).
# =================================================================================

import json
from pyspark.sql.functions import col, expr, current_timestamp, lit, md5, concat_ws, row_number
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# 1. Define Widgets
dbutils.widgets.text("pipeline_run_id", "")
dbutils.widgets.text("silver_table_id", "")
dbutils.widgets.text("source_system", "")
dbutils.widgets.text("schema_name", "")
dbutils.widgets.text("source_bronze_table", "")
dbutils.widgets.text("target_silver_table", "")
dbutils.widgets.text("primary_key_columns", "")
dbutils.widgets.text("scd_type", "1")
dbutils.widgets.text("track_history_columns", "")
dbutils.widgets.text("partition_columns", "")
dbutils.widgets.text("z_order_columns", "")
dbutils.widgets.text("transformation_rules_json", "[]")
dbutils.widgets.text("storage_account", "")

# 2. Get Widget Values
pipeline_run_id = dbutils.widgets.get("pipeline_run_id")
source_system = dbutils.widgets.get("source_system")
schema_name = dbutils.widgets.get("schema_name")
source_bronze_table = dbutils.widgets.get("source_bronze_table")
target_silver_table = dbutils.widgets.get("target_silver_table")
primary_key_columns = dbutils.widgets.get("primary_key_columns").split(",")
scd_type = int(dbutils.widgets.get("scd_type"))
track_history_columns = dbutils.widgets.get("track_history_columns").split(",") if dbutils.widgets.get("track_history_columns") else []
partition_columns = dbutils.widgets.get("partition_columns").split(",") if dbutils.widgets.get("partition_columns") else []
z_order_columns = dbutils.widgets.get("z_order_columns").split(",") if dbutils.widgets.get("z_order_columns") else []
transformation_rules = json.loads(dbutils.widgets.get("transformation_rules_json"))
storage_account = dbutils.widgets.get("storage_account")

# 3. Define Paths
bronze_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/{source_system}/{schema_name}/{source_bronze_table}/"
silver_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/{source_system}/{schema_name}/{target_silver_table}/"

print(f"Reading from Bronze: {bronze_path}")
print(f"Writing to Silver: {silver_path}")

# 4. Read Bronze Data
try:
    df_bronze = spark.read.format("delta").load(bronze_path)
    records_read = df_bronze.count()
except Exception as e:
    print(f"Error reading Bronze path: {str(e)}")
    raise e

if records_read == 0:
    print("No records found in Bronze. Exiting gracefully.")
    dbutils.notebook.exit(json.dumps({"records_read": 0, "records_written": 0}))

# 5. Deduplicate (Keep latest by event time / ingest timestamp)
# Assuming Bronze has _ingest_timestamp or similar. If not, fallback to CDC version or just drop duplicates.
if "_ingest_timestamp" in df_bronze.columns:
    window_spec = Window.partitionBy(*[col(c) for c in primary_key_columns]).orderBy(col("_ingest_timestamp").desc())
    df_dedup = df_bronze.withColumn("rn", row_number().over(window_spec)).filter(col("rn") == 1).drop("rn")
else:
    df_dedup = df_bronze.dropDuplicates(primary_key_columns)

# 6. Apply Transformations
df_transformed = df_dedup
for rule in transformation_rules:
    rule_type = rule.get("rule_type")
    src_col = rule.get("source_column")
    tgt_col = rule.get("target_column")
    expr_str = rule.get("transformation_expression")
    
    if rule_type == "CAST":
        df_transformed = df_transformed.withColumn(tgt_col, col(src_col).cast(expr_str))
    elif rule_type == "EXPRESSION":
        df_transformed = df_transformed.withColumn(tgt_col, expr(expr_str))
    elif rule_type == "RENAME":
        df_transformed = df_transformed.withColumnRenamed(src_col, tgt_col)

# 7. Add Standard Metadata Columns
df_transformed = df_transformed.withColumn("_pipeline_run_id", lit(pipeline_run_id)) \
                               .withColumn("_load_timestamp", current_timestamp()) \
                               .withColumn("_is_deleted", expr("CASE WHEN _cdc_operation = 'D' THEN true ELSE false END") if "_cdc_operation" in df_transformed.columns else lit(False))

# 8. Write to Silver (Idempotent MERGE)
if not DeltaTable.isDeltaTable(spark, silver_path):
    print("Target table does not exist. Performing initial load.")
    
    if scd_type == 2:
        df_transformed = df_transformed.withColumn("_valid_from", current_timestamp()) \
                                       .withColumn("_valid_to", lit(None).cast("timestamp")) \
                                       .withColumn("_is_current", lit(True)) \
                                       .withColumn("_hash_key", md5(concat_ws("||", *[col(c) for c in track_history_columns])))
    
    writer = df_transformed.write.format("delta").mode("overwrite")
    if partition_columns and partition_columns[0] != "":
        writer = writer.partitionBy(*partition_columns)
    writer.save(silver_path)
    records_written = df_transformed.count()
    
else:
    print("Target table exists. Performing MERGE.")
    target_table = DeltaTable.forPath(spark, silver_path)
    
    # Build match condition
    match_cond = " AND ".join([f"target.{c} = source.{c}" for c in primary_key_columns])
    
    if scd_type == 1:
        # SCD Type 1: Overwrite
        update_dict = {c: f"source.{c}" for c in df_transformed.columns}
        
        target_table.alias("target").merge(
            df_transformed.alias("source"),
            match_cond
        ).whenMatchedUpdate(
            set=update_dict
        ).whenNotMatchedInsert(
            values=update_dict
        ).execute()
        
    elif scd_type == 2:
        # SCD Type 2: History Tracking
        df_transformed = df_transformed.withColumn("_hash_key", md5(concat_ws("||", *[col(c) for c in track_history_columns])))
        
        # Identify records that need to be closed (Hash changed)
        staged_updates = df_transformed.alias("source").join(
            target_table.toDF().alias("target"),
            expr(f"{match_cond} AND target._is_current = true AND target._hash_key != source._hash_key")
        ).selectExpr("source.*", "true as _merge_update")
        
        # Identify new records or unchanged records
        staged_inserts = df_transformed.withColumn("_merge_update", lit(False))
        
        # Union them for the merge
        staged_data = staged_updates.unionByName(staged_inserts)
        
        insert_dict = {c: f"source.{c}" for c in df_transformed.columns}
        insert_dict["_valid_from"] = "current_timestamp()"
        insert_dict["_valid_to"] = "CAST(NULL AS TIMESTAMP)"
        insert_dict["_is_current"] = "true"
        
        target_table.alias("target").merge(
            staged_data.alias("source"),
            f"{match_cond} AND target._is_current = true"
        ).whenMatchedUpdate(
            condition="source._merge_update = true",
            set={
                "_valid_to": "current_timestamp()",
                "_is_current": "false",
                "_pipeline_run_id": "source._pipeline_run_id"
            }
        ).whenNotMatchedInsert(
            values=insert_dict
        ).execute()

    # Get metrics from Delta history
    history = target_table.history(1).collect()[0]
    metrics = history.operationMetrics
    records_written = int(metrics.get("numTargetRowsInserted", 0)) + int(metrics.get("numTargetRowsUpdated", 0))

# 9. Optimize and Z-Order
if z_order_columns and z_order_columns[0] != "":
    print(f"Optimizing and Z-Ordering by: {z_order_columns}")
    spark.sql(f"OPTIMIZE delta.`{silver_path}` ZORDER BY ({','.join(z_order_columns)})")

# 10. Return Metrics
result = {
    "records_read": records_read,
    "records_written": records_written
}
dbutils.notebook.exit(json.dumps(result))