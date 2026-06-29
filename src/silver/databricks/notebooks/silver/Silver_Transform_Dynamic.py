# Databricks notebook source
# =================================================================================
# SILVER LAYER TRANSFORMATION PIPELINE
# =================================================================================
# CRITICAL AUTH POLICY: Authentication is handled entirely by Unity Catalog via 
# Managed Identity (Access Connector). NO fs.azure.* keys or secrets are set here.
# The cluster simply reads/writes abfss:// paths authorized by External Locations.
# =================================================================================

import json
from pyspark.sql.functions import (
    col, expr, current_timestamp, lit, row_number, when, array, array_remove, 
    md5, concat_ws, coalesce
)
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# 1. Define and get widgets
dbutils.widgets.text("pipeline_run_id", "")
dbutils.widgets.text("silver_table_id", "")
dbutils.widgets.text("source_bronze_path", "")
dbutils.widgets.text("target_silver_path", "")
dbutils.widgets.text("table_name", "")
dbutils.widgets.text("primary_key_columns", "")
dbutils.widgets.text("scd_type", "1")
dbutils.widgets.text("partition_columns", "")
dbutils.widgets.text("z_order_columns", "")
dbutils.widgets.text("transformation_rules_json", "[]")
dbutils.widgets.text("storage_account", "")

pipeline_run_id = dbutils.widgets.get("pipeline_run_id")
source_bronze_path = dbutils.widgets.get("source_bronze_path")
target_silver_path = dbutils.widgets.get("target_silver_path")
table_name = dbutils.widgets.get("table_name")
primary_key_columns = [k.strip() for k in dbutils.widgets.get("primary_key_columns").split(",")]
scd_type = int(dbutils.widgets.get("scd_type"))
partition_columns = [p.strip() for p in dbutils.widgets.get("partition_columns").split(",") if p.strip()]
z_order_columns = [z.strip() for z in dbutils.widgets.get("z_order_columns").split(",") if z.strip()]
transformation_rules = json.loads(dbutils.widgets.get("transformation_rules_json"))
storage_account = dbutils.widgets.get("storage_account")

# Construct full ABFSS paths
bronze_uri = f"abfss://bronze@{storage_account}.dfs.core.windows.net/{source_bronze_path.replace('bronze/', '', 1)}"
silver_uri = f"abfss://silver@{storage_account}.dfs.core.windows.net/{target_silver_path.replace('silver/', '', 1)}"
quarantine_uri = f"abfss://silver@{storage_account}.dfs.core.windows.net/quarantine/{table_name}"

print(f"Reading from: {bronze_uri}")
print(f"Writing to: {silver_uri}")

# 2. Read Bronze Data
df_bronze = spark.read.format("delta").load(bronze_uri)
records_read = df_bronze.count()

if records_read == 0:
    dbutils.notebook.exit(json.dumps({
        "records_read": 0, "records_written": 0, "records_filtered": 0, "records_quarantined": 0
    }))

# 3. Deduplication (Keep latest by event time / CDC version)
# Approved Decision: source_primary_key + keep_latest_by_event_time
order_col = "_cdc_version" if "_cdc_version" in df_bronze.columns else "LAST_UPDATE_DATE"
if order_col not in df_bronze.columns:
    order_col = "CREATED_DATE" # Fallback

window_spec = Window.partitionBy(*[col(c) for c in primary_key_columns]).orderBy(col(order_col).desc())
df_dedup = df_bronze.withColumn("_rn", row_number().over(window_spec)).filter(col("_rn") == 1).drop("_rn")

# 4. Apply Dynamic Transformations
df_transformed = df_dedup
for rule in transformation_rules:
    r_type = rule.get("rule_type")
    s_col = rule.get("source_column")
    t_col = rule.get("target_column")
    expr_str = rule.get("transformation_expression")
    
    if r_type == "CAST":
        df_transformed = df_transformed.withColumn(t_col, col(s_col).cast(expr_str))
    elif r_type == "TRANSFORM":
        df_transformed = df_transformed.withColumn(t_col, expr(expr_str))
    elif r_type == "RENAME":
        df_transformed = df_transformed.withColumnRenamed(s_col, t_col)
    elif r_type == "FILTER":
        df_transformed = df_transformed.filter(expr(expr_str))

# Ensure all columns are snake_case (standardization)
for c in df_transformed.columns:
    if c.isupper():
        df_transformed = df_transformed.withColumnRenamed(c, c.lower())

# 5. Apply Data Quality Rules (Approved Project Decisions)
# Rule 1: TOTAL_AMOUNT > 0 (ERROR -> Quarantine)
# Rule 2: CUSTOMER_ID IS NOT NULL (ERROR -> Quarantine)
# Rule 3: STATUS IS NOT NULL (WARN -> Tag and keep)

df_dq = df_transformed.withColumn("_dq_warnings", array())
df_dq = df_dq.withColumn("_dq_failed", lit(False))
df_dq = df_dq.withColumn("_dq_failure_reason", lit(""))

# Apply Warn Rule
if "status" in df_dq.columns:
    df_dq = df_dq.withColumn(
        "_dq_warnings",
        when(col("status").isNull(), expr("array_append(_dq_warnings, 'STATUS IS NULL')")).otherwise(col("_dq_warnings"))
    )

# Apply Error Rules
if "total_amount" in df_dq.columns:
    df_dq = df_dq.withColumn(
        "_dq_failed",
        when((col("total_amount") <= 0) | col("total_amount").isNull(), lit(True)).otherwise(col("_dq_failed"))
    ).withColumn(
        "_dq_failure_reason",
        when((col("total_amount") <= 0) | col("total_amount").isNull(), concat_ws(";", col("_dq_failure_reason"), lit("TOTAL_AMOUNT <= 0"))).otherwise(col("_dq_failure_reason"))
    )

if "customer_id" in df_dq.columns:
    df_dq = df_dq.withColumn(
        "_dq_failed",
        when(col("customer_id").isNull(), lit(True)).otherwise(col("_dq_failed"))
    ).withColumn(
        "_dq_failure_reason",
        when(col("customer_id").isNull(), concat_ws(";", col("_dq_failure_reason"), lit("CUSTOMER_ID IS NULL"))).otherwise(col("_dq_failure_reason"))
    )

# Split into valid and quarantine DataFrames
df_quarantine = df_dq.filter(col("_dq_failed") == True)
df_valid = df_dq.filter(col("_dq_failed") == False).drop("_dq_failed", "_dq_failure_reason")

records_quarantined = df_quarantine.count()
records_filtered = records_read - df_dedup.count()

# Write Quarantined records
if records_quarantined > 0:
    df_quarantine.withColumn("_quarantine_timestamp", current_timestamp()) \
                 .withColumn("_pipeline_run_id", lit(pipeline_run_id)) \
                 .write.format("delta").mode("append").save(quarantine_uri)

# 6. Add Silver Metadata Columns
df_final = df_valid.withColumn("_pipeline_run_id", lit(pipeline_run_id)) \
                   .withColumn("_load_timestamp", current_timestamp()) \
                   .withColumn("_is_deleted", when(col("_cdc_operation") == 'D', lit(True)).otherwise(lit(False)))

# 7. MERGE into Silver Delta Table
if not DeltaTable.isDeltaTable(spark, silver_uri):
    # Initial Load
    print("Target table does not exist. Performing initial load.")
    if scd_type == 2:
        df_final = df_final.withColumn("_is_current", lit(True)) \
                           .withColumn("_valid_from", current_timestamp()) \
                           .withColumn("_valid_to", lit(None).cast("timestamp"))
    
    writer = df_final.write.format("delta").mode("overwrite")
    if partition_columns:
        writer = writer.partitionBy(*partition_columns)
    writer.save(silver_uri)
    records_written = df_final.count()
else:
    # Incremental MERGE
    print("Target table exists. Performing MERGE.")
    target_table = DeltaTable.forPath(spark, silver_uri)
    
    # Build match condition
    match_cond = " AND ".join([f"target.{c} = source.{c}" for c in primary_key_columns])
    
    if scd_type == 1:
        # SCD Type 1 (Overwrite)
        target_table.alias("target").merge(
            df_final.alias("source"),
            match_cond
        ).whenMatchedUpdateAll(
        ).whenNotMatchedInsertAll(
        ).execute()
        
    elif scd_type == 2:
        # SCD Type 2 (History Tracking)
        # Tracked columns for customers: status, customer_type, marketing_opt_in
        tracked_cols = ["status", "customer_type", "marketing_opt_in"]
        
        # Generate hash for change detection
        df_final = df_final.withColumn("_hash_key", md5(concat_ws("||", *[coalesce(col(c).cast("string"), lit("")) for c in tracked_cols])))
        
        # Identify records that need to be closed (Updates where hash changed)
        staged_updates = df_final.alias("updates").join(
            target_table.toDF().alias("target"),
            expr(f"{match_cond} AND target._is_current = true AND target._hash_key != updates._hash_key")
        ).selectExpr("updates.*", "true as _merge_update")
        
        # Identify new inserts (New records + New versions of updated records)
        staged_inserts = df_final.withColumn("_merge_update", lit(False))
        
        # Union them for the MERGE source
        staged_data = staged_updates.unionByName(staged_inserts)
        
        # Execute SCD2 MERGE
        target_table.alias("target").merge(
            staged_data.alias("source"),
            f"{match_cond} AND target._is_current = true AND source._merge_update = true"
        ).whenMatchedUpdate(
            set={
                "_is_current": lit(False),
                "_valid_to": "source._load_timestamp",
                "_last_modified_date": current_timestamp()
            }
        ).whenNotMatchedInsert(
            values={
                **{c: f"source.{c}" for c in df_final.columns},
                "_is_current": lit(True),
                "_valid_from": "source._load_timestamp",
                "_valid_to": lit(None).cast("timestamp")
            }
        ).execute()
        
    records_written = df_final.count()

# 8. Optimize and Z-Order
if z_order_columns:
    print(f"Optimizing table with Z-Order on: {', '.join(z_order_columns)}")
    spark.sql(f"OPTIMIZE delta.`{silver_uri}` ZORDER BY ({', '.join(z_order_columns)})")

# 9. Return Metrics
result = {
    "records_read": records_read,
    "records_written": records_written,
    "records_filtered": records_filtered,
    "records_quarantined": records_quarantined
}
dbutils.notebook.exit(json.dumps(result))