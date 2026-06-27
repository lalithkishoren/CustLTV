# Databricks notebook source
import json
from pyspark.sql.functions import col, expr, current_timestamp, lit, md5, concat_ws, row_number, when
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# COMMAND ----------
# 1. WIDGET DEFINITIONS & PARAMETER EXTRACTION
# COMMAND ----------
dbutils.widgets.text("pipeline_run_id", "")
dbutils.widgets.text("silver_table_id", "")
dbutils.widgets.text("source_bronze_table", "")
dbutils.widgets.text("target_silver_table", "")
dbutils.widgets.text("primary_key_columns", "")
dbutils.widgets.text("scd_type", "1")
dbutils.widgets.text("track_history_columns", "")
dbutils.widgets.text("partition_columns", "")
dbutils.widgets.text("z_order_columns", "")
dbutils.widgets.text("transformation_rules_json", "[]")
dbutils.widgets.text("storage_account", "")
dbutils.widgets.text("storage_access_key", "")

pipeline_run_id = dbutils.widgets.get("pipeline_run_id")
silver_table_id = dbutils.widgets.get("silver_table_id")
source_bronze_table = dbutils.widgets.get("source_bronze_table")
target_silver_table = dbutils.widgets.get("target_silver_table")
primary_key_columns = dbutils.widgets.get("primary_key_columns").split(",")
scd_type = int(dbutils.widgets.get("scd_type"))
track_history_columns = dbutils.widgets.get("track_history_columns").split(",") if dbutils.widgets.get("track_history_columns") else []
partition_columns = dbutils.widgets.get("partition_columns").split(",") if dbutils.widgets.get("partition_columns") else []
z_order_columns = dbutils.widgets.get("z_order_columns").split(",") if dbutils.widgets.get("z_order_columns") else []
transformation_rules = json.loads(dbutils.widgets.get("transformation_rules_json"))
storage_account = dbutils.widgets.get("storage_account")
storage_access_key = dbutils.widgets.get("storage_access_key")

# COMMAND ----------
# 2. STORAGE AUTHENTICATION (CRITICAL)
# COMMAND ----------
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    storage_access_key
)

bronze_path = f"abfss://datalake@{storage_account}.dfs.core.windows.net/bronze/{source_bronze_table}/"
silver_path = f"abfss://datalake@{storage_account}.dfs.core.windows.net/silver/{target_silver_table}/"
quarantine_path = f"abfss://datalake@{storage_account}.dfs.core.windows.net/silver/quarantine/{target_silver_table}/"

# COMMAND ----------
# 3. READ BRONZE DATA & DEDUPLICATE (Keep Latest by Event Time)
# COMMAND ----------
df_bronze = spark.read.format("delta").load(bronze_path)
records_read = df_bronze.count()

# Deduplication logic: Keep latest record per primary key based on _cdc_version or _ingest_timestamp
if "_cdc_version" in df_bronze.columns:
    order_col = col("_cdc_version").desc()
elif "LAST_UPDATE_DATE" in df_bronze.columns:
    order_col = col("LAST_UPDATE_DATE").desc()
else:
    order_col = col("_ingest_timestamp").desc()

window_spec = Window.partitionBy(*[col(c) for c in primary_key_columns]).orderBy(order_col)
df_deduped = df_bronze.withColumn("rn", row_number().over(window_spec)).filter(col("rn") == 1).drop("rn")

# COMMAND ----------
# 4. APPLY DYNAMIC TRANSFORMATIONS
# COMMAND ----------
df_transformed = df_deduped

for rule in transformation_rules:
    rule_type = rule.get("rule_type")
    source_col = rule.get("source_column")
    target_col = rule.get("target_column")
    expression = rule.get("transformation_expression")
    
    if rule_type == 'FILTER':
        df_transformed = df_transformed.filter(expr(expression))
    elif rule_type == 'TRANSFORM':
        df_transformed = df_transformed.withColumn(target_col, expr(expression))
    elif rule_type == 'RENAME':
        df_transformed = df_transformed.withColumnRenamed(source_col, target_col)
    elif rule_type == 'CAST':
        df_transformed = df_transformed.withColumn(target_col, col(source_col).cast(expression))
    elif rule_type == 'DEDUPE':
        # Handled globally above, but can be overridden here if specific keys are provided
        pass

# Add standard metadata columns
df_transformed = df_transformed.withColumn("_pipeline_run_id", lit(pipeline_run_id)) \
                               .withColumn("_load_timestamp", current_timestamp()) \
                               .withColumn("_is_deleted", when(col("_cdc_operation") == 'D', lit(True)).otherwise(lit(False)))

# COMMAND ----------
# 5. APPLY DATA QUALITY RULES (Quarantine Bad Rows)
# COMMAND ----------
# Hardcoded enterprise rules based on LLD and Approved Decisions
dq_error_condition = None

if target_silver_table == 'oe_order_headers_all':
    dq_error_condition = expr("total_amount <= 0 OR total_amount IS NULL OR order_id IS NULL OR customer_id IS NULL")
elif target_silver_table == 'customers':
    dq_error_condition = expr("customer_id IS NULL")
    # Warning rule: STATUS IS NOT NULL (Tag only, do not drop)
    df_transformed = df_transformed.withColumn("_dq_warning", when(col("status").isNull(), lit("STATUS_IS_NULL")).otherwise(lit(None)))
else:
    # Generic PK null check
    pk_null_expr = " OR ".join([f"{c} IS NULL" for c in primary_key_columns])
    dq_error_condition = expr(pk_null_expr)

# Split into valid and quarantined DataFrames
df_quarantine = df_transformed.filter(dq_error_condition).withColumn("_dq_failure_reason", lit("Failed Enterprise DQ Rule"))
df_valid = df_transformed.filter(~dq_error_condition)

records_quarantined = df_quarantine.count()
records_filtered = records_read - df_valid.count() - records_quarantined

# Write quarantined rows to DLQ path
if records_quarantined > 0:
    df_quarantine.write.format("delta").mode("append").save(quarantine_path)

# COMMAND ----------
# 6. MERGE INTO SILVER (SCD Type 1 or Type 2)
# COMMAND ----------
records_written = 0

if DeltaTable.isDeltaTable(spark, silver_path):
    target_table = DeltaTable.forPath(spark, silver_path)
    
    # Build match condition
    match_cond = " AND ".join([f"target.{c} = source.{c}" for c in primary_key_columns])
    
    if scd_type == 1:
        # SCD Type 1: Overwrite
        target_table.alias("target").merge(
            df_valid.alias("source"),
            match_cond
        ).whenMatchedUpdateAll(
        ).whenNotMatchedInsertAll(
        ).execute()
        
    elif scd_type == 2:
        # SCD Type 2: History Tracking
        # Generate Hash for tracked columns
        df_valid = df_valid.withColumn("_hash_key", md5(concat_ws("||", *[col(c) for c in track_history_columns])))
        
        # Identify updates (Hash differs)
        staged_updates = df_valid.alias("updates") \
            .join(target_table.toDF().alias("target"), [col(f"updates.{c}") == col(f"target.{c}") for c in primary_key_columns]) \
            .filter("target._is_current = true AND updates._hash_key != target._hash_key") \
            .selectExpr("updates.*") \
            .withColumn("_merge_key", lit(None)) # Force insert for new version
            
        # Combine with original valid data
        df_staged = df_valid.withColumn("_merge_key", concat_ws("-", *[col(c) for c in primary_key_columns])) \
            .unionByName(staged_updates)
            
        # Execute MERGE
        target_table.alias("target").merge(
            df_staged.alias("source"),
            f"concat_ws('-', target.{primary_key_columns[0]}) = source._merge_key"
        ).whenMatchedUpdate(
            condition="target._is_current = true AND target._hash_key != source._hash_key",
            set={
                "_is_current": lit(False),
                "_valid_to": "source._load_timestamp",
                "_last_modified_date": "source._load_timestamp"
            }
        ).whenNotMatchedInsert(
            values={
                **{c: f"source.{c}" for c in df_valid.columns if c not in ["_merge_key"]},
                "_is_current": lit(True),
                "_valid_from": "source._load_timestamp",
                "_valid_to": lit(None).cast("timestamp")
            }
        ).execute()
        
    records_written = df_valid.count()
else:
    # Initial Load
    if scd_type == 2:
        df_valid = df_valid.withColumn("_hash_key", md5(concat_ws("||", *[col(c) for c in track_history_columns]))) \
                           .withColumn("_is_current", lit(True)) \
                           .withColumn("_valid_from", col("_load_timestamp")) \
                           .withColumn("_valid_to", lit(None).cast("timestamp"))
                           
    writer = df_valid.write.format("delta").mode("overwrite")
    if partition_columns:
        writer = writer.partitionBy(*partition_columns)
    writer.save(silver_path)
    records_written = df_valid.count()

# COMMAND ----------
# 7. POST-PROCESSING (OPTIMIZE & Z-ORDER)
# COMMAND ----------
if z_order_columns:
    z_order_str = ", ".join(z_order_columns)
    spark.sql(f"OPTIMIZE delta.`{silver_path}` ZORDER BY ({z_order_str})")
else:
    spark.sql(f"OPTIMIZE delta.`{silver_path}`")

# COMMAND ----------
# 8. RETURN METRICS TO ADF
# COMMAND ----------
result = {
    "records_read": records_read,
    "records_written": records_written,
    "records_filtered": records_filtered,
    "records_quarantined": records_quarantined
}
dbutils.notebook.exit(json.dumps(result))