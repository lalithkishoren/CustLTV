# ===============================================================================
# Incremental CDC Processing (Parquet Staging -> Delta MERGE)
# ===============================================================================

from pyspark.sql.functions import current_timestamp, lit, col, row_number
from pyspark.sql.window import Window
from delta.tables import DeltaTable
import json

# 1. Define Widgets
dbutils.widgets.text("pipeline_run_id", "")
dbutils.widgets.text("table_id", "")
dbutils.widgets.text("source_system", "")
dbutils.widgets.text("schema_name", "")
dbutils.widgets.text("table_name", "")
dbutils.widgets.text("primary_key_columns", "")
dbutils.widgets.text("last_sync_version", "")
dbutils.widgets.text("storage_account", "")
dbutils.widgets.text("sql_server", "")
dbutils.widgets.text("sql_database", "")
dbutils.widgets.text("sql_username", "")
dbutils.widgets.text("sql_password", "")

# 2. Get Widget Values
pipeline_run_id = dbutils.widgets.get("pipeline_run_id")
table_id = dbutils.widgets.get("table_id")
source_system = dbutils.widgets.get("source_system")
schema_name = dbutils.widgets.get("schema_name")
table_name = dbutils.widgets.get("table_name")
primary_key_columns = dbutils.widgets.get("primary_key_columns")
last_sync_version = dbutils.widgets.get("last_sync_version")
storage_account = dbutils.widgets.get("storage_account")
sql_server = dbutils.widgets.get("sql_server")
sql_database = dbutils.widgets.get("sql_database")
sql_username = dbutils.widgets.get("sql_username")
sql_password = dbutils.widgets.get("sql_password")

# ADLS auth: handled by Unity Catalog (NO account key, NO fs.azure.* configs)

# 3. Define Paths
staging_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/staging/{source_system}/{schema_name}/{table_name}/incremental/{pipeline_run_id}/"
delta_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/{source_system}/{schema_name}/{table_name}/"

print(f"Reading CDC from staging: {staging_path}")
print(f"Merging into Delta: {delta_path}")

# 4. Read Staging Parquet
source_df = spark.read.format("parquet").load(staging_path)
records_processed = source_df.count()
print(f"Records read from staging: {records_processed}")

if records_processed == 0:
    dbutils.notebook.exit(json.dumps({"status": "SUCCESS", "records_processed": 0}))

# Extract new sync version
current_sync_version = str(source_df.agg({"_current_sync_version": "max"}).collect()[0][0])

# 5. Deduplicate Source Data (Keep latest change per key)
pk_cols = [pk.strip() for pk in primary_key_columns.split(',')]
window_spec = Window.partitionBy(*pk_cols).orderBy(col("SYS_CHANGE_VERSION").desc())

source_deduped = source_df.withColumn("row_num", row_number().over(window_spec)) \
                          .filter(col("row_num") == 1) \
                          .drop("row_num")

# Add Audit/Metadata Columns
source_deduped = source_deduped \
    .withColumn("_pipeline_run_id", lit(pipeline_run_id)) \
    .withColumn("_ingest_timestamp", current_timestamp()) \
    .withColumn("_source_system", lit(source_system)) \
    .withColumnRenamed("SYS_CHANGE_OPERATION", "_cdc_operation")

# 6. Build Merge Condition
merge_condition = " AND ".join([f"target.{pk} = source.{pk}" for pk in pk_cols])
print(f"Merge Condition: {merge_condition}")

# 7. Execute MERGE
target_table = DeltaTable.forPath(spark, delta_path)

# Get columns for update/insert (exclude PKs and system columns from update)
all_columns = source_deduped.columns
update_columns = [c for c in all_columns if c not in pk_cols and c not in ['SYS_CHANGE_VERSION', '_current_sync_version']]

target_table.alias("target").merge(
    source_deduped.alias("source"),
    merge_condition
).whenMatchedDelete(
    condition="source._cdc_operation = 'D'"
).whenMatchedUpdate(
    condition="source._cdc_operation IN ('U', 'I')",
    set={column: f"source.{column}" for column in update_columns}
).whenNotMatchedInsert(
    condition="source._cdc_operation != 'D'",
    values={column: f"source.{column}" for column in all_columns}
).execute()

print("MERGE executed successfully.")

# 8. OPTIMIZE Z-ORDER
print(f"Optimizing Delta table: {delta_path}")
spark.sql(f"""
    OPTIMIZE delta.`{delta_path}`
    ZORDER BY ({primary_key_columns}, _ingest_timestamp)
""")

# 9. Update SQL Control Table via JDBC
jdbc_url = f"jdbc:sqlserver://{sql_server}:1433;database={sql_database};encrypt=true;trustServerCertificate=false;hostNameInCertificate=*.database.windows.net;loginTimeout=30;"
connection_properties = {
    "user": sql_username,
    "password": sql_password,
    "driver": "com.microsoft.sqlserver.jdbc.SQLServerDriver"
}

update_query = f"""
    EXEC control.sp_UpdateTableMetadata 
        @TableId = {table_id}, 
        @Status = 'SUCCESS', 
        @PipelineRunId = '{pipeline_run_id}', 
        @RecordsLoaded = {records_processed}, 
        @SyncVersion = '{current_sync_version}', 
        @MarkInitialLoadComplete = 0
"""

spark.read.jdbc(url=jdbc_url, table=f"({update_query}) AS tmp", properties=connection_properties)
print(f"Control table updated successfully. New sync version: {current_sync_version}")

# 10. Clean up staging files
dbutils.fs.rm(staging_path, recurse=True)
print(f"Cleaned up staging path: {staging_path}")

# 11. Return Status
result = {
    "status": "SUCCESS",
    "records_processed": records_processed,
    "sync_version": current_sync_version
}
dbutils.notebook.exit(json.dumps(result))