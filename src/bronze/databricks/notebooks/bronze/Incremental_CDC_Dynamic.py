# Databricks notebook source
# ===============================================================================
# Incremental_CDC_Dynamic.py
# PURPOSE: Apply CDC changes using MERGE from Staging (Parquet) to Bronze (Delta)
# ===============================================================================

import pyodbc
from pyspark.sql.functions import current_timestamp, lit, col, row_number
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# 1. Define Widgets
dbutils.widgets.text("pipeline_run_id", "", "Pipeline Run ID")
dbutils.widgets.text("table_id", "", "Table ID")
dbutils.widgets.text("source_system", "", "Source System")
dbutils.widgets.text("schema_name", "", "Schema Name")
dbutils.widgets.text("table_name", "", "Table Name")
dbutils.widgets.text("primary_key_columns", "", "Primary Key Columns")
dbutils.widgets.text("last_sync_version", "", "Last Sync Version")
dbutils.widgets.text("storage_account", "{{PLACEHOLDER_STORAGE_ACCOUNT}}", "Storage Account")
dbutils.widgets.text("sql_server", "{{PLACEHOLDER_SQL_SERVER_FQDN}}", "SQL Server")
dbutils.widgets.text("sql_database", "{{PLACEHOLDER_SQL_DATABASE}}", "SQL Database")
dbutils.widgets.text("sql_username", "{{PLACEHOLDER_SQL_USERNAME}}", "SQL Username")
dbutils.widgets.text("sql_password", "{{PLACEHOLDER_SQL_PASSWORD}}", "SQL Password")

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

# ADLS auth: handled by Unity Catalog (NO account key, NO fs.azure.* auth)

# 3. Define Paths
staging_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/staging/{source_system}/{schema_name}/{table_name}/incremental/{pipeline_run_id}/"
delta_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/{source_system}/{schema_name}/{table_name}/"

# 4. Read CDC changes from Staging
try:
    cdc_df = spark.read.format("parquet").load(staging_path)
    records_processed = cdc_df.count()
    if records_processed == 0:
        dbutils.notebook.exit('{"status": "SUCCESS", "records_loaded": 0}')
except Exception as e:
    raise Exception(f"Failed to read staging data: {str(e)}")

# Extract new sync version from the first row (injected by sp_GetCDCChanges)
new_sync_version = str(cdc_df.select("_current_sync_version").first()[0])

# 5. Add Metadata Columns
source_df = cdc_df \
    .withColumn("_pipeline_run_id", lit(pipeline_run_id)) \
    .withColumn("_ingest_timestamp", current_timestamp()) \
    .withColumn("_source_system", lit(source_system)) \
    .withColumn("_cdc_operation", col("SYS_CHANGE_OPERATION"))

# 6. Pre-MERGE Deduplication (CRITICAL)
pk_cols = [pk.strip() for pk in primary_key_columns.split(',')]

# Validate PK columns exist
for pk in pk_cols:
    if pk not in source_df.columns:
        raise Exception(f"Primary key column {pk} not found in source dataframe.")

window_spec = Window.partitionBy(*pk_cols).orderBy(col("SYS_CHANGE_VERSION").desc())

source_deduped = source_df.withColumn("row_num", row_number().over(window_spec)) \
                          .filter(col("row_num") == 1) \
                          .drop("row_num")

# 7. Read existing DELTA TABLE for MERGE
if not DeltaTable.isDeltaTable(spark, delta_path):
    raise Exception(f"Target Delta table does not exist at {delta_path}. Run Initial Load first.")

target_table = DeltaTable.forPath(spark, delta_path)

# 8. Build merge condition dynamically
merge_condition = " AND ".join([f"target.{pk} = source.{pk}" for pk in pk_cols])

# Get columns for update/insert (exclude technical CDC columns)
exclude_cols = ["SYS_CHANGE_OPERATION", "SYS_CHANGE_VERSION", "_current_sync_version"]
all_columns = [c for c in source_deduped.columns if c not in exclude_cols]
update_columns = [c for c in all_columns if c not in pk_cols]

# 9. COMPLETE MERGE implementation
print(f"Executing MERGE on {delta_path} with condition: {merge_condition}")

target_table.alias("target").merge(
    source_deduped.alias("source"),
    merge_condition
).whenMatchedDelete(
    condition="source.SYS_CHANGE_OPERATION = 'D'"
).whenMatchedUpdate(
    condition="source.SYS_CHANGE_OPERATION IN ('U', 'I')",
    set={column: f"source.{column}" for column in update_columns}
).whenNotMatchedInsert(
    condition="source.SYS_CHANGE_OPERATION != 'D'",
    values={column: f"source.{column}" for column in all_columns}
).execute()

# 10. POST-MERGE Optimization (CRITICAL for performance)
print(f"Optimizing Delta table: {delta_path}")
spark.sql(f"""
    OPTIMIZE delta.`{delta_path}`
    ZORDER BY ({primary_key_columns}, _ingest_timestamp)
""")

# 11. Update Control Table via pyodbc
conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={sql_server};DATABASE={sql_database};UID={sql_username};PWD={sql_password}"

try:
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    
    update_sql = """
        EXEC control.sp_UpdateTableMetadata 
            @TableId = ?, 
            @Status = 'SUCCESS', 
            @PipelineRunId = ?, 
            @RecordsLoaded = ?, 
            @SyncVersion = ?, 
            @MarkInitialLoadComplete = 0
    """
    cursor.execute(update_sql, (table_id, pipeline_run_id, records_processed, new_sync_version))
    conn.commit()
    cursor.close()
    conn.close()
    print("Successfully updated control database.")
except Exception as e:
    raise Exception(f"Failed to update control database: {str(e)}")

# 12. Return Status
dbutils.notebook.exit(f'{{"status": "SUCCESS", "records_processed": {records_processed}, "new_sync_version": "{new_sync_version}"}}')