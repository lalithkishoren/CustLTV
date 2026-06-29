# Databricks notebook source
# ===============================================================================
# Initial_Load_Dynamic.py
# PURPOSE: Process initial load from Staging (Parquet) to Bronze (Delta)
# ===============================================================================

import pyodbc
from pyspark.sql.functions import current_timestamp, lit, col

# 1. Define Widgets
dbutils.widgets.text("pipeline_run_id", "", "Pipeline Run ID")
dbutils.widgets.text("table_id", "", "Table ID")
dbutils.widgets.text("source_system", "", "Source System")
dbutils.widgets.text("schema_name", "", "Schema Name")
dbutils.widgets.text("table_name", "", "Table Name")
dbutils.widgets.text("primary_key_columns", "", "Primary Key Columns")
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
storage_account = dbutils.widgets.get("storage_account")
sql_server = dbutils.widgets.get("sql_server")
sql_database = dbutils.widgets.get("sql_database")
sql_username = dbutils.widgets.get("sql_username")
sql_password = dbutils.widgets.get("sql_password")

# ADLS auth: handled by Unity Catalog (NO account key, NO fs.azure.* auth)

# 3. Define Paths
# CRITICAL: Read from STAGING path (Parquet), Write to DELTA TABLE path
staging_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/staging/{source_system}/{schema_name}/{table_name}/initial/{pipeline_run_id}/"
delta_path = f"abfss://bronze@{storage_account}.dfs.core.windows.net/{source_system}/{schema_name}/{table_name}/"

print(f"Reading from Staging: {staging_path}")
print(f"Writing to Delta: {delta_path}")

# 4. Read Staging Data
try:
    df = spark.read.format("parquet").load(staging_path)
    records_loaded = df.count()
    print(f"Read {records_loaded} records from staging.")
except Exception as e:
    raise Exception(f"Failed to read staging data: {str(e)}")

# 5. Add Metadata Columns
df_with_metadata = df \
    .withColumn("_pipeline_run_id", lit(pipeline_run_id)) \
    .withColumn("_ingest_timestamp", current_timestamp()) \
    .withColumn("_source_system", lit(source_system)) \
    .withColumn("_cdc_operation", lit("I"))

# 6. Write to Delta Table (Overwrite for Initial Load)
df_with_metadata.write \
    .format("delta") \
    .mode("overwrite") \
    .option("mergeSchema", "true") \
    .save(delta_path)

print(f"Successfully wrote to Delta table at {delta_path}")

# 7. Get Current Sync Version (if applicable, from source DB via JDBC)
# For simplicity in this script, we assume the initial load sync version is 0 or fetched prior.
# In a real scenario, you might query the source DB here. We will set it to '0' to start CDC.
current_sync_version = '0'

# 8. Update Control Table via pyodbc
conn_str = f"DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={sql_server};DATABASE={sql_database};UID={sql_username};PWD={sql_password}"

try:
    conn = pyodbc.connect(conn_str)
    cursor = conn.cursor()
    
    # CRITICAL SECTION - Update control table
    update_sql = """
        EXEC control.sp_UpdateTableMetadata 
            @TableId = ?, 
            @Status = 'SUCCESS', 
            @PipelineRunId = ?, 
            @RecordsLoaded = ?, 
            @SyncVersion = ?, 
            @MarkInitialLoadComplete = 1
    """
    cursor.execute(update_sql, (table_id, pipeline_run_id, records_loaded, current_sync_version))
    conn.commit()
    
    # Verification query
    cursor.execute("SELECT initial_load_completed FROM control.table_metadata WHERE table_id = ?", (table_id,))
    row = cursor.fetchone()
    print(f"Verification: initial_load_completed = {row[0]}")
    
    cursor.close()
    conn.close()
    print("Successfully updated control database.")
except Exception as e:
    raise Exception(f"Failed to update control database: {str(e)}")

# 9. Return Status
dbutils.notebook.exit(f'{{"status": "SUCCESS", "records_loaded": {records_loaded}, "table": "{schema_name}.{table_name}"}}')