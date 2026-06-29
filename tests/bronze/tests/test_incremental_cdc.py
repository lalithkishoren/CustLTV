import pytest
from pyspark.sql.types import StructType, StructField, StringType, IntegerType
from chispa.dataframe_comparer import assert_df_equality

NOTEBOOK_PATH = "databricks/notebooks/bronze/Incremental_CDC_Dynamic.py"

@pytest.fixture
def setup_cdc_environment(spark, tmp_path):
    """Sets up the initial Delta table and the incoming CDC staging data."""
    
    # 1. Create Initial Delta Table (Target)
    target_schema = StructType([
        StructField("CUSTOMER_ID", IntegerType(), True),
        StructField("NAME", StringType(), True),
        StructField("STATUS", StringType(), True),
        StructField("_pipeline_run_id", StringType(), True),
        StructField("_source_system", StringType(), True),
        StructField("_cdc_operation", StringType(), True),
        StructField("ingest_date", StringType(), True)
    ])
    
    target_data = [
        (1, "Alice", "ACTIVE", "run-000", "SRC-002", "I", "2023-01-01"),
        (2, "Bob", "ACTIVE", "run-000", "SRC-002", "I", "2023-01-01"),
        (3, "Charlie", "ACTIVE", "run-000", "SRC-002", "I", "2023-01-01")
    ]
    
    delta_path = f"{tmp_path}/bronze/src-002/crm/customers/"
    spark.createDataFrame(target_data, target_schema).write.format("delta").save(delta_path)
    
    # 2. Create CDC Staging Data (Source)
    # Includes:
    # - Update to Alice (ID 1)
    # - Delete Bob (ID 2)
    # - Insert Dave (ID 4)
    # - Multiple changes for Eve (ID 5) to test deduplication (Fan-out prevention)
    source_schema = StructType([
        StructField("SYS_CHANGE_OPERATION", StringType(), True),
        StructField("SYS_CHANGE_VERSION", IntegerType(), True),
        StructField("CUSTOMER_ID", IntegerType(), True),
        StructField("_current_sync_version", IntegerType(), True),
        StructField("NAME", StringType(), True),
        StructField("STATUS", StringType(), True)
    ])
    
    source_data = [
        ("U", 101, 1, 105, "Alice Updated", "ACTIVE"),       # Update
        ("D", 102, 2, 105, None, None),                      # Delete
        ("I", 103, 4, 105, "Dave", "ACTIVE"),                # Insert
        ("I", 104, 5, 105, "Eve", "ACTIVE"),                 # Insert (Older version)
        ("U", 105, 5, 105, "Eve", "INACTIVE")                # Update (Newer version - should win)
    ]
    
    staging_dir = f"{tmp_path}/bronze/staging/src-002/crm/customers/incremental/run-124/"
    spark.createDataFrame(source_data, source_schema).write.mode("overwrite").parquet(staging_dir)
    
    return delta_path

@pytest.mark.integration
@pytest.mark.unit
def test_incremental_cdc_scd_and_dedup(spark, run_notebook, setup_cdc_environment):
    """
    Tests Incremental CDC:
    1. UNIT (Dedup): Multiple changes for the same PK are deduplicated (Eve ID 5).
    2. SCD Type 1: Updates modify rows, Deletes remove rows, Inserts add rows.
    """
    delta_path = setup_cdc_environment
    
    widgets = {
        "pipeline_run_id": "run-124",
        "table_id": "1",
        "source_system": "src-002",
        "schema_name": "crm",
        "table_name": "customers",
        "primary_key_columns": "CUSTOMER_ID",
        "last_sync_version": "100"
    }
    
    result = run_notebook(NOTEBOOK_PATH, widgets)
    
    assert result["status"] == "SUCCESS"
    assert result["records_loaded"] == 5 # 5 raw records read from staging
    
    # Verify Delta Table State
    df_final = spark.read.format("delta").load(delta_path).orderBy("CUSTOMER_ID").collect()
    
    # Expected state:
    # ID 1: Updated
    # ID 2: Deleted (Not present)
    # ID 3: Untouched
    # ID 4: Inserted
    # ID 5: Inserted/Updated to INACTIVE (Dedup logic applied)
    
    assert len(df_final) == 4
    
    # ID 1 (Update)
    assert df_final[0]["CUSTOMER_ID"] == 1
    assert df_final[0]["NAME"] == "Alice Updated"
    assert df_final[0]["_cdc_operation"] == "U"
    
    # ID 3 (Untouched)
    assert df_final[1]["CUSTOMER_ID"] == 3
    assert df_final[1]["NAME"] == "Charlie"
    assert df_final[1]["_cdc_operation"] == "I" # Original operation
    
    # ID 4 (Insert)
    assert df_final[2]["CUSTOMER_ID"] == 4
    assert df_final[2]["NAME"] == "Dave"
    assert df_final[2]["_cdc_operation"] == "I"
    
    # ID 5 (Dedup check - should be INACTIVE)
    assert df_final[3]["CUSTOMER_ID"] == 5
    assert df_final[3]["STATUS"] == "INACTIVE"
    assert df_final[3]["_cdc_operation"] == "U"

@pytest.mark.idempotency
def test_incremental_cdc_idempotency(spark, run_notebook, setup_cdc_environment):
    """
    Tests Idempotency:
    Running the exact same CDC merge twice should not duplicate rows or fail.
    """
    delta_path = setup_cdc_environment
    
    widgets = {
        "pipeline_run_id": "run-124",
        "table_id": "1",
        "source_system": "src-002",
        "schema_name": "crm",
        "table_name": "customers",
        "primary_key_columns": "CUSTOMER_ID",
        "last_sync_version": "100"
    }
    
    # Run 1
    run_notebook(NOTEBOOK_PATH, widgets)
    df_run1 = spark.read.format("delta").load(delta_path).drop("_ingest_timestamp")
    
    # Run 2
    run_notebook(NOTEBOOK_PATH, widgets)
    df_run2 = spark.read.format("delta").load(delta_path).drop("_ingest_timestamp")
    
    # Assert exact match
    assert_df_equality(df_run1, df_run2, ignore_row_order=True)