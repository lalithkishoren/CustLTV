import pytest
from pyspark.sql import Row
from src.bronze_notebook_logic import add_metadata_incremental, merge_incremental

@pytest.mark.scd
def test_merge_incremental_scd_behaviors(spark, temp_delta_path, cdc_schema):
    """
    SCD: Tests Type 1 upsert and delete handling (tombstone/hard delete).
    Validates Insert (I), Update (U), and Delete (D) operations.
    """
    # 1. Setup Initial Delta Table
    initial_data = [
        Row(CUSTOMER_ID=1, NAME="Alice", AMOUNT=100, _pipeline_run_id="run-0", _source_system="SRC-002", _cdc_operation="I", ingest_date="2023-01-01"),
        Row(CUSTOMER_ID=2, NAME="Bob", AMOUNT=200, _pipeline_run_id="run-0", _source_system="SRC-002", _cdc_operation="I", ingest_date="2023-01-01")
    ]
    initial_df = spark.createDataFrame(initial_data)
    initial_df.write.format("delta").save(temp_delta_path)
    
    # 2. Create CDC Changes (Update Alice, Delete Bob, Insert Charlie)
    cdc_data = [
        Row(SYS_CHANGE_OPERATION="U", SYS_CHANGE_VERSION=101, CUSTOMER_ID=1, _current_sync_version=101, NAME="Alice Updated", AMOUNT=150),
        Row(SYS_CHANGE_OPERATION="D", SYS_CHANGE_VERSION=101, CUSTOMER_ID=2, _current_sync_version=101, NAME="Bob", AMOUNT=200),
        Row(SYS_CHANGE_OPERATION="I", SYS_CHANGE_VERSION=101, CUSTOMER_ID=3, _current_sync_version=101, NAME="Charlie", AMOUNT=300)
    ]
    cdc_df = spark.createDataFrame(cdc_data, cdc_schema)
    cdc_with_meta = add_metadata_incremental(cdc_df, "run-1", "src-002")
    
    # 3. Execute Merge
    merge_incremental(spark, cdc_with_meta, temp_delta_path, "CUSTOMER_ID")
    
    # 4. Assertions
    result_df = spark.read.format("delta").load(temp_delta_path)
    results = {row["CUSTOMER_ID"]: row for row in result_df.collect()}
    
    # Alice should be updated
    assert results[1]["NAME"] == "Alice Updated"
    assert results[1]["AMOUNT"] == 150
    assert results[1]["_cdc_operation"] == "U"
    
    # Bob should be deleted (hard delete per notebook logic: whenMatchedDelete)
    assert 2 not in results
    
    # Charlie should be inserted
    assert results[3]["NAME"] == "Charlie"
    assert results[3]["_cdc_operation"] == "I"