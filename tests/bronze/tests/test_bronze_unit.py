import pytest
from chispa.dataframe_comparer import assert_df_equality
from pyspark.sql import Row
from src.bronze_notebook_logic import deduplicate_cdc, add_metadata_incremental

@pytest.mark.unit
def test_deduplicate_cdc(spark, cdc_schema):
    """
    UNIT: Tests that the deduplication logic correctly keeps the row with the 
    highest SYS_CHANGE_VERSION for a given primary key.
    """
    data = [
        Row(SYS_CHANGE_OPERATION="I", SYS_CHANGE_VERSION=100, CUSTOMER_ID=1, _current_sync_version=105, NAME="Alice", AMOUNT=100),
        Row(SYS_CHANGE_OPERATION="U", SYS_CHANGE_VERSION=105, CUSTOMER_ID=1, _current_sync_version=105, NAME="Alice Updated", AMOUNT=150),
        Row(SYS_CHANGE_OPERATION="I", SYS_CHANGE_VERSION=102, CUSTOMER_ID=2, _current_sync_version=105, NAME="Bob", AMOUNT=200)
    ]
    df = spark.createDataFrame(data, cdc_schema)
    
    result_df = deduplicate_cdc(df, primary_key_columns="CUSTOMER_ID")
    
    expected_data = [
        Row(SYS_CHANGE_OPERATION="U", SYS_CHANGE_VERSION=105, CUSTOMER_ID=1, _current_sync_version=105, NAME="Alice Updated", AMOUNT=150),
        Row(SYS_CHANGE_OPERATION="I", SYS_CHANGE_VERSION=102, CUSTOMER_ID=2, _current_sync_version=105, NAME="Bob", AMOUNT=200)
    ]
    expected_df = spark.createDataFrame(expected_data, cdc_schema)
    
    assert_df_equality(result_df, expected_df, ignore_row_order=True)

@pytest.mark.unit
def test_add_metadata_incremental(spark, cdc_schema):
    """
    UNIT: Tests that lineage and audit columns are correctly appended.
    """
    data = [Row(SYS_CHANGE_OPERATION="I", SYS_CHANGE_VERSION=100, CUSTOMER_ID=1, _current_sync_version=105, NAME="Alice", AMOUNT=100)]
    df = spark.createDataFrame(data, cdc_schema)
    
    result_df = add_metadata_incremental(df, pipeline_run_id="run-123", source_system="src-002")
    
    assert "_pipeline_run_id" in result_df.columns
    assert "_ingest_timestamp" in result_df.columns
    assert "_source_system" in result_df.columns
    assert "_cdc_operation" in result_df.columns
    assert "ingest_date" in result_df.columns
    
    row = result_df.first()
    assert row["_pipeline_run_id"] == "run-123"
    assert row["_source_system"] == "SRC-002"
    assert row["_cdc_operation"] == "I"