import pytest
from pyspark.sql import Row
from src.bronze_notebook_logic import add_metadata_incremental

@pytest.mark.contract
def test_bronze_schema_contract(spark, cdc_schema):
    """
    CONTRACT: The output schema matches the declared data contract.
    Ensures additive schema evolution is supported but core columns exist.
    """
    data = [Row(SYS_CHANGE_OPERATION="I", SYS_CHANGE_VERSION=100, CUSTOMER_ID=1, _current_sync_version=105, NAME="Alice", AMOUNT=100)]
    df = spark.createDataFrame(data, cdc_schema)
    
    result_df = add_metadata_incremental(df, "run-1", "src-002")
    
    expected_columns = {
        "CUSTOMER_ID", "NAME", "AMOUNT", 
        "_pipeline_run_id", "_ingest_timestamp", "_source_system", "_cdc_operation", "ingest_date"
    }
    
    actual_columns = set(result_df.columns)
    
    # Assert all expected columns are present (allows additive columns like SYS_CHANGE_*)
    assert expected_columns.issubset(actual_columns)
    
    # Assert specific types
    dtypes = dict(result_df.dtypes)
    assert dtypes["CUSTOMER_ID"] == "int"
    assert dtypes["_pipeline_run_id"] == "string"
    assert dtypes["_ingest_timestamp"] == "timestamp"