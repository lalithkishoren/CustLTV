import pytest
from pyspark.sql import Row
from src.bronze_notebook_logic import add_metadata_incremental, merge_incremental

@pytest.mark.idempotency
def test_merge_idempotency(spark, temp_delta_path, cdc_schema):
    """
    IDEMPOTENCY: Running the same load/MERGE twice produces the SAME result.
    Proves re-runs are safe and do not create duplicates.
    """
    # 1. Setup Initial Delta Table
    spark.createDataFrame([], schema="CUSTOMER_ID INT, NAME STRING, AMOUNT INT, _pipeline_run_id STRING, _source_system STRING, _cdc_operation STRING, ingest_date STRING") \
         .write.format("delta").save(temp_delta_path)
    
    # 2. Create CDC Payload
    cdc_data = [
        Row(SYS_CHANGE_OPERATION="I", SYS_CHANGE_VERSION=100, CUSTOMER_ID=1, _current_sync_version=100, NAME="Alice", AMOUNT=100)
    ]
    cdc_df = spark.createDataFrame(cdc_data, cdc_schema)
    cdc_with_meta = add_metadata_incremental(cdc_df, "run-1", "src-002")
    
    # 3. First Run
    merge_incremental(spark, cdc_with_meta, temp_delta_path, "CUSTOMER_ID")
    count_run_1 = spark.read.format("delta").load(temp_delta_path).count()
    assert count_run_1 == 1
    
    # 4. Second Run (Exact same payload)
    merge_incremental(spark, cdc_with_meta, temp_delta_path, "CUSTOMER_ID")
    count_run_2 = spark.read.format("delta").load(temp_delta_path).count()
    
    # 5. Assert no duplicates were created
    assert count_run_1 == count_run_2 == 1