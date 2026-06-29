import pytest
from pyspark.sql import Row
from pyspark.sql.functions import sum as _sum
from src.bronze_notebook_logic import add_metadata_incremental, merge_incremental

@pytest.mark.reconciliation
def test_source_target_reconciliation(spark, temp_delta_path, cdc_schema):
    """
    RECONCILIATION: Source-vs-target row counts and key sums reconcile.
    Critical for loads/migrations.
    """
    # 1. Setup Initial Delta Table
    spark.createDataFrame([], schema="CUSTOMER_ID INT, NAME STRING, AMOUNT INT, _pipeline_run_id STRING, _source_system STRING, _cdc_operation STRING, ingest_date STRING") \
         .write.format("delta").save(temp_delta_path)
         
    # 2. Source Data
    source_data = [
        Row(SYS_CHANGE_OPERATION="I", SYS_CHANGE_VERSION=1, CUSTOMER_ID=1, _current_sync_version=1, NAME="A", AMOUNT=10),
        Row(SYS_CHANGE_OPERATION="I", SYS_CHANGE_VERSION=1, CUSTOMER_ID=2, _current_sync_version=1, NAME="B", AMOUNT=20),
        Row(SYS_CHANGE_OPERATION="I", SYS_CHANGE_VERSION=1, CUSTOMER_ID=3, _current_sync_version=1, NAME="C", AMOUNT=30)
    ]
    source_df = spark.createDataFrame(source_data, cdc_schema)
    
    # Calculate Source Metrics
    source_count = source_df.count()
    source_amount_sum = source_df.select(_sum("AMOUNT")).collect()[0][0]
    
    # 3. Process
    cdc_with_meta = add_metadata_incremental(source_df, "run-1", "src-002")
    merge_incremental(spark, cdc_with_meta, temp_delta_path, "CUSTOMER_ID")
    
    # 4. Target Metrics
    target_df = spark.read.format("delta").load(temp_delta_path)
    target_count = target_df.count()
    target_amount_sum = target_df.select(_sum("AMOUNT")).collect()[0][0]
    
    # 5. Reconcile
    assert source_count == target_count
    assert source_amount_sum == target_amount_sum