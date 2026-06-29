import pytest
from pyspark.sql import Row
from src.bronze_notebook_logic import apply_data_quality_rules

@pytest.mark.dq
def test_data_quality_severity_split(spark, cdc_schema):
    """
    DATA QUALITY: Asserts the SEVERITY split.
    ERROR severity (CUSTOMER_ID is null) -> Dropped.
    WARN severity (AMOUNT <= 0) -> Tagged in dq_warnings, NOT dropped.
    """
    data = [
        Row(SYS_CHANGE_OPERATION="I", SYS_CHANGE_VERSION=1, CUSTOMER_ID=1, _current_sync_version=1, NAME="Valid Row", AMOUNT=100),
        Row(SYS_CHANGE_OPERATION="I", SYS_CHANGE_VERSION=1, CUSTOMER_ID=None, _current_sync_version=1, NAME="Error Row (Null PK)", AMOUNT=100),
        Row(SYS_CHANGE_OPERATION="I", SYS_CHANGE_VERSION=1, CUSTOMER_ID=2, _current_sync_version=1, NAME="Warn Row (Negative Amt)", AMOUNT=-50)
    ]
    df = spark.createDataFrame(data, cdc_schema)
    
    result_df = apply_data_quality_rules(df)
    results = {row["NAME"]: row for row in result_df.collect()}
    
    # Assert ERROR row is dropped
    assert "Error Row (Null PK)" not in results
    
    # Assert Valid row is kept and has empty warnings
    assert "Valid Row" in results
    assert len(results["Valid Row"]["dq_warnings"]) == 0
    
    # Assert WARN row is kept and tagged
    assert "Warn Row (Negative Amt)" in results
    assert "AMOUNT_LESS_THAN_ZERO" in results["Warn Row (Negative Amt)"]["dq_warnings"]