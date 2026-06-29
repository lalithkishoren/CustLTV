import json
import pytest
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, DecimalType, BooleanType
from chispa.dataframe_comparer import assert_df_equality
from datetime import datetime

NOTEBOOK_PATH = "databricks/notebooks/silver/Silver_Transform_Dynamic.py"

@pytest.fixture
def customers_rules():
    return [
        {"rule_type": "CAST", "source_column": "CUSTOMER_ID", "target_column": "customer_id", "transformation_expression": "BIGINT"},
        {"rule_type": "TRANSFORM", "source_column": "EMAIL", "target_column": "email", "transformation_expression": "LOWER(TRIM(EMAIL))"},
        {"rule_type": "TRANSFORM", "source_column": "STATUS", "target_column": "status", "transformation_expression": "UPPER(TRIM(STATUS))"}
    ]

@pytest.fixture
def orders_rules():
    return [
        {"rule_type": "CAST", "source_column": "ORDER_ID", "target_column": "order_id", "transformation_expression": "BIGINT"},
        {"rule_type": "CAST", "source_column": "CUSTOMER_ID", "target_column": "customer_id", "transformation_expression": "BIGINT"},
        {"rule_type": "CAST", "source_column": "TOTAL_AMOUNT", "target_column": "total_amount", "transformation_expression": "DECIMAL(15,2)"}
    ]

@pytest.mark.spark
@pytest.mark.dq
def test_dq_quarantine_and_reconciliation_orders(spark, temp_dir, execute_notebook, orders_rules):
    """
    DATA QUALITY & RECONCILIATION: 
    Tests that error-severity rules (TOTAL_AMOUNT <= 0, NULLs) route to quarantine.
    Validates that records_read = records_written + records_filtered + records_quarantined.
    """
    bronze_path = f"{temp_dir}/bronze/src-001/erp/oe_order_headers_all/"
    
    # Synthetic data: 1 valid, 3 invalid (violating DQ rules)
    data = [
        Row(ORDER_ID="1", CUSTOMER_ID="100", TOTAL_AMOUNT="150.00", _ingest_timestamp=datetime(2023, 1, 1), _cdc_operation="I"), # Valid
        Row(ORDER_ID="2", CUSTOMER_ID="101", TOTAL_AMOUNT="-10.00", _ingest_timestamp=datetime(2023, 1, 1), _cdc_operation="I"), # Invalid: Negative amount
        Row(ORDER_ID="3", CUSTOMER_ID="102", TOTAL_AMOUNT=None, _ingest_timestamp=datetime(2023, 1, 1), _cdc_operation="I"),     # Invalid: Null amount
        Row(ORDER_ID="4", CUSTOMER_ID=None, TOTAL_AMOUNT="50.00", _ingest_timestamp=datetime(2023, 1, 1), _cdc_operation="I")      # Invalid: Null customer
    ]
    spark.createDataFrame(data).write.format("delta").save(bronze_path)

    params = {
        "pipeline_run_id": "run_001",
        "silver_table_id": "20",
        "source_bronze_table": "src-001/erp/oe_order_headers_all",
        "target_silver_table": "oe_order_headers_all",
        "primary_key_columns": "order_id",
        "scd_type": "1",
        "transformation_rules_json": json.dumps(orders_rules)
    }

    result = execute_notebook(NOTEBOOK_PATH, params)

    # RECONCILIATION ASSERTIONS
    assert result["records_read"] == 4
    assert result["records_quarantined"] == 3
    assert result["records_written"] == 1
    assert result["records_filtered"] == 0

    # Verify Silver Table
    silver_df = spark.read.format("delta").load(f"{temp_dir}/silver/oe_order_headers_all/")
    assert silver_df.count() == 1
    assert silver_df.collect()[0]["order_id"] == 1

    # Verify Quarantine Table
    quarantine_df = spark.read.format("delta").load(f"{temp_dir}/silver/quarantine/oe_order_headers_all/")
    assert quarantine_df.count() == 3
    assert "_dq_failure_reason" in quarantine_df.columns

@pytest.mark.spark
@pytest.mark.dq
def test_dq_warning_and_deduplication_customers(spark, temp_dir, execute_notebook, customers_rules):
    """
    DATA QUALITY (WARN) & DEDUPLICATION:
    Tests that warn-severity rules (STATUS IS NULL) tag rows but do NOT drop them.
    Tests deduplication keeps the latest row by _ingest_timestamp.
    """
    bronze_path = f"{temp_dir}/bronze/src-002/crm/customers/"
    
    data = [
        # Deduplication test: ID 1 has two records, T2 is later
        Row(CUSTOMER_ID="1", EMAIL=" TEST@MAIL.COM ", STATUS="active", _ingest_timestamp=datetime(2023, 1, 1), _cdc_operation="I"),
        Row(CUSTOMER_ID="1", EMAIL=" test@mail.com ", STATUS="ACTIVE", _ingest_timestamp=datetime(2023, 1, 2), _cdc_operation="U"),
        # Warning test: ID 2 has NULL status (should be tagged, not dropped)
        Row(CUSTOMER_ID="2", EMAIL="user2@mail.com", STATUS=None, _ingest_timestamp=datetime(2023, 1, 1), _cdc_operation="I"),
        # Error test: ID NULL (should be quarantined)
        Row(CUSTOMER_ID=None, EMAIL="user3@mail.com", STATUS="active", _ingest_timestamp=datetime(2023, 1, 1), _cdc_operation="I")
    ]
    spark.createDataFrame(data).write.format("delta").save(bronze_path)

    params = {
        "pipeline_run_id": "run_002",
        "silver_table_id": "10",
        "source_bronze_table": "src-002/crm/customers",
        "target_silver_table": "customers",
        "primary_key_columns": "customer_id",
        "scd_type": "1",
        "transformation_rules_json": json.dumps(customers_rules)
    }

    result = execute_notebook(NOTEBOOK_PATH, params)

    assert result["records_read"] == 4
    assert result["records_quarantined"] == 1 # The NULL customer_id
    assert result["records_written"] == 2     # ID 1 (deduped) and ID 2 (warned)

    silver_df = spark.read.format("delta").load(f"{temp_dir}/silver/customers/").orderBy("customer_id")
    results = silver_df.collect()
    
    # Assert Deduplication & Transformations (UNIT)
    assert results[0]["customer_id"] == 1
    assert results[0]["email"] == "test@mail.com" # Trimmed and lowered
    assert results[0]["status"] == "ACTIVE"
    assert results[0]["_dq_warning"] is None

    # Assert Warning Tagging
    assert results[1]["customer_id"] == 2
    assert results[1]["status"] is None
    assert results[1]["_dq_warning"] == "STATUS_IS_NULL" # Tagged, not dropped

@pytest.mark.spark
@pytest.mark.scd
def test_scd_type_2_history_tracking(spark, temp_dir, execute_notebook, customers_rules):
    """
    SCD TYPE 2 & IDEMPOTENCY:
    Tests that updates to tracked columns expire the old record and insert a new active one.
    Tests that running the exact same data twice (Idempotency) results in no changes.
    """
    bronze_path = f"{temp_dir}/bronze/src-002/crm/customers/"
    params = {
        "pipeline_run_id": "run_003",
        "silver_table_id": "10",
        "source_bronze_table": "src-002/crm/customers",
        "target_silver_table": "customers",
        "primary_key_columns": "customer_id",
        "scd_type": "2",
        "track_history_columns": "status",
        "transformation_rules_json": json.dumps(customers_rules)
    }

    # LOAD 1: Initial Insert
    data1 = [Row(CUSTOMER_ID="1", EMAIL="test@mail.com", STATUS="ACTIVE", _ingest_timestamp=datetime(2023, 1, 1), _cdc_operation="I")]
    spark.createDataFrame(data1).write.format("delta").mode("overwrite").save(bronze_path)
    execute_notebook(NOTEBOOK_PATH, params)

    # LOAD 2: Update tracked column (STATUS)
    data2 = [Row(CUSTOMER_ID="1", EMAIL="test@mail.com", STATUS="INACTIVE", _ingest_timestamp=datetime(2023, 1, 2), _cdc_operation="U")]
    spark.createDataFrame(data2).write.format("delta").mode("overwrite").save(bronze_path)
    execute_notebook(NOTEBOOK_PATH, params)

    silver_df = spark.read.format("delta").load(f"{temp_dir}/silver/customers/").orderBy("status")
    results = silver_df.collect()

    assert len(results) == 2
    
    # Old record should be expired
    assert results[0]["status"] == "ACTIVE"
    assert results[0]["_is_current"] == False
    assert results[0]["_valid_to"] is not None

    # New record should be current
    assert results[1]["status"] == "INACTIVE"
    assert results[1]["_is_current"] == True
    assert results[1]["_valid_to"] is None

    # LOAD 3: IDEMPOTENCY CHECK (Run exact same data again)
    execute_notebook(NOTEBOOK_PATH, params)
    silver_df_idempotent = spark.read.format("delta").load(f"{temp_dir}/silver/customers/")
    
    # Count should still be exactly 2, no new rows created
    assert silver_df_idempotent.count() == 2