import pytest
from pyspark.sql.types import *
from chispa.dataframe_comparisons import assert_df_equality
from datetime import datetime

NOTEBOOK_PATH = "databricks/notebooks/gold/Gold_Build_Fact.py"

@pytest.mark.data_quality
@pytest.mark.reconciliation
def test_fact_sales_dq_and_reconciliation(spark, temp_datalake, run_notebook, setup_silver_tables):
    """
    Tests Data Quality rules (dropping bad rows) and Join Cardinality/Reconciliation.
    DQ-G-F-001: line_total >= 0
    DQ-G-F-002: quantity > 0
    """
    # Setup Silver Headers
    headers_schema = StructType([
        StructField("order_id", LongType(), True),
        StructField("order_number", StringType(), True),
        StructField("customer_id", LongType(), True),
        StructField("shipping_address_id", LongType(), True),
        StructField("order_date", TimestampType(), True),
        StructField("order_status", StringType(), True),
        StructField("subtotal_amount", DecimalType(15,2), True),
        StructField("discount_amount", DecimalType(15,2), True),
        StructField("tax_amount", DecimalType(15,2), True)
    ])
    headers_data = [
        (1, "ORD-001", 1001, 5001, datetime(2023, 10, 15), "COMPLETED", 100.00, 10.00, 5.00)
    ]
    setup_silver_tables("oe_order_headers_all", headers_data, headers_schema)
    
    # Setup Silver Lines (Includes 1 Valid, 2 Invalid rows)
    lines_schema = StructType([
        StructField("line_id", LongType(), True),
        StructField("order_id", LongType(), True),
        StructField("product_id", LongType(), True),
        StructField("quantity", IntegerType(), True),
        StructField("unit_price", DecimalType(15,2), True),
        StructField("line_total", DecimalType(15,2), True)
    ])
    lines_data = [
        (101, 1, 2001, 2, 50.00, 100.00),   # Valid
        (102, 1, 2002, 0, 50.00, 0.00),     # Invalid: quantity = 0
        (103, 1, 2003, 1, -10.00, -10.00)   # Invalid: line_total < 0
    ]
    setup_silver_tables("oe_order_lines_all", lines_data, lines_schema)
    
    # Setup empty CRS to satisfy joins
    crs_schema = StructType([
        StructField("customer_id", LongType(), True),
        StructField("campaign_id", IntegerType(), True),
        StructField("channel", StringType(), True)
    ])
    setup_silver_tables("customer_registration_source", [], crs_schema)
    
    # Setup empty dimensions to satisfy lookups (will default to -1)
    spark.sql("CREATE DATABASE IF NOT EXISTS gold")
    spark.sql("CREATE TABLE IF NOT EXISTS gold.dim_customer (dim_customer_key BIGINT, customer_id BIGINT, _valid_from TIMESTAMP, _valid_to TIMESTAMP) USING DELTA")
    spark.sql("CREATE TABLE IF NOT EXISTS gold.dim_product (dim_product_key BIGINT, inventory_item_id BIGINT) USING DELTA")
    spark.sql("CREATE TABLE IF NOT EXISTS gold.dim_location (dim_location_key BIGINT, address_id BIGINT) USING DELTA")
    spark.sql("CREATE TABLE IF NOT EXISTS gold.dim_campaign (dim_campaign_key BIGINT, campaign_id INT) USING DELTA")
    
    run_notebook(NOTEBOOK_PATH, {
        "pipeline_run_id": "run_fact_001",
        "target_gold_table": "fact_sales",
        "storage_account": "test_account"
    })
    
    fact_df = spark.read.format("delta").load(f"{temp_datalake}/gold/facts/fact_sales/")
    
    # Reconciliation & DQ Assertions
    # Source had 3 lines. 2 violated DQ rules. Target should have exactly 1 line.
    assert fact_df.count() == 1
    
    valid_row = fact_df.collect()[0]
    assert valid_row["line_id"] == 101
    assert valid_row["quantity"] == 2
    assert valid_row["line_total"] == 100.00
    
    # Verify Partitioning Key
    assert valid_row["order_year_month"] == 202310
    
    # Verify Allocation Logic (100/100 * 10 = 10.00 discount)
    assert valid_row["allocated_discount_amount"] == 10.00