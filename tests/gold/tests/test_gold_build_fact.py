import pytest
from datetime import datetime
from decimal import Decimal
from pyspark.sql import Row

@pytest.mark.data_quality
@pytest.mark.integration
def test_fact_sales_dq_and_lookups(spark, run_notebook, setup_silver_data):
    """
    Data Quality & Integration Test: 
    1. Validates bad rows (negative line_total, zero quantity) are dropped (DQ-G-F-001, DQ-G-F-002).
    2. Validates dimension lookups resolve correctly, falling back to Unknown (-1) or Organic (-2).
    3. Validates business partitioning (order_year_month).
    """
    # Setup Silver Sources
    headers = [
        Row(order_id=1, order_number="ORD-001", customer_id=100, shipping_address_id=50, 
            order_date=datetime(2023, 5, 15), order_status="CLOSED", subtotal_amount=Decimal("100.00"), 
            discount_amount=Decimal("10.00"), tax_amount=Decimal("5.00"))
    ]
    lines = [
        # Valid Row
        Row(line_id=10, order_id=1, product_id=200, quantity=2, line_total=Decimal("100.00"), unit_price=Decimal("50.00")),
        # DQ Violation: Negative line_total
        Row(line_id=11, order_id=1, product_id=201, quantity=1, line_total=Decimal("-10.00"), unit_price=Decimal("-10.00")),
        # DQ Violation: Zero quantity
        Row(line_id=12, order_id=1, product_id=202, quantity=0, line_total=Decimal("50.00"), unit_price=Decimal("50.00"))
    ]
    crs = [
        Row(customer_id=100, channel="Organic", campaign_id=None)
    ]
    
    setup_silver_data("oe_order_headers_all", spark.createDataFrame(headers))
    setup_silver_data("oe_order_lines_all", spark.createDataFrame(lines))
    setup_silver_data("customer_registration_source", spark.createDataFrame(crs))
    
    # Setup Gold Dimensions (Mocking existing tables)
    spark.sql("CREATE DATABASE IF NOT EXISTS gold")
    spark.sql("""
        CREATE TABLE IF NOT EXISTS gold.dim_customer (
            dim_customer_key BIGINT, customer_id BIGINT, _valid_from TIMESTAMP, _valid_to TIMESTAMP
        ) USING DELTA
    """)
    spark.sql("INSERT INTO gold.dim_customer VALUES (999, 100, '2020-01-01', '2099-12-31')")
    
    spark.sql("CREATE TABLE IF NOT EXISTS gold.dim_product (dim_product_key BIGINT, inventory_item_id BIGINT) USING DELTA")
    spark.sql("CREATE TABLE IF NOT EXISTS gold.dim_location (dim_location_key BIGINT, address_id BIGINT) USING DELTA")
    spark.sql("CREATE TABLE IF NOT EXISTS gold.dim_campaign (dim_campaign_key BIGINT, campaign_id BIGINT) USING DELTA")
    
    widgets = {
        "pipeline_run_id": "run_fact_001",
        "target_gold_table": "fact_sales",
        "storage_account": "test_storage"
    }
    run_notebook("Gold_Build_Fact.py", widgets)
    
    fact_df = spark.sql("SELECT * FROM gold.fact_sales")
    
    # 1. Data Quality: Only 1 valid row should survive
    assert fact_df.count() == 1
    row = fact_df.first()
    assert row["line_id"] == 10
    
    # 2. Lookups
    assert row["dim_customer_key"] == 999 # Matched
    assert row["dim_product_key"] == -1 # Unmatched -> Unknown
    assert row["dim_location_key"] == -1 # Unmatched -> Unknown
    assert row["dim_campaign_key"] == -2 # Organic channel -> Organic member
    
    # 3. Calculations
    assert row["allocated_discount_amount"] == Decimal("10.00") # (100/100) * 10
    
    # 4. Partitioning
    assert row["order_year_month"] == 202305