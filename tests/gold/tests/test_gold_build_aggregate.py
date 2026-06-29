import pytest
from datetime import datetime
from decimal import Decimal
from pyspark.sql import Row

@pytest.mark.unit
@pytest.mark.reconciliation
def test_kpi_calculations(spark, run_notebook, setup_silver_data):
    """
    Unit & Reconciliation Test: Validates governed KPI definitions.
    - Customer Acquisition Cost (CAC)
    - Return on Ad Spend (ROAS)
    - Average Order Value (AOV)
    - Purchase Frequency
    """
    spark.sql("CREATE DATABASE IF NOT EXISTS gold")
    
    # Mock Fact Sales
    spark.sql("""
        CREATE TABLE IF NOT EXISTS gold.fact_sales (
            dim_date_key INT, dim_customer_key BIGINT, dim_campaign_key BIGINT, 
            order_number STRING, quantity INT, line_total DECIMAL(15,2)
        ) USING DELTA
    """)
    spark.sql("""
        INSERT INTO gold.fact_sales VALUES 
        (20230101, 1, 10, 'ORD1', 2, 100.00),
        (20230115, 1, 10, 'ORD2', 1, 50.00),
        (20230120, 2, -2, 'ORD3', 5, 200.00) -- Organic
    """)
    
    # Mock Dimensions
    spark.sql("CREATE TABLE IF NOT EXISTS gold.dim_date (dim_date_key INT, year INT, month_number INT, full_date DATE) USING DELTA")
    spark.sql("INSERT INTO gold.dim_date VALUES (20230101, 2023, 1, '2023-01-01'), (20230115, 2023, 1, '2023-01-15'), (20230120, 2023, 1, '2023-01-20')")
    
    spark.sql("CREATE TABLE IF NOT EXISTS gold.dim_campaign (dim_campaign_key BIGINT, campaign_id BIGINT, campaign_name STRING, channel STRING) USING DELTA")
    spark.sql("INSERT INTO gold.dim_campaign VALUES (10, 100, 'Promo', 'Paid Social'), (-2, -2, 'ORGANIC', 'ORGANIC')")
    
    spark.sql("CREATE TABLE IF NOT EXISTS gold.dim_customer (dim_customer_key BIGINT, customer_id BIGINT, status STRING, _valid_from TIMESTAMP, registration_date TIMESTAMP) USING DELTA")
    spark.sql("INSERT INTO gold.dim_customer VALUES (1, 1001, 'ACTIVE', '2023-01-01', '2023-01-01'), (2, 1002, 'ACTIVE', '2023-01-01', '2023-01-01')")
    
    # Mock Silver Campaign Spend
    mc_data = [Row(campaign_id=100, total_spend=Decimal("500.00"), customers_acquired=10)]
    setup_silver_data("marketing_campaigns", spark.createDataFrame(mc_data))
    
    # Run Aggregates
    widgets = {"pipeline_run_id": "run_agg", "storage_account": "test_storage"}
    
    # 1. Test Campaign ROI (CAC & ROAS)
    widgets["target_gold_table"] = "agg_monthly_campaign_roi"
    run_notebook("Gold_Build_Aggregate.py", widgets)
    
    roi_df = spark.sql("SELECT * FROM gold.agg_monthly_campaign_roi ORDER BY dim_campaign_key")
    rows = roi_df.collect()
    
    # Organic Campaign (-2)
    assert rows[0]["dim_campaign_key"] == -2
    assert rows[0]["customer_acquisition_cost"] == Decimal("0.00") # Organic CAC is 0
    
    # Paid Campaign (10)
    assert rows[1]["dim_campaign_key"] == 10
    assert rows[1]["customer_acquisition_cost"] == Decimal("50.00") # 500 spend / 10 acquired
    assert rows[1]["return_on_ad_spend"] == Decimal("0.30") # 150 revenue / 500 spend
    
    # 2. Test Customer CLV (AOV & Purchase Frequency)
    widgets["target_gold_table"] = "agg_customer_clv_metrics"
    run_notebook("Gold_Build_Aggregate.py", widgets)
    
    clv_df = spark.sql("SELECT * FROM gold.agg_customer_clv_metrics WHERE dim_customer_key = 1")
    clv_row = clv_df.first()
    
    assert clv_row["average_order_value"] == Decimal("75.00") # 150 total / 2 orders
    # Purchase Frequency: 2 orders / (14 days / 30.0) = 2 / 0.466 = 4.285
    expected_freq = Decimal(2) / (Decimal(14) / Decimal(30.0))
    assert round(clv_row["purchase_frequency"], 2) == round(expected_freq, 2)