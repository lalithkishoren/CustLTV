import pytest
from pyspark.sql.types import *
from datetime import datetime, date

NOTEBOOK_PATH = "databricks/notebooks/gold/Gold_Build_Aggregate.py"

@pytest.mark.integration
def test_aggregate_kpis_clv_metrics(spark, temp_datalake, run_notebook):
    """
    Tests the exact KPI expressions defined in the contract:
    - Average Order Value (AOV): SUM(TOTAL_AMOUNT) / COUNT(DISTINCT ORDER_ID)
    - Purchase Frequency: COUNT(DISTINCT ORDER_ID) / (DATEDIFF(day, MIN(ORDER_DATE), MAX(ORDER_DATE)) / 30.0)
    """
    spark.sql("CREATE DATABASE IF NOT EXISTS gold")
    
    # Mock Fact Sales
    spark.sql("""
        CREATE OR REPLACE TABLE gold.fact_sales (
            dim_customer_key BIGINT,
            dim_date_key INT,
            order_number STRING,
            line_total DECIMAL(15,2),
            quantity INT
        ) USING DELTA
    """)
    spark.sql("""
        INSERT INTO gold.fact_sales VALUES 
        (1, 20230101, 'ORD-1', 100.00, 1),
        (1, 20230101, 'ORD-1', 50.00, 1),
        (1, 20230302, 'ORD-2', 150.00, 2)
    """)
    
    # Mock Dim Customer
    spark.sql("""
        CREATE OR REPLACE TABLE gold.dim_customer (
            dim_customer_key BIGINT,
            customer_id BIGINT,
            status STRING,
            _valid_from TIMESTAMP,
            registration_date TIMESTAMP
        ) USING DELTA
    """)
    spark.sql("""
        INSERT INTO gold.dim_customer VALUES 
        (1, 1001, 'ACTIVE', '2023-01-01T00:00:00', '2023-01-01T00:00:00')
    """)
    
    # Mock Dim Date
    spark.sql("""
        CREATE OR REPLACE TABLE gold.dim_date (
            dim_date_key INT,
            full_date DATE
        ) USING DELTA
    """)
    spark.sql("""
        INSERT INTO gold.dim_date VALUES 
        (20230101, '2023-01-01'),
        (20230302, '2023-03-02')
    """)
    
    run_notebook(NOTEBOOK_PATH, {
        "pipeline_run_id": "run_agg_001",
        "target_gold_table": "agg_customer_clv_metrics",
        "storage_account": "test_account"
    })
    
    agg_df = spark.read.format("delta").load(f"{temp_datalake}/gold/aggregates/agg_customer_clv_metrics/")
    result = agg_df.filter("dim_customer_key = 1").collect()[0]
    
    # AOV = (100 + 50 + 150) / 2 distinct orders = 300 / 2 = 150.00
    assert float(result["average_order_value"]) == 150.00
    
    # Purchase Frequency = 2 orders / (DATEDIFF('2023-03-02', '2023-01-01') / 30.0)
    # DATEDIFF = 60 days. 60 / 30.0 = 2.0. PF = 2 / 2.0 = 1.0
    assert float(result["purchase_frequency"]) == 1.0
    
    assert float(result["lifetime_revenue"]) == 300.00
    assert result["total_orders"] == 2

@pytest.mark.integration
def test_aggregate_kpis_campaign_roi(spark, temp_datalake, run_notebook, setup_silver_tables):
    """
    Tests the CAC KPI expression:
    - Customer Acquisition Cost (CAC): SUM(TOTAL_SPEND) / NULLIF(SUM(CUSTOMERS_ACQUIRED), 0)
    """
    spark.sql("CREATE DATABASE IF NOT EXISTS gold")
    
    # Mock Fact Sales
    spark.sql("""
        CREATE OR REPLACE TABLE gold.fact_sales (
            dim_campaign_key BIGINT,
            dim_date_key INT,
            order_number STRING,
            line_total DECIMAL(15,2)
        ) USING DELTA
    """)
    spark.sql("INSERT INTO gold.fact_sales VALUES (1, 20230101, 'ORD-1', 500.00)")
    
    # Mock Dim Date
    spark.sql("CREATE OR REPLACE TABLE gold.dim_date (dim_date_key INT, year INT, month_number INT) USING DELTA")
    spark.sql("INSERT INTO gold.dim_date VALUES (20230101, 2023, 1)")
    
    # Mock Dim Campaign
    spark.sql("CREATE OR REPLACE TABLE gold.dim_campaign (dim_campaign_key BIGINT, campaign_id INT, campaign_name STRING, channel STRING) USING DELTA")
    spark.sql("INSERT INTO gold.dim_campaign VALUES (1, 999, 'Summer Promo', 'Social')")
    
    # Mock Silver Marketing Campaigns
    mc_schema = StructType([
        StructField("campaign_id", IntegerType(), True),
        StructField("total_spend", DecimalType(15,2), True),
        StructField("customers_acquired", IntegerType(), True)
    ])
    setup_silver_tables("marketing_campaigns", [(999, 1000.00, 50)], mc_schema)
    
    run_notebook(NOTEBOOK_PATH, {
        "pipeline_run_id": "run_agg_002",
        "target_gold_table": "agg_monthly_campaign_roi",
        "storage_account": "test_account"
    })
    
    agg_df = spark.read.format("delta").load(f"{temp_datalake}/gold/aggregates/agg_monthly_campaign_roi/")
    result = agg_df.filter("dim_campaign_key = 1").collect()[0]
    
    # CAC = 1000.00 spend / 50 acquired = 20.00
    assert float(result["customer_acquisition_cost"]) == 20.00
    
    # ROAS = 500.00 revenue / 1000.00 spend = 0.5
    assert float(result["return_on_ad_spend"]) == 0.5