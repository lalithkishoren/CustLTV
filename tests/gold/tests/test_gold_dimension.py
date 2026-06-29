import pytest
from pyspark.sql.types import *
from chispa.dataframe_comparisons import assert_df_equality
from datetime import datetime

NOTEBOOK_PATH = "databricks/notebooks/gold/Gold_Build_Dimension.py"

@pytest.mark.scd
@pytest.mark.idempotency
def test_dim_customer_scd2_and_idempotency(spark, temp_datalake, run_notebook, setup_silver_tables):
    """
    Tests SCD Type 2 logic (event time validity, is_current flags) and Idempotency.
    """
    schema = StructType([
        StructField("customer_id", LongType(), True),
        StructField("email", StringType(), True),
        StructField("phone", StringType(), True),
        StructField("first_name", StringType(), True),
        StructField("last_name", StringType(), True),
        StructField("gender", StringType(), True),
        StructField("customer_type", StringType(), True),
        StructField("status", StringType(), True),
        StructField("marketing_opt_in", BooleanType(), True),
        StructField("registration_date", TimestampType(), True),
        StructField("last_update_date", TimestampType(), True)
    ])
    
    # 1. Initial Load
    data_v1 = [(1001, "test@test.com", "555-0000", "John", "Doe", "M", "Retail", "ACTIVE", True, datetime(2023, 1, 1), datetime(2023, 1, 1))]
    setup_silver_tables("customers", data_v1, schema)
    
    params = {
        "pipeline_run_id": "run_001",
        "target_gold_table": "dim_customer",
        "storage_account": "test_account",
        "storage_access_key": "dummy"
    }
    
    run_notebook(NOTEBOOK_PATH, params)
    
    gold_df = spark.read.format("delta").load(f"{temp_datalake}/gold/dimensions/dim_customer/")
    
    # Verify Unknown Member (-1) and Initial Record
    assert gold_df.count() == 2
    john_v1 = gold_df.filter("customer_id = 1001").collect()[0]
    assert john_v1["_is_current"] == True
    assert john_v1["_version"] == 1
    assert john_v1["_valid_to"] is None
    
    # 2. Idempotency Check (Run again with same data)
    run_notebook(NOTEBOOK_PATH, params)
    gold_df_idempotent = spark.read.format("delta").load(f"{temp_datalake}/gold/dimensions/dim_customer/")
    assert gold_df_idempotent.count() == 2 # No duplicates created
    
    # 3. SCD Type 2 Update (Change status and customer_type)
    data_v2 = [(1001, "test@test.com", "555-0000", "John", "Doe", "M", "Wholesale", "INACTIVE", True, datetime(2023, 1, 1), datetime(2023, 2, 1))]
    setup_silver_tables("customers", data_v2, schema)
    
    params["pipeline_run_id"] = "run_002"
    run_notebook(NOTEBOOK_PATH, params)
    
    gold_df_updated = spark.read.format("delta").load(f"{temp_datalake}/gold/dimensions/dim_customer/")
    
    # Should now have 3 records: Unknown, John v1 (historical), John v2 (current)
    assert gold_df_updated.count() == 3
    
    history = gold_df_updated.filter("customer_id = 1001").orderBy("_version").collect()
    
    # Assert v1 is closed out
    assert history[0]["_is_current"] == False
    assert history[0]["_valid_to"] == datetime(2023, 2, 1)
    assert history[0]["customer_type"] == "Retail"
    
    # Assert v2 is current
    assert history[1]["_is_current"] == True
    assert history[1]["_valid_to"] is None
    assert history[1]["customer_type"] == "Wholesale"
    assert history[1]["status"] == "INACTIVE"

@pytest.mark.contract
def test_dim_campaign_contract_and_organic_member(spark, temp_datalake, run_notebook, setup_silver_tables):
    """
    Tests SCD Type 1 contract and the explicit inclusion of the Organic member (-2).
    """
    schema = StructType([
        StructField("campaign_id", IntegerType(), True),
        StructField("campaign_name", StringType(), True),
        StructField("channel", StringType(), True),
        StructField("sub_channel", StringType(), True),
        StructField("status", StringType(), True),
        StructField("start_date", DateType(), True),
        StructField("end_date", DateType(), True)
    ])
    
    setup_silver_tables("marketing_campaigns", [], schema)
    
    run_notebook(NOTEBOOK_PATH, {
        "pipeline_run_id": "run_001",
        "target_gold_table": "dim_campaign",
        "storage_account": "test_account"
    })
    
    gold_df = spark.read.format("delta").load(f"{temp_datalake}/gold/dimensions/dim_campaign/")
    
    # Contract Validation
    expected_columns = {"dim_campaign_key", "campaign_id", "campaign_name", "channel", "sub_channel", "status", "start_date", "end_date", "_created_date", "_last_modified_date", "_pipeline_run_id"}
    assert set(gold_df.columns) == expected_columns
    
    # Verify Special Members
    members = gold_df.select("campaign_id", "campaign_name").orderBy("campaign_id").collect()
    assert members[0]["campaign_id"] == -2
    assert members[0]["campaign_name"] == "ORGANIC"
    assert members[1]["campaign_id"] == -1
    assert members[1]["campaign_name"] == "UNKNOWN"