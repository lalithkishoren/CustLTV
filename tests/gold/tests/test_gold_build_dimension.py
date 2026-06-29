import pytest
from datetime import datetime
from pyspark.sql import Row
from chispa.dataframe_comparisons import assert_df_equality

@pytest.mark.scd
@pytest.mark.idempotency
def test_dim_customer_scd2_event_time(spark, run_notebook, setup_silver_data, tmp_path):
    """
    SCD Type 2 Test: Validates that dim_customer correctly applies event-time based 
    effective dating (_valid_from, _valid_to) and handles idempotency.
    """
    # 1. Initial Load (Day 1)
    initial_data = [
        Row(customer_id=100, email="a@test.com", phone="123", first_name="John", last_name="Doe", 
            gender="M", customer_type="Retail", status="ACTIVE", marketing_opt_in=True, 
            registration_date=datetime(2023, 1, 1), last_update_date=datetime(2023, 1, 1, 10, 0, 0))
    ]
    setup_silver_data("customers", spark.createDataFrame(initial_data))
    
    widgets = {
        "pipeline_run_id": "run_001",
        "target_gold_table": "dim_customer",
        "storage_account": "test_storage"
    }
    run_notebook("Gold_Build_Dimension.py", widgets)
    
    dim_df = spark.sql("SELECT customer_id, status, _is_current, _version, _valid_from, _valid_to FROM gold.dim_customer WHERE customer_id = 100")
    assert dim_df.count() == 1
    row1 = dim_df.first()
    assert row1["_is_current"] is True
    assert row1["_version"] == 1
    assert row1["_valid_from"] == datetime(2023, 1, 1, 10, 0, 0)
    assert row1["_valid_to"] is None

    # 2. Update Load (Day 2) - Status changes to CHURNED
    updated_data = [
        Row(customer_id=100, email="a@test.com", phone="123", first_name="John", last_name="Doe", 
            gender="M", customer_type="Retail", status="CHURNED", marketing_opt_in=True, 
            registration_date=datetime(2023, 1, 1), last_update_date=datetime(2023, 1, 2, 15, 0, 0))
    ]
    setup_silver_data("customers", spark.createDataFrame(updated_data))
    
    widgets["pipeline_run_id"] = "run_002"
    run_notebook("Gold_Build_Dimension.py", widgets)
    
    dim_df_updated = spark.sql("SELECT customer_id, status, _is_current, _version, _valid_from, _valid_to FROM gold.dim_customer WHERE customer_id = 100 ORDER BY _version")
    assert dim_df_updated.count() == 2
    
    rows = dim_df_updated.collect()
    # Old record closed out
    assert rows[0]["_is_current"] is False
    assert rows[0]["_valid_to"] == datetime(2023, 1, 2, 15, 0, 0)
    # New record active
    assert rows[1]["_is_current"] is True
    assert rows[1]["_version"] == 2
    assert rows[1]["status"] == "CHURNED"
    assert rows[1]["_valid_from"] == datetime(2023, 1, 2, 15, 0, 0)

    # 3. Idempotency Check - Re-run Day 2
    run_notebook("Gold_Build_Dimension.py", widgets)
    dim_df_idempotent = spark.sql("SELECT * FROM gold.dim_customer WHERE customer_id = 100")
    assert dim_df_idempotent.count() == 2 # No new rows created on blind re-run

@pytest.mark.contract
def test_dim_campaign_contract_and_unknown_members(spark, run_notebook, setup_silver_data):
    """
    Contract & Integration Test: Validates SCD1 dimension schema contract and 
    ensures governed Unknown (-1) and Organic (-2) members are injected.
    """
    campaign_data = [
        Row(campaign_id=10, campaign_name="Summer Sale", channel="Email", sub_channel="Newsletter", 
            status="Active", start_date=datetime(2023, 6, 1), end_date=datetime(2023, 8, 31))
    ]
    setup_silver_data("marketing_campaigns", spark.createDataFrame(campaign_data))
    
    widgets = {
        "pipeline_run_id": "run_003",
        "target_gold_table": "dim_campaign",
        "storage_account": "test_storage"
    }
    run_notebook("Gold_Build_Dimension.py", widgets)
    
    # Contract Check
    schema = spark.table("gold.dim_campaign").schema
    expected_columns = {"dim_campaign_key", "campaign_id", "campaign_name", "channel", "_created_date"}
    actual_columns = set(schema.names)
    assert expected_columns.issubset(actual_columns), "Schema contract violation: Missing required columns"
    
    # Unknown Members Check
    unknown_df = spark.sql("SELECT campaign_id, campaign_name FROM gold.dim_campaign WHERE campaign_id IN (-1, -2) ORDER BY campaign_id")
    rows = unknown_df.collect()
    assert len(rows) == 2
    assert rows[0]["campaign_name"] == "ORGANIC" # -2
    assert rows[1]["campaign_name"] == "UNKNOWN" # -1