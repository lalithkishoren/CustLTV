import json
import pytest
from pyspark.sql import Row

NOTEBOOK_PATH = "databricks/notebooks/silver/Silver_Data_Quality.py"

@pytest.mark.spark
@pytest.mark.dq
def test_post_load_dq_warnings(spark, temp_dir, execute_notebook):
    """
    DATA QUALITY: Tests post-load observability checks (Warnings).
    Validates email format regex and referential integrity.
    """
    silver_customers_path = f"{temp_dir}/silver/customers/"
    silver_orders_path = f"{temp_dir}/silver/oe_order_headers_all/"
    
    # Create Silver Customers (1 valid email, 1 invalid email)
    customers_data = [
        Row(customer_id=1, email="valid.email@domain.com"),
        Row(customer_id=2, email="invalid-email-format")
    ]
    spark.createDataFrame(customers_data).write.format("delta").save(silver_customers_path)

    # Create Silver Orders (1 valid FK, 1 orphaned FK)
    orders_data = [
        Row(order_id=100, customer_id=1), # Exists in customers
        Row(order_id=101, customer_id=99) # Orphaned
    ]
    spark.createDataFrame(orders_data).write.format("delta").save(silver_orders_path)

    # Test Customers DQ
    params_cust = {
        "target_silver_table": "customers"
    }
    result_cust = execute_notebook(NOTEBOOK_PATH, params_cust)
    
    assert result_cust["total_records_checked"] == 2
    assert len(result_cust["dq_warnings"]) == 1
    assert result_cust["dq_warnings"][0]["rule_id"] == "DQ-F-001"
    assert result_cust["dq_warnings"][0]["failed_count"] == 1

    # Test Orders DQ
    params_ord = {
        "target_silver_table": "oe_order_headers_all"
    }
    result_ord = execute_notebook(NOTEBOOK_PATH, params_ord)
    
    assert result_ord["total_records_checked"] == 2
    assert len(result_ord["dq_warnings"]) == 1
    assert result_ord["dq_warnings"][0]["rule_id"] == "DQ-RI-001"
    assert result_ord["dq_warnings"][0]["failed_count"] == 1