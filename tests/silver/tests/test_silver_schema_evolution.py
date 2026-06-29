import json
import pytest
from pyspark.sql import Row

NOTEBOOK_PATH = "databricks/notebooks/silver/Silver_Schema_Evolution.py"

@pytest.mark.spark
def test_schema_drift_detection(spark, temp_dir, execute_notebook):
    """
    CONTRACT: Tests that intentional schema evolution policy is enforced.
    Detects new columns in Bronze that are missing in Silver.
    """
    bronze_path = f"{temp_dir}/bronze/src-001/erp/brands/"
    silver_path = f"{temp_dir}/silver/brands/"
    
    # Bronze has an extra column 'new_brand_category'
    bronze_data = [Row(brand_id=1, brand_name="Acme", new_brand_category="Premium")]
    spark.createDataFrame(bronze_data).write.format("delta").save(bronze_path)

    # Silver is missing the new column
    silver_data = [Row(brand_id=1, brand_name="Acme")]
    spark.createDataFrame(silver_data).write.format("delta").save(silver_path)

    params = {
        "source_bronze_table": "src-001/erp/brands",
        "target_silver_table": "brands"
    }

    result = execute_notebook(NOTEBOOK_PATH, params)

    assert result["schema_drift_detected"] is True
    assert "new_brand_category" in result["new_columns"]