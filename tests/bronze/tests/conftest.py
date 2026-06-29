import os
import json
import pytest
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

class NotebookExit(Exception):
    """Exception to simulate dbutils.notebook.exit()"""
    def __init__(self, message):
        self.message = message

class MockWidget:
    def __init__(self):
        self.widgets = {}
    def text(self, name, default_value=""):
        if name not in self.widgets:
            self.widgets[name] = default_value
    def get(self, name):
        return self.widgets.get(name, "")
    def set(self, name, value):
        self.widgets[name] = value

class MockFS:
    def ls(self, path):
        return [f"mock_file_at_{path}"]
    def mount(self, source, mount_point, extra_configs):
        pass
    def mounts(self):
        return []

class MockNotebook:
    def exit(self, value):
        raise NotebookExit(value)

class MockDBUtils:
    def __init__(self):
        self.widgets = MockWidget()
        self.fs = MockFS()
        self.notebook = MockNotebook()

@pytest.fixture(scope="session")
def spark():
    """Session-scoped local Delta-configured SparkSession."""
    builder = SparkSession.builder.appName("BronzeLayerTests") \
        .master("local[*]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
    
    spark_session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield spark_session
    spark_session.stop()

@pytest.fixture(scope="function")
def dbutils_mock():
    return MockDBUtils()

@pytest.fixture(scope="function")
def mock_jdbc(spark):
    """Mocks spark.read.jdbc to prevent actual SQL Server connections during tests."""
    original_jdbc = spark.read.jdbc

    def mocked_jdbc(url, table, properties=None, **kwargs):
        if "CHANGE_TRACKING_CURRENT_VERSION" in table:
            return spark.createDataFrame([(100,)], ["current_version"])
        elif "sp_UpdateTableMetadata" in table:
            return spark.createDataFrame([(1,)], ["success"])
        return spark.createDataFrame([(1,)], ["dummy"])

    spark.read.jdbc = mocked_jdbc
    yield
    spark.read.jdbc = original_jdbc

@pytest.fixture(scope="function")
def run_notebook(spark, dbutils_mock, mock_jdbc, tmp_path):
    """
    Reads a Databricks notebook, patches ABFSS paths to local tmp_path, 
    and executes it in the current Python process to test the actual code.
    """
    def _run(notebook_path, widget_values):
        # Set widgets
        for k, v in widget_values.items():
            dbutils_mock.widgets.set(k, v)
            
        with open(notebook_path, "r") as f:
            code = f.read()
            
        # Patch ABFSS paths to use local pytest tmp_path
        local_base = f"file://{tmp_path}".replace("\\", "/")
        code = code.replace(
            'f"abfss://datalake@{storage_account}.dfs.core.windows.net', 
            f'f"{local_base}'
        )
        
        # Execute the notebook code
        namespace = {
            "spark": spark,
            "dbutils": dbutils_mock,
            "display": lambda x: None
        }
        
        try:
            exec(code, namespace)
            return None
        except NotebookExit as e:
            return json.loads(e.message)
            
    return _run