import os
import re
import json
import shutil
import tempfile
import pytest
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from delta.tables import DeltaTable

class NotebookExit(Exception):
    """Exception to simulate dbutils.notebook.exit()"""
    def __init__(self, result):
        self.result = result

class MockWidgets:
    def __init__(self):
        self.params = {}
    def text(self, name, default=""):
        if name not in self.params:
            self.params[name] = default
    def get(self, name):
        return self.params.get(name, "")
    def set_param(self, name, value):
        self.params[name] = value

class MockNotebook:
    def exit(self, result):
        raise NotebookExit(result)

class MockDBUtils:
    def __init__(self):
        self.widgets = MockWidgets()
        self.notebook = MockNotebook()

@pytest.fixture(scope="session")
def spark():
    """Session-scoped local SparkSession configured for Delta Lake."""
    builder = SparkSession.builder.appName("SilverLayerTestHarness") \
        .master("local[*]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.default.parallelism", "1") \
        .config("spark.databricks.delta.schema.autoMerge.enabled", "false")

    spark_session = configure_spark_with_delta_pip(builder).getOrCreate()
    yield spark_session
    spark_session.stop()

@pytest.fixture(scope="function")
def temp_dir():
    """Provides a clean temporary directory for each test."""
    path = tempfile.mkdtemp()
    yield path
    shutil.rmtree(path)

@pytest.fixture(scope="function")
def dbutils_mock():
    return MockDBUtils()

@pytest.fixture(scope="function")
def execute_notebook(spark, temp_dir, dbutils_mock):
    """
    Executes a Databricks notebook locally by mocking dbutils and replacing 
    hardcoded ABFSS paths with local temporary directories.
    """
    def _run(notebook_path, params):
        # Set widget parameters
        for k, v in params.items():
            dbutils_mock.widgets.set_param(k, v)
            
        with open(notebook_path, 'r') as f:
            code = f.read()
            
        # Mock storage paths to use local temp_dir instead of abfss
        code = re.sub(
            r'f"abfss://datalake@\{storage_account\}\.dfs\.core\.windows\.net/',
            f'f"{temp_dir}/',
            code
        )
        # Remove spark.conf.set for azure keys as it's not needed locally
        code = re.sub(r'spark\.conf\.set\([^)]+\)', 'pass', code)
        
        # Setup execution environment
        global_env = {
            'spark': spark,
            'dbutils': dbutils_mock,
            'DeltaTable': DeltaTable,
            'json': json
        }
        
        try:
            exec(code, global_env)
            return None
        except NotebookExit as e:
            return json.loads(e.result)
            
    return _run