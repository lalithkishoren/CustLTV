import os
import tempfile
import shutil
import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType, DecimalType, BooleanType, TimestampType, DateType

class DBUtilsWidgetsMock:
    def __init__(self):
        self._widgets = {}
    def text(self, name, default=""):
        if name not in self._widgets:
            self._widgets[name] = default
    def get(self, name):
        return self._widgets.get(name, "")
    def set(self, name, value):
        self._widgets[name] = value

class DBUtilsNotebookMock:
    def exit(self, value):
        raise StopIteration(value)

class DBUtilsMock:
    def __init__(self):
        self.widgets = DBUtilsWidgetsMock()
        self.notebook = DBUtilsNotebookMock()

@pytest.fixture(scope="session")
def spark():
    """Session-scoped local Delta-configured SparkSession."""
    builder = SparkSession.builder \
        .appName("GoldLayerTests") \
        .master("local[*]") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.sql.shuffle.partitions", "1") \
        .config("spark.default.parallelism", "1") \
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
    
    spark_session = builder.getOrCreate()
    yield spark_session
    spark_session.stop()

@pytest.fixture(scope="function")
def temp_datalake():
    """Creates a temporary directory to simulate ADLS Gen2 datalake."""
    temp_dir = tempfile.mkdtemp()
    yield temp_dir
    shutil.rmtree(temp_dir)

@pytest.fixture(scope="function")
def dbutils_mock():
    return DBUtilsMock()

@pytest.fixture(scope="function")
def run_notebook(spark, temp_datalake, dbutils_mock):
    """
    Fixture to execute a Databricks notebook locally by patching the ADLS paths
    to point to the local temporary datalake directory.
    """
    def _run(notebook_path, parameters):
        # Set parameters in mock
        for k, v in parameters.items():
            dbutils_mock.widgets.set(k, v)
            
        with open(notebook_path, 'r') as f:
            code = f.read()
            
        # Remove magic commands
        code = '\n'.join([line for line in code.split('\n') if not line.startswith('# MAGIC')])
        
        # Patch ADLS paths to local temp directory for testing
        # Replaces: abfss://datalake@{storage_account}.dfs.core.windows.net
        adls_pattern = f"abfss://datalake@{parameters.get('storage_account', 'test_account')}.dfs.core.windows.net"
        code = code.replace(adls_pattern, temp_datalake)
        
        # Execute in a controlled namespace
        namespace = {'spark': spark, 'dbutils': dbutils_mock}
        try:
            exec(code, namespace)
        except StopIteration as e:
            # Expected exit from dbutils.notebook.exit()
            assert str(e) == "1"
        except Exception as e:
            raise e
            
    return _run

@pytest.fixture(scope="function")
def setup_silver_tables(spark, temp_datalake):
    """Helper to create empty Silver tables or populate them with test data."""
    def _setup(table_name, data, schema):
        path = f"{temp_datalake}/silver/{table_name}"
        df = spark.createDataFrame(data, schema)
        df.write.format("delta").mode("overwrite").save(path)
        return path
    return _setup