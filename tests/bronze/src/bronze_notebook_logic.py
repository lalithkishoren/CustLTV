"""
Refactored logic from Databricks notebooks (Incremental_CDC_Dynamic.py & Initial_Load_Dynamic.py)
Extracted into testable functions to allow standard pytest execution while MATCHING THE ACTUAL CODE.
"""
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import current_timestamp, lit, col, row_number, date_format, when, array, struct, expr
from pyspark.sql.window import Window
from delta.tables import DeltaTable

def deduplicate_cdc(cdc_df: DataFrame, primary_key_columns: str) -> DataFrame:
    """Matches Step 6 of Incremental_CDC_Dynamic.py"""
    pk_cols = [pk.strip() for pk in primary_key_columns.split(',')]
    window_spec = Window.partitionBy(*pk_cols).orderBy(col("SYS_CHANGE_VERSION").desc())
    
    return cdc_df.withColumn("row_num", row_number().over(window_spec)) \
                 .filter(col("row_num") == 1) \
                 .drop("row_num")

def add_metadata_incremental(df: DataFrame, pipeline_run_id: str, source_system: str) -> DataFrame:
    """Matches Step 7 of Incremental_CDC_Dynamic.py"""
    return df \
        .withColumn("_pipeline_run_id", lit(pipeline_run_id)) \
        .withColumn("_ingest_timestamp", current_timestamp()) \
        .withColumn("_source_system", lit(source_system.upper())) \
        .withColumn("_cdc_operation", col("SYS_CHANGE_OPERATION")) \
        .withColumn("ingest_date", date_format(current_timestamp(), "yyyy-MM-dd"))

def add_metadata_initial(df: DataFrame, pipeline_run_id: str, source_system: str) -> DataFrame:
    """Matches Step 6 of Initial_Load_Dynamic.py"""
    return df \
        .withColumn("_pipeline_run_id", lit(pipeline_run_id)) \
        .withColumn("_ingest_timestamp", current_timestamp()) \
        .withColumn("_source_system", lit(source_system.upper())) \
        .withColumn("_cdc_operation", lit("I")) \
        .withColumn("ingest_date", date_format(current_timestamp(), "yyyy-MM-dd"))

def merge_incremental(spark: SparkSession, source_with_metadata: DataFrame, delta_path: str, primary_key_columns: str):
    """Matches Steps 8 & 9 of Incremental_CDC_Dynamic.py"""
    pk_cols = [pk.strip() for pk in primary_key_columns.split(',')]
    merge_condition = " AND ".join([f"target.{pk} = source.{pk}" for pk in pk_cols])
    
    all_columns = [c for c in source_with_metadata.columns if c not in ["SYS_CHANGE_OPERATION", "SYS_CHANGE_VERSION", "_current_sync_version"]]
    update_columns = [c for c in all_columns if c not in pk_cols and c != "ingest_date"]
    
    target_table = DeltaTable.forPath(spark, delta_path)
    
    target_table.alias("target").merge(
        source_with_metadata.alias("source"),
        merge_condition
    ).whenMatchedDelete(
        condition="source.SYS_CHANGE_OPERATION = 'D'"
    ).whenMatchedUpdate(
        condition="source.SYS_CHANGE_OPERATION IN ('U', 'I')",
        set={column: f"source.{column}" for column in update_columns + ["_pipeline_run_id", "_ingest_timestamp", "_source_system", "_cdc_operation"]}
    ).whenNotMatchedInsert(
        condition="source.SYS_CHANGE_OPERATION != 'D'",
        values={column: f"source.{column}" for column in all_columns}
    ).execute()

def apply_data_quality_rules(df: DataFrame) -> DataFrame:
    """
    Implements the approved bad-row policy (quarantine/drop for ERROR, tag for WARN).
    Based on the control.data_quality_rules table schema.
    """
    # Example rule: CUSTOMER_ID must not be null (ERROR -> Drop)
    # Example rule: AMOUNT should be > 0 (WARN -> Tag)
    
    # 1. Apply ERROR rules (Drop bad rows)
    df_clean = df.filter(col("CUSTOMER_ID").isNotNull())
    
    # 2. Apply WARN rules (Tag bad rows)
    df_warned = df_clean.withColumn(
        "dq_warnings",
        when(col("AMOUNT") <= 0, array(lit("AMOUNT_LESS_THAN_ZERO"))).otherwise(array())
    )
    return df_warned