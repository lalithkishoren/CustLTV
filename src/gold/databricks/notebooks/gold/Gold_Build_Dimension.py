# Databricks notebook source
# =================================================================================
# GOLD LAYER: BUILD DIMENSION
# =================================================================================
# Auth handled by Unity Catalog (Managed Identity via Access Connector).
# NO fs.azure.* auth required. Paths use abfss://<layer>@<storage>...
# =================================================================================

dbutils.widgets.text("pipeline_run_id", "")
dbutils.widgets.text("gold_table_id", "")
dbutils.widgets.text("source_silver_tables", "")
dbutils.widgets.text("target_gold_table", "")
dbutils.widgets.text("scd_type", "1")
dbutils.widgets.text("business_key_columns", "")
dbutils.widgets.text("storage_account", "")

pipeline_run_id = dbutils.widgets.get("pipeline_run_id")
source_silver_tables = dbutils.widgets.get("source_silver_tables")
target_gold_table = dbutils.widgets.get("target_gold_table")
scd_type = int(dbutils.widgets.get("scd_type"))
business_key = dbutils.widgets.get("business_key_columns")
storage_account = dbutils.widgets.get("storage_account")

from pyspark.sql.functions import col, lit, current_timestamp, coalesce, max as spark_max, row_number
from pyspark.sql.window import Window
from delta.tables import DeltaTable

# 1. Read Silver Source (Assuming first table in comma-separated list is the base)
base_silver_table = source_silver_tables.split(",")[0]
silver_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/{base_silver_table.split('.')[-1]}/"
df_source = spark.read.format("delta").load(silver_path)

# 2. Define Gold Path
gold_path = f"abfss://gold@{storage_account}.dfs.core.windows.net/dimensions/{target_gold_table}/"
surrogate_key_col = f"{target_gold_table}_key"

# 3. Check if Gold table exists
try:
    gold_table = DeltaTable.forPath(spark, gold_path)
    table_exists = True
except Exception:
    table_exists = False

# 4. Transformation Logic based on LLD
if target_gold_table == 'dim_customer':
    df_transformed = df_source.select(
        col("customer_id"),
        col("email"),
        col("phone"),
        coalesce(col("first_name"), lit("")).alias("first_name"),
        coalesce(col("last_name"), lit("")).alias("last_name"),
        col("gender"),
        col("customer_type"),
        coalesce(col("status"), lit("UNKNOWN")).alias("status"),
        coalesce(col("marketing_opt_in"), lit(False)).alias("marketing_opt_in"),
        col("registration_date"),
        col("last_update_date").alias("_valid_from") # Event time validity
    ).withColumn("full_name", col("first_name") + lit(" ") + col("last_name"))
    
elif target_gold_table == 'dim_product':
    # Join logic for product
    cat_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/categories/"
    brand_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/brands/"
    df_cat = spark.read.format("delta").load(cat_path)
    df_brand = spark.read.format("delta").load(brand_path)
    
    df_transformed = df_source.join(df_cat, "category_id", "left") \
                              .join(df_brand, "brand_id", "left") \
                              .select(
                                  col("inventory_item_id"),
                                  col("sku"),
                                  col("product_name"),
                                  col("category_name"),
                                  col("brand_name"),
                                  col("unit_cost")
                              )
elif target_gold_table == 'dim_location':
    tier_path = f"abfss://silver@{storage_account}.dfs.core.windows.net/city_tier_master/"
    df_tier = spark.read.format("delta").load(tier_path)
    
    df_transformed = df_source.join(df_tier, ["city", "state"], "left") \
                              .select(
                                  col("address_id"),
                                  col("city"),
                                  col("state"),
                                  col("postal_code"),
                                  col("country"),
                                  coalesce(col("tier"), lit("UNKNOWN")).alias("city_tier")
                              )
elif target_gold_table == 'dim_campaign':
    df_transformed = df_source.select(
        col("campaign_id"),
        col("campaign_name"),
        col("channel"),
        col("sub_channel"),
        col("status"),
        col("start_date"),
        col("end_date")
    )
elif target_gold_table == 'dim_registration_source':
    df_transformed = df_source.select(
        col("registration_source_id"),
        col("channel"),
        col("utm_source"),
        col("utm_medium"),
        col("utm_campaign"),
        col("device_type")
    )
else:
    df_transformed = df_source

# Add Audit Columns
df_transformed = df_transformed.withColumn("_pipeline_run_id", lit(pipeline_run_id)) \
                               .withColumn("_last_modified_date", current_timestamp())

# 5. Apply SCD Logic
if not table_exists:
    # Initial Load - Generate SKs
    window_spec = Window.orderBy(business_key)
    df_final = df_transformed.withColumn(surrogate_key_col, row_number().over(window_spec))
    
    if scd_type == 2:
        df_final = df_final.withColumn("_valid_to", lit(None).cast("timestamp")) \
                           .withColumn("_is_current", lit(True)) \
                           .withColumn("_version", lit(1))
                           
    df_final = df_final.withColumn("_created_date", current_timestamp())
    
    # Write Initial Data
    df_final.write.format("delta").mode("overwrite").save(gold_path)
    
    # Insert Unknown Member (-1)
    spark.sql(f"""
        INSERT INTO delta.`{gold_path}` ({surrogate_key_col}, {business_key}, _created_date, _last_modified_date, _pipeline_run_id)
        VALUES (-1, -1, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), '{pipeline_run_id}')
    """)
    
    if target_gold_table == 'dim_campaign':
        # Insert Organic Member (-2)
        spark.sql(f"""
            INSERT INTO delta.`{gold_path}` ({surrogate_key_col}, {business_key}, campaign_name, channel, _created_date, _last_modified_date, _pipeline_run_id)
            VALUES (-2, -2, 'ORGANIC', 'ORGANIC', CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), '{pipeline_run_id}')
        """)
        
    records_processed = df_final.count()
else:
    # Incremental Load
    max_sk = spark.sql(f"SELECT COALESCE(MAX({surrogate_key_col}), 0) FROM delta.`{gold_path}`").collect()[0][0]
    
    if scd_type == 1:
        # SCD Type 1: Overwrite
        df_transformed.createOrReplaceTempView("source_updates")
        
        merge_sql = f"""
            MERGE INTO delta.`{gold_path}` AS target
            USING source_updates AS source
            ON target.{business_key} = source.{business_key}
            WHEN MATCHED THEN UPDATE SET *
            WHEN NOT MATCHED THEN INSERT (
                {surrogate_key_col}, *
            ) VALUES (
                {max_sk} + ROW_NUMBER() OVER (ORDER BY source.{business_key}), *
            )
        """
        spark.sql(merge_sql)
        
    elif scd_type == 2:
        # SCD Type 2: History Tracking
        # Simplified for brevity: In production, this requires a two-pass MERGE or complex join
        # Pass 1: Close old records
        df_transformed.createOrReplaceTempView("source_updates")
        
        spark.sql(f"""
            MERGE INTO delta.`{gold_path}` AS target
            USING source_updates AS source
            ON target.{business_key} = source.{business_key} AND target._is_current = true
            WHEN MATCHED AND (target.status != source.status OR target.customer_type != source.customer_type) THEN
                UPDATE SET target._valid_to = source._valid_from, target._is_current = false
        """)
        
        # Pass 2: Insert new records
        spark.sql(f"""
            INSERT INTO delta.`{gold_path}`
            SELECT 
                {max_sk} + ROW_NUMBER() OVER (ORDER BY s.{business_key}) AS {surrogate_key_col},
                s.*,
                NULL AS _valid_to,
                true AS _is_current,
                COALESCE(t._version, 0) + 1 AS _version,
                CURRENT_TIMESTAMP() AS _created_date
            FROM source_updates s
            LEFT JOIN delta.`{gold_path}` t ON s.{business_key} = t.{business_key} AND t._is_current = false AND t._valid_to = s._valid_from
            WHERE NOT EXISTS (
                SELECT 1 FROM delta.`{gold_path}` active 
                WHERE active.{business_key} = s.{business_key} AND active._is_current = true
            )
        """)
        
    records_processed = df_transformed.count()

# Optimize Z-Order
spark.sql(f"OPTIMIZE delta.`{gold_path}` ZORDER BY ({business_key})")

dbutils.notebook.exit(str(records_processed))