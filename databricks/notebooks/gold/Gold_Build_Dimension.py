# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Dimension Builder
# MAGIC Builds and maintains Slowly Changing Dimensions (SCD1/SCD2) in the Gold layer.

# COMMAND ----------

# 1. Widget Definitions
dbutils.widgets.text("pipeline_run_id", "")
dbutils.widgets.text("gold_table_id", "")
dbutils.widgets.text("source_silver_tables", "")
dbutils.widgets.text("target_gold_table", "")
dbutils.widgets.text("storage_account", "")
dbutils.widgets.text("storage_access_key", "")

pipeline_run_id = dbutils.widgets.get("pipeline_run_id")
target_gold_table = dbutils.widgets.get("target_gold_table")
storage_account = dbutils.widgets.get("storage_account")
storage_access_key = dbutils.widgets.get("storage_access_key")

# COMMAND ----------

# 2. Storage Authentication - REQUIRED for ADLS Gen2 access
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    storage_access_key
)

# COMMAND ----------

# 3. Setup Paths and Database
gold_db = "gold"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {gold_db}")

gold_path = f"abfss://datalake@{storage_account}.dfs.core.windows.net/gold/dimensions/{target_gold_table}/"
silver_base_path = f"abfss://datalake@{storage_account}.dfs.core.windows.net/silver/"

# COMMAND ----------

# 4. Dimension Processing Logic
if target_gold_table == 'dim_customer':
    # SCD Type 2 Dimension
    silver_df = spark.read.format("delta").load(f"{silver_base_path}customers/")
    silver_df.createOrReplaceTempView("src_customers")
    
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {gold_db}.dim_customer (
            dim_customer_key BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
            customer_id BIGINT,
            email STRING,
            phone STRING,
            full_name STRING,
            gender STRING,
            customer_type STRING,
            status STRING,
            marketing_opt_in BOOLEAN,
            registration_date TIMESTAMP,
            _valid_from TIMESTAMP,
            _valid_to TIMESTAMP,
            _is_current BOOLEAN,
            _version INT,
            _created_date TIMESTAMP,
            _last_modified_date TIMESTAMP,
            _pipeline_run_id STRING
        ) USING DELTA LOCATION '{gold_path}'
    """)
    
    # Insert Unknown Member if not exists
    spark.sql(f"""
        INSERT INTO {gold_db}.dim_customer (customer_id, full_name, status, marketing_opt_in, _valid_from, _is_current, _version)
        SELECT -1, 'UNKNOWN', 'UNKNOWN', false, '1900-01-01', true, 1
        WHERE NOT EXISTS (SELECT 1 FROM {gold_db}.dim_customer WHERE customer_id = -1)
    """)
    
    # SCD2 MERGE Logic (Event Time based on last_update_date)
    spark.sql(f"""
        MERGE INTO {gold_db}.dim_customer target
        USING (
            SELECT 
                customer_id,
                email,
                phone,
                CONCAT_WS(' ', first_name, last_name) AS full_name,
                gender,
                customer_type,
                COALESCE(status, 'UNKNOWN') AS status,
                COALESCE(marketing_opt_in, false) AS marketing_opt_in,
                registration_date,
                last_update_date AS _valid_from
            FROM src_customers
        ) source
        ON target.customer_id = source.customer_id
        
        WHEN MATCHED AND target._is_current = true AND (
            target.customer_type != source.customer_type OR 
            target.status != source.status OR 
            target.marketing_opt_in != source.marketing_opt_in
        ) THEN
            UPDATE SET 
                target._valid_to = source._valid_from,
                target._is_current = false,
                target._last_modified_date = CURRENT_TIMESTAMP()
                
        WHEN NOT MATCHED THEN
            INSERT (
                customer_id, email, phone, full_name, gender, customer_type, status, marketing_opt_in, registration_date,
                _valid_from, _valid_to, _is_current, _version, _created_date, _last_modified_date, _pipeline_run_id
            ) VALUES (
                source.customer_id, source.email, source.phone, source.full_name, source.gender, source.customer_type, source.status, source.marketing_opt_in, source.registration_date,
                source._valid_from, NULL, true, 1, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), '{pipeline_run_id}'
            )
    """)
    
    # Insert new versions for updated records
    spark.sql(f"""
        INSERT INTO {gold_db}.dim_customer (
            customer_id, email, phone, full_name, gender, customer_type, status, marketing_opt_in, registration_date,
            _valid_from, _valid_to, _is_current, _version, _created_date, _last_modified_date, _pipeline_run_id
        )
        SELECT 
            s.customer_id, s.email, s.phone, CONCAT_WS(' ', s.first_name, s.last_name), s.gender, s.customer_type, COALESCE(s.status, 'UNKNOWN'), COALESCE(s.marketing_opt_in, false), s.registration_date,
            s.last_update_date, NULL, true, t._version + 1, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), '{pipeline_run_id}'
        FROM src_customers s
        JOIN {gold_db}.dim_customer t ON s.customer_id = t.customer_id
        WHERE t._is_current = false AND t._valid_to = s.last_update_date
    """)

elif target_gold_table == 'dim_product':
    # SCD Type 1 Dimension
    spark.read.format("delta").load(f"{silver_base_path}mtl_system_items_b/").createOrReplaceTempView("src_items")
    spark.read.format("delta").load(f"{silver_base_path}categories/").createOrReplaceTempView("src_categories")
    spark.read.format("delta").load(f"{silver_base_path}brands/").createOrReplaceTempView("src_brands")
    
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {gold_db}.dim_product (
            dim_product_key BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
            inventory_item_id BIGINT,
            sku STRING,
            product_name STRING,
            category_name STRING,
            brand_name STRING,
            unit_cost DECIMAL(15,2),
            _created_date TIMESTAMP,
            _last_modified_date TIMESTAMP,
            _pipeline_run_id STRING
        ) USING DELTA LOCATION '{gold_path}'
    """)
    
    spark.sql(f"""
        INSERT INTO {gold_db}.dim_product (inventory_item_id, sku, product_name)
        SELECT -1, 'UNKNOWN', 'UNKNOWN'
        WHERE NOT EXISTS (SELECT 1 FROM {gold_db}.dim_product WHERE inventory_item_id = -1)
    """)
    
    spark.sql(f"""
        MERGE INTO {gold_db}.dim_product target
        USING (
            SELECT 
                i.inventory_item_id,
                i.sku,
                i.product_name,
                c.category_name,
                b.brand_name,
                i.unit_cost
            FROM src_items i
            LEFT JOIN src_categories c ON i.category_id = c.category_id
            LEFT JOIN src_brands b ON i.brand_id = b.brand_id
        ) source
        ON target.inventory_item_id = source.inventory_item_id
        
        WHEN MATCHED THEN
            UPDATE SET 
                target.sku = source.sku,
                target.product_name = source.product_name,
                target.category_name = source.category_name,
                target.brand_name = source.brand_name,
                target.unit_cost = source.unit_cost,
                target._last_modified_date = CURRENT_TIMESTAMP()
                
        WHEN NOT MATCHED THEN
            INSERT (inventory_item_id, sku, product_name, category_name, brand_name, unit_cost, _created_date, _last_modified_date, _pipeline_run_id)
            VALUES (source.inventory_item_id, source.sku, source.product_name, source.category_name, source.brand_name, source.unit_cost, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), '{pipeline_run_id}')
    """)

elif target_gold_table == 'dim_campaign':
    # SCD Type 1 Dimension with Organic member
    spark.read.format("delta").load(f"{silver_base_path}marketing_campaigns/").createOrReplaceTempView("src_campaigns")
    
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {gold_db}.dim_campaign (
            dim_campaign_key BIGINT GENERATED ALWAYS AS IDENTITY (START WITH 1 INCREMENT BY 1),
            campaign_id INT,
            campaign_name STRING,
            channel STRING,
            sub_channel STRING,
            status STRING,
            start_date DATE,
            end_date DATE,
            _created_date TIMESTAMP,
            _last_modified_date TIMESTAMP,
            _pipeline_run_id STRING
        ) USING DELTA LOCATION '{gold_path}'
    """)
    
    spark.sql(f"""
        INSERT INTO {gold_db}.dim_campaign (campaign_id, campaign_name, channel)
        SELECT -1, 'UNKNOWN', 'UNKNOWN' WHERE NOT EXISTS (SELECT 1 FROM {gold_db}.dim_campaign WHERE campaign_id = -1)
    """)
    spark.sql(f"""
        INSERT INTO {gold_db}.dim_campaign (campaign_id, campaign_name, channel)
        SELECT -2, 'ORGANIC', 'ORGANIC' WHERE NOT EXISTS (SELECT 1 FROM {gold_db}.dim_campaign WHERE campaign_id = -2)
    """)
    
    spark.sql(f"""
        MERGE INTO {gold_db}.dim_campaign target
        USING src_campaigns source
        ON target.campaign_id = source.campaign_id
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT *
    """)

elif target_gold_table == 'dim_date':
    # Static Date Dimension Generation
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {gold_db}.dim_date (
            dim_date_key INT,
            full_date DATE,
            day_of_week INT,
            day_name STRING,
            day_of_month INT,
            day_of_year INT,
            week_of_year INT,
            month_number INT,
            month_name STRING,
            quarter INT,
            year INT,
            is_weekend BOOLEAN,
            is_holiday BOOLEAN
        ) USING DELTA LOCATION '{gold_path}'
    """)
    
    spark.sql(f"""
        INSERT OVERWRITE {gold_db}.dim_date
        SELECT 
            CAST(date_format(d, 'yyyyMMdd') AS INT) AS dim_date_key,
            CAST(d AS DATE) AS full_date,
            dayofweek(d) AS day_of_week,
            date_format(d, 'EEEE') AS day_name,
            day(d) AS day_of_month,
            dayofyear(d) AS day_of_year,
            weekofyear(d) AS week_of_year,
            month(d) AS month_number,
            date_format(d, 'MMMM') AS month_name,
            quarter(d) AS quarter,
            year(d) AS year,
            CASE WHEN dayofweek(d) IN (1, 7) THEN true ELSE false END AS is_weekend,
            false AS is_holiday
        FROM (
            SELECT explode(sequence(to_date('2015-01-01'), to_date('2030-12-31'), interval 1 day)) AS d
        )
    """)

# COMMAND ----------

# 5. Optimize and Z-Order
if target_gold_table == 'dim_customer':
    spark.sql(f"OPTIMIZE {gold_db}.dim_customer ZORDER BY (customer_id)")
elif target_gold_table == 'dim_product':
    spark.sql(f"OPTIMIZE {gold_db}.dim_product ZORDER BY (inventory_item_id, category_name)")

# COMMAND ----------

# 6. Return Success
dbutils.notebook.exit("1")