# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Fact Builder
# MAGIC Builds Fact tables with dimension lookups, measure calculations, and data quality enforcement.

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

# 2. Storage Authentication
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    storage_access_key
)

# COMMAND ----------

# 3. Setup Paths and Database
gold_db = "gold"
spark.sql(f"CREATE DATABASE IF NOT EXISTS {gold_db}")

gold_path = f"abfss://datalake@{storage_account}.dfs.core.windows.net/gold/facts/{target_gold_table}/"
silver_base_path = f"abfss://datalake@{storage_account}.dfs.core.windows.net/silver/"

# COMMAND ----------

# 4. Fact Processing Logic
if target_gold_table == 'fact_sales':
    
    # Read Silver Sources
    spark.read.format("delta").load(f"{silver_base_path}oe_order_lines_all/").createOrReplaceTempView("src_lines")
    spark.read.format("delta").load(f"{silver_base_path}oe_order_headers_all/").createOrReplaceTempView("src_headers")
    spark.read.format("delta").load(f"{silver_base_path}customer_registration_source/").createOrReplaceTempView("src_crs")
    
    # Create Fact Table
    spark.sql(f"""
        CREATE TABLE IF NOT EXISTS {gold_db}.fact_sales (
            dim_date_key INT NOT NULL,
            dim_customer_key BIGINT NOT NULL,
            dim_product_key BIGINT NOT NULL,
            dim_location_key BIGINT NOT NULL,
            dim_campaign_key BIGINT NOT NULL,
            order_id BIGINT,
            order_number STRING,
            line_id BIGINT,
            order_status STRING,
            quantity INT,
            line_total DECIMAL(15,2),
            allocated_discount_amount DECIMAL(15,2),
            allocated_tax_amount DECIMAL(15,2),
            unit_price DECIMAL(15,2),
            order_year_month INT,
            _created_date TIMESTAMP,
            _last_modified_date TIMESTAMP,
            _pipeline_run_id STRING
        ) USING DELTA 
        PARTITIONED BY (order_year_month)
        LOCATION '{gold_path}'
    """)
    
    # Process Fact with Lookups and DQ Rules
    spark.sql(f"""
        MERGE INTO {gold_db}.fact_sales target
        USING (
            SELECT 
                CAST(DATE_FORMAT(h.order_date, 'yyyyMMdd') AS INT) AS dim_date_key,
                COALESCE(c.dim_customer_key, -1) AS dim_customer_key,
                COALESCE(p.dim_product_key, -1) AS dim_product_key,
                COALESCE(loc.dim_location_key, -1) AS dim_location_key,
                COALESCE(
                    cam.dim_campaign_key, 
                    CASE WHEN crs.channel = 'Organic' THEN -2 ELSE -1 END
                ) AS dim_campaign_key,
                h.order_id,
                h.order_number,
                l.line_id,
                h.order_status,
                l.quantity,
                l.line_total,
                (l.line_total / NULLIF(h.subtotal_amount, 0)) * h.discount_amount AS allocated_discount_amount,
                (l.line_total / NULLIF(h.subtotal_amount, 0)) * h.tax_amount AS allocated_tax_amount,
                l.unit_price,
                CAST(DATE_FORMAT(h.order_date, 'yyyyMM') AS INT) AS order_year_month
            FROM src_lines l
            JOIN src_headers h ON l.order_id = h.order_id
            LEFT JOIN {gold_db}.dim_customer c 
                ON h.customer_id = c.customer_id 
                AND h.order_date >= c._valid_from 
                AND h.order_date < COALESCE(c._valid_to, '2099-12-31')
            LEFT JOIN {gold_db}.dim_product p ON l.product_id = p.inventory_item_id
            LEFT JOIN {gold_db}.dim_location loc ON h.shipping_address_id = loc.address_id
            LEFT JOIN src_crs crs ON h.customer_id = crs.customer_id
            LEFT JOIN {gold_db}.dim_campaign cam ON crs.campaign_id = cam.campaign_id
            WHERE l.line_total >= 0 AND l.quantity > 0 -- DQ Rules DQ-G-F-001, DQ-G-F-002
        ) source
        ON target.line_id = source.line_id
        
        WHEN MATCHED THEN UPDATE SET *
        WHEN NOT MATCHED THEN INSERT (
            dim_date_key, dim_customer_key, dim_product_key, dim_location_key, dim_campaign_key,
            order_id, order_number, line_id, order_status, quantity, line_total, 
            allocated_discount_amount, allocated_tax_amount, unit_price, order_year_month,
            _created_date, _last_modified_date, _pipeline_run_id
        ) VALUES (
            source.dim_date_key, source.dim_customer_key, source.dim_product_key, source.dim_location_key, source.dim_campaign_key,
            source.order_id, source.order_number, source.line_id, source.order_status, source.quantity, source.line_total, 
            source.allocated_discount_amount, source.allocated_tax_amount, source.unit_price, source.order_year_month,
            CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), '{pipeline_run_id}'
        )
    """)

# COMMAND ----------

# 5. Optimize and Z-Order
if target_gold_table == 'fact_sales':
    spark.sql(f"OPTIMIZE {gold_db}.fact_sales ZORDER BY (dim_customer_key, dim_product_key)")

# COMMAND ----------

# 6. Return Success
dbutils.notebook.exit("1")