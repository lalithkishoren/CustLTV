# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Aggregate Builder
# MAGIC Builds highly optimized aggregate tables for BI consumption and ML feature extraction.

# COMMAND ----------

# 1. Widget Definitions
dbutils.widgets.text("pipeline_run_id", "")
dbutils.widgets.text("gold_table_id", "")
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

gold_path = f"abfss://datalake@{storage_account}.dfs.core.windows.net/gold/aggregates/{target_gold_table}/"

# COMMAND ----------

# 4. Aggregate Processing Logic
if target_gold_table == 'agg_monthly_campaign_roi':
    
    spark.sql(f"""
        CREATE OR REPLACE TABLE {gold_db}.agg_monthly_campaign_roi 
        USING DELTA LOCATION '{gold_path}' AS
        SELECT
            d.year * 100 + d.month_number AS year_month,
            f.dim_campaign_key,
            c.campaign_name,
            c.channel,
            SUM(f.line_total) AS total_revenue,
            COUNT(DISTINCT f.order_number) AS total_orders,
            MAX(mc.total_spend) AS total_spend,
            MAX(mc.customers_acquired) AS customers_acquired,
            CASE 
                WHEN c.channel = 'ORGANIC' THEN 0.00
                ELSE MAX(mc.total_spend) / NULLIF(MAX(mc.customers_acquired), 0) 
            END AS customer_acquisition_cost,
            SUM(f.line_total) / NULLIF(MAX(mc.total_spend), 0) AS return_on_ad_spend,
            CURRENT_TIMESTAMP() AS _created_date,
            '{pipeline_run_id}' AS _pipeline_run_id
        FROM {gold_db}.fact_sales f
        JOIN {gold_db}.dim_date d ON f.dim_date_key = d.dim_date_key
        JOIN {gold_db}.dim_campaign c ON f.dim_campaign_key = c.dim_campaign_key
        LEFT JOIN silver.marketing_campaigns mc ON c.campaign_id = mc.campaign_id
        GROUP BY 
            d.year * 100 + d.month_number,
            f.dim_campaign_key,
            c.campaign_name,
            c.channel
    """)
    
    spark.sql(f"OPTIMIZE {gold_db}.agg_monthly_campaign_roi ZORDER BY (year_month, dim_campaign_key)")

elif target_gold_table == 'agg_customer_clv_metrics':
    
    spark.sql(f"""
        CREATE OR REPLACE TABLE {gold_db}.agg_customer_clv_metrics 
        USING DELTA LOCATION '{gold_path}' AS
        SELECT
            f.dim_customer_key,
            c.customer_id,
            c.status,
            SUM(f.line_total) AS lifetime_revenue,
            COUNT(DISTINCT f.order_number) AS total_orders,
            SUM(f.quantity) AS total_items_purchased,
            MIN(d.full_date) AS first_order_date,
            MAX(d.full_date) AS last_order_date,
            SUM(f.line_total) / NULLIF(COUNT(DISTINCT f.order_number), 0) AS average_order_value,
            COUNT(DISTINCT f.order_number) / (DATEDIFF(MAX(d.full_date), MIN(d.full_date)) / 30.0) AS purchase_frequency,
            DATEDIFF(
                COALESCE(
                    CASE WHEN c.status = 'CHURNED' THEN CAST(c._valid_from AS DATE) ELSE NULL END, 
                    MAX(d.full_date)
                ), 
                CAST(c.registration_date AS DATE)
            ) AS customer_lifespan_days,
            CURRENT_TIMESTAMP() AS _created_date,
            '{pipeline_run_id}' AS _pipeline_run_id
        FROM {gold_db}.fact_sales f
        JOIN {gold_db}.dim_customer c ON f.dim_customer_key = c.dim_customer_key
        JOIN {gold_db}.dim_date d ON f.dim_date_key = d.dim_date_key
        GROUP BY 
            f.dim_customer_key,
            c.customer_id,
            c.status,
            c._valid_from,
            c.registration_date
    """)
    
    spark.sql(f"OPTIMIZE {gold_db}.agg_customer_clv_metrics ZORDER BY (dim_customer_key)")

# COMMAND ----------

# 5. Return Success
dbutils.notebook.exit("1")