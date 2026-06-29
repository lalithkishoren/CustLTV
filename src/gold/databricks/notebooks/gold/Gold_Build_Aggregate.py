# Databricks notebook source
# =================================================================================
# GOLD LAYER: BUILD AGGREGATE
# =================================================================================
# Auth handled by Unity Catalog (Managed Identity via Access Connector).
# NO fs.azure.* auth required. Paths use abfss://<layer>@<storage>...
# =================================================================================

dbutils.widgets.text("pipeline_run_id", "")
dbutils.widgets.text("gold_table_id", "")
dbutils.widgets.text("target_gold_table", "")
dbutils.widgets.text("storage_account", "")

pipeline_run_id = dbutils.widgets.get("pipeline_run_id")
target_gold_table = dbutils.widgets.get("target_gold_table")
storage_account = dbutils.widgets.get("storage_account")

# Register Gold Tables as Temp Views
gold_tables = ['fact_sales', 'dim_customer', 'dim_campaign', 'dim_date']
for tbl in gold_tables:
    try:
        folder = "facts" if "fact" in tbl else "dimensions"
        spark.read.format("delta").load(f"abfss://gold@{storage_account}.dfs.core.windows.net/{folder}/{tbl}/").createOrReplaceTempView(f"gold_{tbl}")
    except Exception:
        pass

# Register Silver Marketing Campaigns for CAC
spark.read.format("delta").load(f"abfss://silver@{storage_account}.dfs.core.windows.net/marketing_campaigns/").createOrReplaceTempView("silver_marketing_campaigns")

gold_path = f"abfss://gold@{storage_account}.dfs.core.windows.net/aggregates/{target_gold_table}/"

# Execute specific SQL based on LLD Aggregate definitions
if target_gold_table == 'agg_monthly_campaign_roi':
    sql_query = f"""
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
        FROM gold_fact_sales f
        JOIN gold_dim_date d ON f.dim_date_key = d.dim_date_key
        JOIN gold_dim_campaign c ON f.dim_campaign_key = c.dim_campaign_key
        LEFT JOIN silver_marketing_campaigns mc ON c.campaign_id = mc.campaign_id
        GROUP BY 
            d.year * 100 + d.month_number,
            f.dim_campaign_key,
            c.campaign_name,
            c.channel
    """
    zorder_cols = "year_month, dim_campaign_key"

elif target_gold_table == 'agg_customer_clv_metrics':
    sql_query = f"""
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
            DATEDIFF(
                COALESCE(
                    CASE WHEN c.status = 'CHURNED' THEN CAST(c._valid_from AS DATE) ELSE NULL END, 
                    MAX(d.full_date)
                ), 
                CAST(c.registration_date AS DATE)
            ) AS customer_lifespan_days,
            CURRENT_TIMESTAMP() AS _created_date,
            '{pipeline_run_id}' AS _pipeline_run_id
        FROM gold_fact_sales f
        JOIN gold_dim_customer c ON f.dim_customer_key = c.dim_customer_key
        JOIN gold_dim_date d ON f.dim_date_key = d.dim_date_key
        GROUP BY 
            f.dim_customer_key,
            c.customer_id,
            c.status,
            c._valid_from,
            c.registration_date
    """
    zorder_cols = "dim_customer_key"

# Execute Transformation
df_agg = spark.sql(sql_query)
records_processed = df_agg.count()

# Write to Gold (Full Overwrite for Aggregates)
df_agg.write \
    .format("delta") \
    .mode("overwrite") \
    .save(gold_path)

# Optimize Z-Order
spark.sql(f"OPTIMIZE delta.`{gold_path}` ZORDER BY ({zorder_cols})")

dbutils.notebook.exit(str(records_processed))