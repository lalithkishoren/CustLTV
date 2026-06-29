# Databricks notebook source
# =================================================================================
# GOLD LAYER: BUILD AGGREGATE
# =================================================================================
# CRITICAL AUTH POLICY: Authentication is handled entirely by Unity Catalog.
# NO fs.azure.* auth is set here. We simply read/write abfss:// paths.

dbutils.widgets.text("pipeline_run_id", "")
dbutils.widgets.text("gold_table_id", "")
dbutils.widgets.text("target_gold_table", "")
dbutils.widgets.text("storage_account", "")

pipeline_run_id = dbutils.widgets.get("pipeline_run_id")
target_gold_table = dbutils.widgets.get("target_gold_table")
storage_account = dbutils.widgets.get("storage_account")

silver_base = f"abfss://silver@{storage_account}.dfs.core.windows.net"
gold_base = f"abfss://gold@{storage_account}.dfs.core.windows.net"
agg_path = f"{gold_base}/aggregates/{target_gold_table}"

# Register Gold Tables for Aggregation
spark.read.format("delta").load(f"{gold_base}/facts/fact_sales").createOrReplaceTempView("fact_sales")
spark.read.format("delta").load(f"{gold_base}/dimensions/dim_customer").createOrReplaceTempView("dim_customer")
spark.read.format("delta").load(f"{gold_base}/dimensions/dim_campaign").createOrReplaceTempView("dim_campaign")
spark.read.format("delta").load(f"{gold_base}/dimensions/dim_date").createOrReplaceTempView("dim_date")
spark.read.format("delta").load(f"{silver_base}/marketing_campaigns").createOrReplaceTempView("silver_marketing_campaigns")

if target_gold_table == 'agg_monthly_campaign_roi':
    sql_logic = f"""
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
    FROM fact_sales f
    JOIN dim_date d ON f.dim_date_key = d.dim_date_key
    JOIN dim_campaign c ON f.dim_campaign_key = c.dim_campaign_key
    LEFT JOIN silver_marketing_campaigns mc ON c.campaign_id = mc.campaign_id
    GROUP BY 
        d.year * 100 + d.month_number,
        f.dim_campaign_key,
        c.campaign_name,
        c.channel
    """

elif target_gold_table == 'agg_customer_clv_metrics':
    sql_logic = f"""
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
    FROM fact_sales f
    JOIN dim_customer c ON f.dim_customer_key = c.dim_customer_key
    JOIN dim_date d ON f.dim_date_key = d.dim_date_key
    GROUP BY 
        f.dim_customer_key,
        c.customer_id,
        c.status,
        c._valid_from,
        c.registration_date
    """

else:
    raise ValueError(f"Unknown aggregate table: {target_gold_table}")

try:
    # Execute aggregation and overwrite target
    df_agg = spark.sql(sql_logic)
    df_agg.write.format("delta").mode("overwrite").save(agg_path)
    
    # Optimize and Z-Order
    if target_gold_table == 'agg_monthly_campaign_roi':
        spark.sql(f"OPTIMIZE delta.`{agg_path}` ZORDER BY (year_month, dim_campaign_key)")
    elif target_gold_table == 'agg_customer_clv_metrics':
        spark.sql(f"OPTIMIZE delta.`{agg_path}` ZORDER BY (dim_customer_key)")
        
    count = spark.read.format("delta").load(agg_path).count()
    dbutils.notebook.exit(str(count))
    
except Exception as e:
    raise Exception(f"Failed to build aggregate {target_gold_table}: {str(e)}")