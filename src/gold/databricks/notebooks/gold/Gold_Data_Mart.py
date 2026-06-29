# Databricks notebook source
# =================================================================================
# GOLD LAYER: DATA MART VIEWS
# =================================================================================
# CRITICAL AUTH POLICY: Authentication is handled entirely by Unity Catalog.
# NO fs.azure.* auth is set here. We simply read/write abfss:// paths.

dbutils.widgets.text("storage_account", "")
storage_account = dbutils.widgets.get("storage_account")

gold_base = f"abfss://gold@{storage_account}.dfs.core.windows.net"

# Create Unity Catalog Views for BI Consumption
# Assuming catalog 'gold' and schema 'marts' exist

spark.sql(f"""
CREATE OR REPLACE VIEW gold.marts.vw_customer_360 AS
SELECT 
    c.customer_id,
    c.full_name,
    c.email,
    c.customer_type,
    c.status,
    a.lifetime_revenue,
    a.total_orders,
    a.average_order_value,
    a.customer_lifespan_days,
    a.first_order_date,
    a.last_order_date
FROM delta.`{gold_base}/dimensions/dim_customer` c
LEFT JOIN delta.`{gold_base}/aggregates/agg_customer_clv_metrics` a 
    ON c.dim_customer_key = a.dim_customer_key
WHERE c._is_current = true
""")

spark.sql(f"""
CREATE OR REPLACE VIEW gold.marts.vw_marketing_performance AS
SELECT 
    year_month,
    campaign_name,
    channel,
    total_revenue,
    total_spend,
    customers_acquired,
    customer_acquisition_cost,
    return_on_ad_spend
FROM delta.`{gold_base}/aggregates/agg_monthly_campaign_roi`
ORDER BY year_month DESC
""")

dbutils.notebook.exit("SUCCESS")