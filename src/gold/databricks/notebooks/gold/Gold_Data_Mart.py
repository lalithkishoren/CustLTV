# Databricks notebook source
# =================================================================================
# GOLD LAYER: DATA MART VIEWS
# =================================================================================
# Auth handled by Unity Catalog (Managed Identity via Access Connector).
# NO fs.azure.* auth required. Paths use abfss://<layer>@<storage>...
# =================================================================================

dbutils.widgets.text("storage_account", "")
storage_account = dbutils.widgets.get("storage_account")

# Create Unity Catalog Views for BI Consumption
# Assuming catalog 'gold' and schema 'marts' exist

spark.sql(f"""
CREATE OR REPLACE VIEW gold.marts.vw_customer_churn_features AS
SELECT 
    c.customer_id,
    c.status,
    c.customer_type,
    a.lifetime_revenue,
    a.average_order_value,
    a.customer_lifespan_days,
    a.total_orders,
    (a.total_orders / NULLIF((a.customer_lifespan_days / 30.0), 0)) AS purchase_frequency_monthly
FROM delta.`abfss://gold@{storage_account}.dfs.core.windows.net/aggregates/agg_customer_clv_metrics/` a
JOIN delta.`abfss://gold@{storage_account}.dfs.core.windows.net/dimensions/dim_customer/` c 
  ON a.dim_customer_key = c.dim_customer_key
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
FROM delta.`abfss://gold@{storage_account}.dfs.core.windows.net/aggregates/agg_monthly_campaign_roi/`
ORDER BY year_month DESC
""")

dbutils.notebook.exit("SUCCESS")