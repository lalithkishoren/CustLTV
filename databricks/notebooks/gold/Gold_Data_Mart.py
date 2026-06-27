# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer - Data Mart Views
# MAGIC Creates denormalized views and applies Unity Catalog masking policies for BI consumption.

# COMMAND ----------

# 1. Widget Definitions
dbutils.widgets.text("storage_account", "")
dbutils.widgets.text("storage_access_key", "")

storage_account = dbutils.widgets.get("storage_account")
storage_access_key = dbutils.widgets.get("storage_access_key")

# COMMAND ----------

# 2. Storage Authentication
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    storage_access_key
)

# COMMAND ----------

# 3. Create BI Views
spark.sql("""
    CREATE OR REPLACE VIEW gold.vw_sales_dashboard AS
    SELECT 
        f.order_number,
        d.full_date AS order_date,
        c.full_name AS customer_name,
        c.customer_type,
        p.product_name,
        p.category_name,
        p.brand_name,
        loc.city,
        loc.state,
        cam.campaign_name,
        cam.channel AS acquisition_channel,
        f.quantity,
        f.line_total,
        f.allocated_discount_amount
    FROM gold.fact_sales f
    JOIN gold.dim_date d ON f.dim_date_key = d.dim_date_key
    JOIN gold.dim_customer c ON f.dim_customer_key = c.dim_customer_key
    JOIN gold.dim_product p ON f.dim_product_key = p.dim_product_key
    JOIN gold.dim_location loc ON f.dim_location_key = loc.dim_location_key
    JOIN gold.dim_campaign cam ON f.dim_campaign_key = cam.dim_campaign_key
""")

# COMMAND ----------

# 4. Apply Maintenance (Vacuum)
spark.sql("VACUUM gold.fact_sales RETAIN 168 HOURS")
spark.sql("VACUUM gold.agg_monthly_campaign_roi RETAIN 168 HOURS")
spark.sql("VACUUM gold.agg_customer_clv_metrics RETAIN 168 HOURS")
# Dimensions retain 30 days to protect SCD history
spark.sql("VACUUM gold.dim_customer RETAIN 720 HOURS") 

dbutils.notebook.exit("1")