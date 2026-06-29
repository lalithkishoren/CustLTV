# Databricks notebook source
# =================================================================================
# GOLD LAYER: BUILD FACT
# =================================================================================
# Auth handled by Unity Catalog (Managed Identity via Access Connector).
# NO fs.azure.* auth required. Paths use abfss://<layer>@<storage>...
# =================================================================================

dbutils.widgets.text("pipeline_run_id", "")
dbutils.widgets.text("gold_table_id", "")
dbutils.widgets.text("source_silver_tables", "")
dbutils.widgets.text("target_gold_table", "")
dbutils.widgets.text("storage_account", "")

pipeline_run_id = dbutils.widgets.get("pipeline_run_id")
target_gold_table = dbutils.widgets.get("target_gold_table")
storage_account = dbutils.widgets.get("storage_account")

# Register Gold Dimension Paths as Temp Views for Lookups
dim_tables = ['dim_customer', 'dim_product', 'dim_location', 'dim_campaign', 'dim_date']
for dim in dim_tables:
    try:
        spark.read.format("delta").load(f"abfss://gold@{storage_account}.dfs.core.windows.net/dimensions/{dim}/").createOrReplaceTempView(dim)
    except Exception:
        pass # dim_date might be generated separately

# Register Silver Source Paths as Temp Views
silver_tables = dbutils.widgets.get("source_silver_tables").split(",")
for tbl in silver_tables:
    tbl_name = tbl.split('.')[-1]
    spark.read.format("delta").load(f"abfss://silver@{storage_account}.dfs.core.windows.net/{tbl_name}/").createOrReplaceTempView(f"silver_{tbl_name}")

gold_path = f"abfss://gold@{storage_account}.dfs.core.windows.net/facts/{target_gold_table}/"

# Execute specific SQL based on LLD Fact definitions
if target_gold_table == 'fact_sales':
    # Register registration source for campaign lookup
    spark.read.format("delta").load(f"abfss://silver@{storage_account}.dfs.core.windows.net/customer_registration_source/").createOrReplaceTempView("silver_customer_registration_source")
    
    sql_query = f"""
        SELECT 
            CAST(DATE_FORMAT(h.order_date, 'yyyyMMdd') AS INT) AS dim_date_key,
            COALESCE(c.dim_customer_key, -1) AS dim_customer_key,
            COALESCE(p.dim_product_key, -1) AS dim_product_key,
            COALESCE(loc.dim_location_key, -1) AS dim_location_key,
            COALESCE(
                CASE WHEN crs.channel = 'Organic' THEN -2 ELSE cam.dim_campaign_key END, 
                -1
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
            DATE_FORMAT(h.order_date, 'yyyyMM') AS order_year_month,
            CURRENT_TIMESTAMP() AS _created_date,
            CURRENT_TIMESTAMP() AS _last_modified_date,
            '{pipeline_run_id}' AS _pipeline_run_id
        FROM silver_oe_order_lines_all l
        JOIN silver_oe_order_headers_all h ON l.order_id = h.order_id
        LEFT JOIN dim_customer c ON h.customer_id = c.customer_id 
            AND h.order_date >= c._valid_from 
            AND h.order_date < COALESCE(c._valid_to, '2099-12-31')
        LEFT JOIN dim_product p ON l.product_id = p.inventory_item_id
        LEFT JOIN dim_location loc ON h.shipping_address_id = loc.address_id
        LEFT JOIN silver_customer_registration_source crs ON h.customer_id = crs.customer_id
        LEFT JOIN dim_campaign cam ON crs.campaign_id = cam.campaign_id
        WHERE l.line_total >= 0 AND l.quantity > 0 -- DQ Rules DQ-G-F-001, DQ-G-F-002
    """
    partition_col = "order_year_month"
    zorder_cols = "dim_customer_key, dim_product_key"

elif target_gold_table == 'fact_interactions':
    sql_query = f"""
        SELECT 
            CAST(DATE_FORMAT(int.interaction_date, 'yyyyMMdd') AS INT) AS dim_date_key,
            COALESCE(c.dim_customer_key, -1) AS dim_customer_key,
            int.interaction_id,
            inc.incident_id,
            int.interaction_type,
            inc.status AS incident_status,
            inc.priority AS incident_priority,
            1 AS interaction_count,
            DATE_FORMAT(int.interaction_date, 'yyyyMM') AS interaction_year_month,
            CURRENT_TIMESTAMP() AS _created_date,
            CURRENT_TIMESTAMP() AS _last_modified_date,
            '{pipeline_run_id}' AS _pipeline_run_id
        FROM silver_interactions int
        JOIN silver_incidents inc ON int.incident_id = inc.incident_id
        LEFT JOIN dim_customer c ON inc.customer_id = c.customer_id 
            AND int.interaction_date >= c._valid_from 
            AND int.interaction_date < COALESCE(c._valid_to, '2099-12-31')
        WHERE int.interaction_id IS NOT NULL -- DQ-G-F-003
    """
    partition_col = "interaction_year_month"
    zorder_cols = "dim_customer_key"

elif target_gold_table == 'fact_surveys':
    sql_query = f"""
        SELECT 
            CAST(DATE_FORMAT(s.response_date, 'yyyyMMdd') AS INT) AS dim_date_key,
            COALESCE(c.dim_customer_key, -1) AS dim_customer_key,
            s.survey_id,
            s.order_id,
            s.incident_id,
            s.survey_type,
            s.nps_category,
            s.nps_score,
            s.csat_score,
            DATE_FORMAT(s.response_date, 'yyyyMM') AS response_year_month,
            CURRENT_TIMESTAMP() AS _created_date,
            CURRENT_TIMESTAMP() AS _last_modified_date,
            '{pipeline_run_id}' AS _pipeline_run_id
        FROM silver_surveys s
        LEFT JOIN dim_customer c ON s.customer_id = c.customer_id 
            AND s.response_date >= c._valid_from 
            AND s.response_date < COALESCE(c._valid_to, '2099-12-31')
        WHERE (s.nps_score BETWEEN 0 AND 10 OR s.nps_score IS NULL) -- DQ-G-F-004
          AND (s.csat_score BETWEEN 1 AND 5 OR s.csat_score IS NULL) -- DQ-G-F-005
    """
    partition_col = "response_year_month"
    zorder_cols = "dim_customer_key"

# Execute Transformation
df_fact = spark.sql(sql_query)
records_processed = df_fact.count()

# Write to Gold (Partition Overwrite for Idempotency)
spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")

df_fact.write \
    .format("delta") \
    .mode("overwrite") \
    .partitionBy(partition_col) \
    .save(gold_path)

# Optimize Z-Order
spark.sql(f"OPTIMIZE delta.`{gold_path}` ZORDER BY ({zorder_cols})")

dbutils.notebook.exit(str(records_processed))