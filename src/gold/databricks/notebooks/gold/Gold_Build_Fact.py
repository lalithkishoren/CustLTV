# Databricks notebook source
# =================================================================================
# GOLD LAYER: BUILD FACT
# =================================================================================
# CRITICAL AUTH POLICY: Authentication is handled entirely by Unity Catalog.
# NO fs.azure.* auth is set here. We simply read/write abfss:// paths.

dbutils.widgets.text("pipeline_run_id", "")
dbutils.widgets.text("gold_table_id", "")
dbutils.widgets.text("source_silver_tables", "")
dbutils.widgets.text("target_gold_table", "")
dbutils.widgets.text("storage_account", "")

pipeline_run_id = dbutils.widgets.get("pipeline_run_id")
target_gold_table = dbutils.widgets.get("target_gold_table")
storage_account = dbutils.widgets.get("storage_account")

silver_base = f"abfss://silver@{storage_account}.dfs.core.windows.net"
gold_base = f"abfss://gold@{storage_account}.dfs.core.windows.net"
fact_path = f"{gold_base}/facts/{target_gold_table}"

# Register Gold Dimensions for Lookups
spark.read.format("delta").load(f"{gold_base}/dimensions/dim_customer").createOrReplaceTempView("dim_customer")
spark.read.format("delta").load(f"{gold_base}/dimensions/dim_product").createOrReplaceTempView("dim_product")
spark.read.format("delta").load(f"{gold_base}/dimensions/dim_location").createOrReplaceTempView("dim_location")
spark.read.format("delta").load(f"{gold_base}/dimensions/dim_campaign").createOrReplaceTempView("dim_campaign")

if target_gold_table == 'fact_sales':
    spark.read.format("delta").load(f"{silver_base}/oe_order_lines_all").createOrReplaceTempView("silver_lines")
    spark.read.format("delta").load(f"{silver_base}/oe_order_headers_all").createOrReplaceTempView("silver_headers")
    spark.read.format("delta").load(f"{silver_base}/customer_registration_source").createOrReplaceTempView("silver_crs")
    
    sql_logic = f"""
    MERGE INTO delta.`{fact_path}` AS target
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
            DATE_FORMAT(h.order_date, 'yyyy-MM') AS order_year_month
        FROM silver_lines l
        JOIN silver_headers h ON l.order_id = h.order_id
        LEFT JOIN dim_customer c ON h.customer_id = c.customer_id 
            AND h.order_date >= c._valid_from 
            AND h.order_date < COALESCE(c._valid_to, '2099-12-31')
        LEFT JOIN dim_product p ON l.product_id = p.inventory_item_id
        LEFT JOIN dim_location loc ON h.shipping_address_id = loc.address_id
        LEFT JOIN silver_crs crs ON h.customer_id = crs.customer_id
        LEFT JOIN dim_campaign cam ON crs.campaign_id = cam.campaign_id
        WHERE l.line_total >= 0 AND l.quantity > 0 -- DQ Rules DQ-G-F-001, DQ-G-F-002
    ) AS source
    ON target.line_id = source.line_id
    
    WHEN MATCHED THEN UPDATE SET 
        target.dim_date_key = source.dim_date_key,
        target.dim_customer_key = source.dim_customer_key,
        target.dim_product_key = source.dim_product_key,
        target.dim_location_key = source.dim_location_key,
        target.dim_campaign_key = source.dim_campaign_key,
        target.order_status = source.order_status,
        target.quantity = source.quantity,
        target.line_total = source.line_total,
        target.allocated_discount_amount = source.allocated_discount_amount,
        target.allocated_tax_amount = source.allocated_tax_amount,
        target.unit_price = source.unit_price,
        target._last_modified_date = CURRENT_TIMESTAMP()
        
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
    """

elif target_gold_table == 'fact_interactions':
    spark.read.format("delta").load(f"{silver_base}/interactions").createOrReplaceTempView("silver_interactions")
    spark.read.format("delta").load(f"{silver_base}/incidents").createOrReplaceTempView("silver_incidents")
    
    sql_logic = f"""
    MERGE INTO delta.`{fact_path}` AS target
    USING (
        SELECT 
            CAST(DATE_FORMAT(int.interaction_date, 'yyyyMMdd') AS INT) AS dim_date_key,
            COALESCE(c.dim_customer_key, -1) AS dim_customer_key,
            int.interaction_id,
            inc.incident_id,
            int.interaction_type,
            inc.status AS incident_status,
            inc.priority AS incident_priority,
            1 AS interaction_count,
            DATE_FORMAT(int.interaction_date, 'yyyy-MM') AS interaction_year_month
        FROM silver_interactions int
        JOIN silver_incidents inc ON int.incident_id = inc.incident_id
        LEFT JOIN dim_customer c ON inc.customer_id = c.customer_id 
            AND int.interaction_date >= c._valid_from 
            AND int.interaction_date < COALESCE(c._valid_to, '2099-12-31')
        WHERE int.interaction_id IS NOT NULL -- DQ-G-F-003
    ) AS source
    ON target.interaction_id = source.interaction_id
    
    WHEN MATCHED THEN UPDATE SET 
        target.incident_status = source.incident_status,
        target.incident_priority = source.incident_priority,
        target._last_modified_date = CURRENT_TIMESTAMP()
        
    WHEN NOT MATCHED THEN INSERT (
        dim_date_key, dim_customer_key, interaction_id, incident_id, interaction_type,
        incident_status, incident_priority, interaction_count, interaction_year_month,
        _created_date, _last_modified_date, _pipeline_run_id
    ) VALUES (
        source.dim_date_key, source.dim_customer_key, source.interaction_id, source.incident_id, source.interaction_type,
        source.incident_status, source.incident_priority, source.interaction_count, source.interaction_year_month,
        CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), '{pipeline_run_id}'
    )
    """

elif target_gold_table == 'fact_surveys':
    spark.read.format("delta").load(f"{silver_base}/surveys").createOrReplaceTempView("silver_surveys")
    
    sql_logic = f"""
    MERGE INTO delta.`{fact_path}` AS target
    USING (
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
            DATE_FORMAT(s.response_date, 'yyyy-MM') AS response_year_month
        FROM silver_surveys s
        LEFT JOIN dim_customer c ON s.customer_id = c.customer_id 
            AND s.response_date >= c._valid_from 
            AND s.response_date < COALESCE(c._valid_to, '2099-12-31')
        WHERE (s.nps_score BETWEEN 0 AND 10 OR s.nps_score IS NULL) -- DQ-G-F-004
          AND (s.csat_score BETWEEN 1 AND 5 OR s.csat_score IS NULL) -- DQ-G-F-005
    ) AS source
    ON target.survey_id = source.survey_id
    
    WHEN MATCHED THEN UPDATE SET 
        target.nps_category = source.nps_category,
        target.nps_score = source.nps_score,
        target.csat_score = source.csat_score,
        target._last_modified_date = CURRENT_TIMESTAMP()
        
    WHEN NOT MATCHED THEN INSERT (
        dim_date_key, dim_customer_key, survey_id, order_id, incident_id, survey_type,
        nps_category, nps_score, csat_score, response_year_month,
        _created_date, _last_modified_date, _pipeline_run_id
    ) VALUES (
        source.dim_date_key, source.dim_customer_key, source.survey_id, source.order_id, source.incident_id, source.survey_type,
        source.nps_category, source.nps_score, source.csat_score, source.response_year_month,
        CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), '{pipeline_run_id}'
    )
    """

else:
    raise ValueError(f"Unknown fact table: {target_gold_table}")

try:
    spark.sql(sql_logic)
    
    # Optimize and Z-Order
    if target_gold_table == 'fact_sales':
        spark.sql(f"OPTIMIZE delta.`{fact_path}` ZORDER BY (dim_customer_key, dim_product_key)")
    else:
        spark.sql(f"OPTIMIZE delta.`{fact_path}` ZORDER BY (dim_customer_key)")
        
    count = spark.read.format("delta").load(fact_path).count()
    dbutils.notebook.exit(str(count))
    
except Exception as e:
    raise Exception(f"Failed to build fact {target_gold_table}: {str(e)}")