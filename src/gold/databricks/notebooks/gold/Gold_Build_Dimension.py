# Databricks notebook source
# =================================================================================
# GOLD LAYER: BUILD DIMENSION
# =================================================================================
# CRITICAL AUTH POLICY: Authentication is handled entirely by Unity Catalog.
# The Databricks Access Connector backs a UC Storage Credential + External Locations.
# NO fs.azure.* auth is set here. We simply read/write abfss:// paths.

dbutils.widgets.text("pipeline_run_id", "")
dbutils.widgets.text("gold_table_id", "")
dbutils.widgets.text("source_silver_tables", "")
dbutils.widgets.text("target_gold_table", "")
dbutils.widgets.text("storage_account", "")

pipeline_run_id = dbutils.widgets.get("pipeline_run_id")
target_gold_table = dbutils.widgets.get("target_gold_table")
storage_account = dbutils.widgets.get("storage_account")

# Base paths
silver_base = f"abfss://silver@{storage_account}.dfs.core.windows.net"
gold_base = f"abfss://gold@{storage_account}.dfs.core.windows.net"
dim_path = f"{gold_base}/dimensions/{target_gold_table}"

# Register Silver tables as temp views for SQL processing
if target_gold_table == 'dim_customer':
    spark.read.format("delta").load(f"{silver_base}/customers").createOrReplaceTempView("silver_customers")
    
    # SCD Type 2 Logic for dim_customer
    sql_logic = f"""
    MERGE INTO delta.`{dim_path}` AS target
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
        FROM silver_customers
    ) AS source
    ON target.customer_id = source.customer_id
    
    WHEN MATCHED AND target._is_current = true AND (
        target.customer_type != source.customer_type OR 
        target.status != source.status OR 
        target.marketing_opt_in != source.marketing_opt_in
    ) THEN UPDATE SET 
        target._valid_to = source._valid_from,
        target._is_current = false,
        target._last_modified_date = CURRENT_TIMESTAMP()
        
    WHEN MATCHED AND target._is_current = true THEN UPDATE SET
        target.email = source.email,
        target.phone = source.phone,
        target.full_name = source.full_name,
        target.gender = source.gender,
        target.registration_date = source.registration_date,
        target._last_modified_date = CURRENT_TIMESTAMP()
        
    WHEN NOT MATCHED THEN INSERT (
        dim_customer_key, customer_id, email, phone, full_name, gender, customer_type, status, marketing_opt_in, registration_date,
        _valid_from, _valid_to, _is_current, _version, _created_date, _last_modified_date, _pipeline_run_id
    ) VALUES (
        COALESCE((SELECT MAX(dim_customer_key) FROM delta.`{dim_path}`), 0) + 1,
        source.customer_id, source.email, source.phone, source.full_name, source.gender, source.customer_type, source.status, source.marketing_opt_in, source.registration_date,
        source._valid_from, NULL, true, 1, CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), '{pipeline_run_id}'
    )
    """

elif target_gold_table == 'dim_product':
    spark.read.format("delta").load(f"{silver_base}/mtl_system_items_b").createOrReplaceTempView("silver_items")
    spark.read.format("delta").load(f"{silver_base}/categories").createOrReplaceTempView("silver_categories")
    spark.read.format("delta").load(f"{silver_base}/brands").createOrReplaceTempView("silver_brands")
    
    sql_logic = f"""
    MERGE INTO delta.`{dim_path}` AS target
    USING (
        SELECT 
            i.inventory_item_id,
            i.sku,
            i.product_name,
            c.category_name,
            b.brand_name,
            i.unit_cost
        FROM silver_items i
        LEFT JOIN silver_categories c ON i.category_id = c.category_id
        LEFT JOIN silver_brands b ON i.brand_id = b.brand_id
    ) AS source
    ON target.inventory_item_id = source.inventory_item_id
    
    WHEN MATCHED THEN UPDATE SET 
        target.sku = source.sku,
        target.product_name = source.product_name,
        target.category_name = source.category_name,
        target.brand_name = source.brand_name,
        target.unit_cost = source.unit_cost,
        target._last_modified_date = CURRENT_TIMESTAMP()
        
    WHEN NOT MATCHED THEN INSERT (
        dim_product_key, inventory_item_id, sku, product_name, category_name, brand_name, unit_cost,
        _created_date, _last_modified_date, _pipeline_run_id
    ) VALUES (
        COALESCE((SELECT MAX(dim_product_key) FROM delta.`{dim_path}`), 0) + 1,
        source.inventory_item_id, source.sku, source.product_name, source.category_name, source.brand_name, source.unit_cost,
        CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), '{pipeline_run_id}'
    )
    """

elif target_gold_table == 'dim_location':
    spark.read.format("delta").load(f"{silver_base}/addresses").createOrReplaceTempView("silver_addresses")
    spark.read.format("delta").load(f"{silver_base}/city_tier_master").createOrReplaceTempView("silver_city_tier")
    
    sql_logic = f"""
    MERGE INTO delta.`{dim_path}` AS target
    USING (
        SELECT 
            a.address_id,
            a.city,
            a.state,
            a.postal_code,
            a.country,
            COALESCE(t.tier, 'UNKNOWN') AS city_tier
        FROM silver_addresses a
        LEFT JOIN silver_city_tier t ON a.city = t.city AND a.state = t.state
    ) AS source
    ON target.address_id = source.address_id
    
    WHEN MATCHED THEN UPDATE SET 
        target.city = source.city,
        target.state = source.state,
        target.postal_code = source.postal_code,
        target.country = source.country,
        target.city_tier = source.city_tier,
        target._last_modified_date = CURRENT_TIMESTAMP()
        
    WHEN NOT MATCHED THEN INSERT (
        dim_location_key, address_id, city, state, postal_code, country, city_tier,
        _created_date, _last_modified_date, _pipeline_run_id
    ) VALUES (
        COALESCE((SELECT MAX(dim_location_key) FROM delta.`{dim_path}`), 0) + 1,
        source.address_id, source.city, source.state, source.postal_code, source.country, source.city_tier,
        CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), '{pipeline_run_id}'
    )
    """

elif target_gold_table == 'dim_campaign':
    spark.read.format("delta").load(f"{silver_base}/marketing_campaigns").createOrReplaceTempView("silver_campaigns")
    
    sql_logic = f"""
    MERGE INTO delta.`{dim_path}` AS target
    USING (
        SELECT 
            campaign_id,
            campaign_name,
            channel,
            sub_channel,
            status,
            start_date,
            end_date
        FROM silver_campaigns
    ) AS source
    ON target.campaign_id = source.campaign_id
    
    WHEN MATCHED THEN UPDATE SET 
        target.campaign_name = source.campaign_name,
        target.channel = source.channel,
        target.sub_channel = source.sub_channel,
        target.status = source.status,
        target.start_date = source.start_date,
        target.end_date = source.end_date,
        target._last_modified_date = CURRENT_TIMESTAMP()
        
    WHEN NOT MATCHED THEN INSERT (
        dim_campaign_key, campaign_id, campaign_name, channel, sub_channel, status, start_date, end_date,
        _created_date, _last_modified_date, _pipeline_run_id
    ) VALUES (
        COALESCE((SELECT MAX(dim_campaign_key) FROM delta.`{dim_path}`), 0) + 1,
        source.campaign_id, source.campaign_name, source.channel, source.sub_channel, source.status, source.start_date, source.end_date,
        CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), '{pipeline_run_id}'
    )
    """

elif target_gold_table == 'dim_registration_source':
    spark.read.format("delta").load(f"{silver_base}/customer_registration_source").createOrReplaceTempView("silver_reg_source")
    
    sql_logic = f"""
    MERGE INTO delta.`{dim_path}` AS target
    USING (
        SELECT 
            registration_source_id,
            channel,
            utm_source,
            utm_medium,
            utm_campaign,
            device_type
        FROM silver_reg_source
    ) AS source
    ON target.registration_source_id = source.registration_source_id
    
    WHEN MATCHED THEN UPDATE SET 
        target.channel = source.channel,
        target.utm_source = source.utm_source,
        target.utm_medium = source.utm_medium,
        target.utm_campaign = source.utm_campaign,
        target.device_type = source.device_type,
        target._last_modified_date = CURRENT_TIMESTAMP()
        
    WHEN NOT MATCHED THEN INSERT (
        dim_registration_source_key, registration_source_id, channel, utm_source, utm_medium, utm_campaign, device_type,
        _created_date, _last_modified_date, _pipeline_run_id
    ) VALUES (
        COALESCE((SELECT MAX(dim_registration_source_key) FROM delta.`{dim_path}`), 0) + 1,
        source.registration_source_id, source.channel, source.utm_source, source.utm_medium, source.utm_campaign, source.device_type,
        CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP(), '{pipeline_run_id}'
    )
    """

elif target_gold_table == 'dim_date':
    # Static date dimension generation (handled via initial load, skipping MERGE for daily runs)
    sql_logic = "SELECT 1 AS dummy"

else:
    raise ValueError(f"Unknown dimension table: {target_gold_table}")

# Execute the MERGE statement
try:
    # Check if table exists, if not, we need to create it with the unknown member first
    from delta.tables import DeltaTable
    if not DeltaTable.isDeltaTable(spark, dim_path):
        print(f"Table {dim_path} does not exist. Initial load required before MERGE.")
        # In a real scenario, the initial load pipeline creates the table and inserts -1.
        # For this script, we assume the table exists.
    
    if target_gold_table != 'dim_date':
        spark.sql(sql_logic)
        
    # Optimize and Z-Order
    if target_gold_table == 'dim_customer':
        spark.sql(f"OPTIMIZE delta.`{dim_path}` ZORDER BY (customer_id)")
    elif target_gold_table == 'dim_product':
        spark.sql(f"OPTIMIZE delta.`{dim_path}` ZORDER BY (inventory_item_id, category_name)")
        
    # Get record count for logging
    count = spark.read.format("delta").load(dim_path).count()
    dbutils.notebook.exit(str(count))
    
except Exception as e:
    raise Exception(f"Failed to build dimension {target_gold_table}: {str(e)}")