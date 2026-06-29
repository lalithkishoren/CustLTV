# Gold Layer Low-Level Design Specification

## 1. Executive Summary

### 1.1 Solution Overview
- **Business Problem**: The Gold layer transforms cleansed, standardized Silver data into a highly optimized dimensional model (Star Schema) to enable rapid Business Intelligence (BI) querying, Customer Lifetime Value (CLV) analytics, RFM (Recency, Frequency, Monetary) segmentation, and proactive churn prediction for Myntra.
- **High-Level Architecture**:
```text
[Silver Layer (Delta Lake)]
    │
    │ Cleansed data with CDC/SCD metadata
    ▼
[Azure Data Factory Orchestrator]
    │
    ▼
[Databricks DLT / Spark Processing]
    ├── 1. Dimension Processing (Surrogate Keys, SCD1/SCD2)
    ├── 2. Fact Processing (Dimension Lookups, Measure Calculations)
    └── 3. Aggregate Processing (Pre-computed KPIs)
    │
    ▼
[Gold Layer (Dimensional Model on ADLS Gen2)] ──► [Power BI / ML Models]
```
- **Dimensional Modeling Approach**: Enterprise Data Warehouse Bus Architecture utilizing conformed dimensions (Date, Customer, Product, Campaign) shared across multiple business process fact tables (Sales, Interactions, Surveys).
- **Key Capabilities**:
  - Sub-second BI query performance via denormalized Star Schemas and Z-Ordering.
  - Accurate historical reporting via SCD Type 2 tracking on Customer attributes (e.g., Churn Status).
  - Pre-calculated complex KPIs (AOV, CAC, Purchase Frequency) in Aggregate tables.

### 1.2 Scope
- **Number of Fact Tables**: 3 (Sales, Customer Interactions, Surveys)
- **Number of Dimension Tables**: 6 (Date, Customer, Product, Location, Campaign, Registration Source)
- **Number of Aggregate Tables**: 2 (Monthly Campaign ROI, Customer CLV Metrics)
- **Out-of-Scope Items**:
  - Machine Learning model training (handled in separate Databricks ML workspace, though Gold provides the feature data).
  - Real-time streaming analytics (architecture is batch/micro-batch based on Blueprint).
- **Assumptions and Constraints**:
  - Silver layer provides reliable `_is_current`, `_valid_from`, and `_valid_to` flags for SCD2 sources.
  - Power BI will use DirectQuery on Databricks SQL Serverless endpoints for Fact tables, and Import mode for Aggregates.

### 1.3 Technology Stack
- **Orchestration**: Azure Data Factory (ADF)
- **Compute**: Azure Databricks (Spark SQL / DataFrames)
- **Storage**: ADLS Gen2 (Delta Lake format)
- **Governance & Security**: Unity Catalog (RBAC, Dynamic Data Masking), Azure Key Vault
- **Consumption**: Power BI, Databricks SQL Serverless

---

## 2. Dimensional Model Overview

### 2.1 Business Process Identification
- **Business Process 1: Order Fulfillment & Sales** - Tracking revenue, discounts, and product sales at the order line level.
- **Business Process 2: Customer Service & Support** - Tracking customer incidents and interactions to measure support load and customer friction.
- **Business Process 3: Customer Feedback** - Tracking NPS and CSAT scores to correlate satisfaction with retention and revenue.

### 2.2 Bus Matrix (Enterprise Data Warehouse Bus Architecture)
- **Fact: fact_sales**
  - dim_date: YES
  - dim_customer: YES
  - dim_product: YES
  - dim_location: YES
  - dim_campaign: YES
  - dim_registration_source: NO
- **Fact: fact_interactions**
  - dim_date: YES
  - dim_customer: YES
  - dim_product: NO
  - dim_location: NO
  - dim_campaign: NO
  - dim_registration_source: NO
- **Fact: fact_surveys**
  - dim_date: YES
  - dim_customer: YES
  - dim_product: NO
  - dim_location: NO
  - dim_campaign: NO
  - dim_registration_source: NO

### 2.3 Conformed Dimensions
- **dim_date**: Used by ALL fact tables (conformed).
- **dim_customer**: Used by ALL fact tables (conformed).
- **dim_product**: Used by fact_sales.
- **dim_campaign**: Used by fact_sales (via customer registration source linkage).

### 2.4 Grain Definition Summary
- **fact_sales**: One row represents a single product line item within a customer order.
- **fact_interactions**: One row represents a single logged interaction/touchpoint for a customer incident.
- **fact_surveys**: One row represents a single submitted survey response from a customer.

---

## 3. Architecture Design

### 3.1 High-Level Architecture
```text
Silver Layer (Cleansed)
    │
    │ Delta tables (silver.customers, silver.oe_order_lines_all, etc.)
    ▼
[Gold Orchestrator Pipeline (ADF)]
    │
    ├── Dimension Processing (Databricks)
    │   ├── Generate SKs via ROW_NUMBER() / MAX(SK)
    │   ├── SCD Type 1 (Overwrite Product, Location, Campaign)
    │   └── SCD Type 2 (History for Customer Status)
    │
    ├── Fact Processing (Databricks)
    │   ├── Dimension Key Lookups (COALESCE to -1 for Unknown)
    │   ├── Measure Calculations (Line Total, Discount)
    │   └── Referential Integrity Enforcement
    │
    └── Aggregate Processing (Databricks)
        ├── GROUP BY aggregations (Monthly, Campaign level)
        └── Pre-computed metrics (AOV, CAC, Purchase Frequency)
    │
    ▼
Gold Layer (Dimensional Model in Unity Catalog)
    │
    ├── Dimensions: gold.dim_customer, gold.dim_product...
    ├── Facts: gold.fact_sales, gold.fact_interactions...
    └── Aggregates: gold.agg_monthly_campaign_roi...
    │
    ▼
BI / Analytics Consumption (Power BI / Databricks SQL)
```

### 3.2 Component Specifications
- **Component: Gold Orchestrator (ADF)**
  - Technology: Azure Data Factory
  - Purpose: Master control flow ensuring Dimensions load before Facts, and Facts before Aggregates.
  - Inputs: `control.table_metadata` completion flags.
  - Outputs: Triggered Databricks notebooks.
- **Component: Dimension & Fact Loader (Databricks)**
  - Technology: Azure Databricks (Spark SQL)
  - Purpose: Executes MERGE statements for SCD logic and Fact inserts.
  - Configuration: Auto-scaling compute, Delta Cache enabled.
- **Component: Gold Storage (ADLS Gen2 / Delta Lake)**
  - Technology: Delta Lake on ADLS Gen2
  - Purpose: Persistent, query-optimized storage for the Star Schema.
  - Configuration: Z-Ordering enabled on high-cardinality SKs, Auto-compaction enabled.

### 3.3 Data Flow Architecture
- **Dimension Load Flow**: Read Silver → Generate SK → Apply SCD1/SCD2 MERGE → Write Gold Dimension.
- **Fact Load Flow**: Read Silver Fact → Lookup SKs from Gold Dimensions (using Event Time for SCD2) → Calculate Measures → Write Gold Fact.
- **Aggregate Load Flow**: Read Gold Fact & Dimensions → Execute GROUP BY SQL → Write Gold Aggregate.

---

## 4. Control Database Extensions

### 4.1 Extended Table: control.table_metadata
- **Column: gold_table_name**
  - Data Type: VARCHAR(255)
- **Column: gold_table_type**
  - Data Type: VARCHAR(50) (DIMENSION, FACT, AGGREGATE)
- **Column: gold_schema**
  - Data Type: VARCHAR(100)
- **Column: gold_path**
  - Data Type: VARCHAR(500)
- **Column: gold_initial_load_completed**
  - Data Type: BIT (Default 0)
- **Column: gold_last_load_status**
  - Data Type: VARCHAR(50)
- **Column: gold_last_load_timestamp**
  - Data Type: DATETIME2
- **Column: gold_records_loaded**
  - Data Type: BIGINT
- **Column: surrogate_key_column**
  - Data Type: VARCHAR(255)
- **Column: grain_description**
  - Data Type: VARCHAR(500)

### 4.2 New Table: control.gold_dimension_config
- **Column: config_id** (PK, IDENTITY)
- **Column: dimension_name** (VARCHAR(255))
- **Column: source_silver_table** (VARCHAR(255))
- **Column: business_key_columns** (VARCHAR(500))
- **Column: scd_type** (INT)
- **Column: tracked_columns** (VARCHAR(MAX))
- **Column: surrogate_key_column** (VARCHAR(255))
- **Column: unknown_member_key** (INT, Default -1)
- **Column: is_active** (BIT, Default 1)

### 4.3 New Table: control.gold_fact_config
- **Column: config_id** (PK, IDENTITY)
- **Column: fact_name** (VARCHAR(255))
- **Column: source_silver_table** (VARCHAR(255))
- **Column: grain_description** (VARCHAR(500))
- **Column: fact_type** (VARCHAR(50))
- **Column: dimension_keys** (VARCHAR(MAX))
- **Column: measure_columns** (VARCHAR(MAX))
- **Column: is_active** (BIT, Default 1)

### 4.4 New Table: control.gold_aggregate_config
- **Column: config_id** (PK, IDENTITY)
- **Column: aggregate_name** (VARCHAR(255))
- **Column: source_fact_table** (VARCHAR(255))
- **Column: aggregation_grain** (VARCHAR(500))
- **Column: refresh_frequency** (VARCHAR(50))
- **Column: is_active** (BIT, Default 1)

---

## 5. Dimension Table Design

### 5.1 Dimension Design Principles
- Every dimension has a surrogate key (BIGINT, auto-generated).
- Every dimension has a natural/business key from the Silver layer.
- Unknown member (key = -1) is explicitly inserted during initial load to handle referential integrity failures gracefully.
- SCD Type 2 validity windows are based on EVENT time (`last_update_date`), not processing time, to prevent history corruption from late-arriving data.

### 5.2 Surrogate Key Generation
- **Method**: `COALESCE(MAX(surrogate_key), 0) + ROW_NUMBER() OVER (ORDER BY business_key)`
- **Starting value**: 1
- **Unknown member**: -1 (Business Key = 'UNKNOWN')
- **N/A member**: -2 (Business Key = 'N/A')

### 5.3 SCD Type 1 Specifications (Overwrite)
- Direct UPDATE of changed attributes based on Business Key match.
- No history tracking. `_last_modified_date` is updated to `CURRENT_TIMESTAMP`.

### 5.4 SCD Type 2 Specifications (History Tracking)
- Full history of attribute changes.
- Metadata columns:
  - `_valid_from` (TIMESTAMP): Record effective start (Event Time).
  - `_valid_to` (TIMESTAMP): Record effective end (NULL = current).
  - `_is_current` (BOOLEAN): Current record flag.
  - `_version` (INT): Version number.

### 5.5 Standard Dimension: dim_date
- **dim_date_key** (INT, PK): YYYYMMDD format
- **full_date** (DATE)
- **day_of_week** (INT): 1-7
- **day_name** (VARCHAR(10)): Monday, Tuesday, etc.
- **day_of_month** (INT): 1-31
- **day_of_year** (INT): 1-366
- **week_of_year** (INT): 1-53
- **month_number** (INT): 1-12
- **month_name** (VARCHAR(15)): January, February, etc.
- **quarter** (INT): 1-4
- **year** (INT): YYYY
- **is_weekend** (BOOLEAN)
- **is_holiday** (BOOLEAN)
- **Date Range**: 2015-01-01 to 2030-12-31

---

## 6. Dimension Table Specifications

### Dimension: dim_customer
- **Source Details**:
  - Silver Table: `silver.customers`
  - Business Key: `customer_id`
  - SCD Type: 2 (History tracking required for Churn analysis)
- **Schema Definition**:
  - **Surrogate Key**:
    - `dim_customer_key` (BIGINT, NOT NULL, PK, AUTO-GENERATED)
  - **Natural/Business Key**:
    - `customer_id` (BIGINT, NOT NULL)
  - **Descriptive Attributes**:
    - **email**:
      - Data Type: STRING
      - Nullable: YES
      - SCD Behavior: Type 1 (overwrite)
      - Description: Customer email address (Masked via Unity Catalog)
      - Transformation: `silver.customers.email`
    - **phone**:
      - Data Type: STRING
      - Nullable: YES
      - SCD Behavior: Type 1 (overwrite)
      - Description: Customer phone number (Masked via Unity Catalog)
      - Transformation: `silver.customers.phone`
    - **full_name**:
      - Data Type: STRING
      - Nullable: YES
      - SCD Behavior: Type 1 (overwrite)
      - Description: Concatenated first and last name
      - Transformation: `CONCAT_WS(' ', silver.customers.first_name, silver.customers.last_name)`
    - **gender**:
      - Data Type: STRING
      - Nullable: YES
      - SCD Behavior: Type 1 (overwrite)
      - Description: Customer gender
      - Transformation: `silver.customers.gender`
    - **customer_type**:
      - Data Type: STRING
      - Nullable: YES
      - SCD Behavior: Type 2 (track history)
      - Description: Segmentation type
      - Transformation: `silver.customers.customer_type`
    - **status**:
      - Data Type: STRING
      - Nullable: NO
      - SCD Behavior: Type 2 (track history)
      - Description: Active/Churned status (Critical for CLV)
      - Transformation: `COALESCE(silver.customers.status, 'UNKNOWN')`
    - **marketing_opt_in**:
      - Data Type: BOOLEAN
      - Nullable: NO
      - SCD Behavior: Type 2 (track history)
      - Description: Opt-in status for campaigns
      - Transformation: `COALESCE(silver.customers.marketing_opt_in, false)`
    - **registration_date**:
      - Data Type: TIMESTAMP
      - Nullable: YES
      - SCD Behavior: Type 1 (overwrite)
      - Description: Original registration date
      - Transformation: `silver.customers.registration_date`
  - **SCD Type 2 Metadata**:
    - `_valid_from` (TIMESTAMP, NOT NULL) - Sourced from `silver.customers.last_update_date`
    - `_valid_to` (TIMESTAMP, NULL for current)
    - `_is_current` (BOOLEAN, NOT NULL)
    - `_version` (INT, NOT NULL)
  - **Audit Columns**:
    - `_created_date` (TIMESTAMP)
    - `_last_modified_date` (TIMESTAMP)
    - `_pipeline_run_id` (STRING)
- **Special Members**:
  - Unknown Member: `dim_customer_key` = -1, `customer_id` = -1, `full_name` = 'UNKNOWN'
- **Data Quality Rules**:
  - DQ-G-D-001: UNIQUENESS - `customer_id` - `COUNT(*) OVER (PARTITION BY customer_id WHERE _is_current = true) = 1` - ERROR
  - DQ-G-D-002: SCD_VALIDITY - `_valid_from` - `_valid_from < COALESCE(_valid_to, '2099-12-31')` - ERROR
- **Gold Layer Target**:
  - Path: `gold/dimensions/dim_customer/`
  - Format: Delta Lake
  - Z-Order: `customer_id`

### Dimension: dim_product
- **Source Details**:
  - Silver Tables: `silver.mtl_system_items_b` (Base), `silver.categories`, `silver.brands`
  - Business Key: `inventory_item_id`
  - SCD Type: 1 (Overwrite)
- **Schema Definition**:
  - **Surrogate Key**:
    - `dim_product_key` (BIGINT, NOT NULL, PK, AUTO-GENERATED)
  - **Natural/Business Key**:
    - `inventory_item_id` (BIGINT, NOT NULL)
  - **Descriptive Attributes**:
    - **sku**:
      - Data Type: STRING
      - Nullable: NO
      - SCD Behavior: Type 1
      - Description: Stock Keeping Unit
      - Transformation: `silver.mtl_system_items_b.sku`
    - **product_name**:
      - Data Type: STRING
      - Nullable: YES
      - SCD Behavior: Type 1
      - Description: Product Name
      - Transformation: `silver.mtl_system_items_b.product_name`
    - **category_name**:
      - Data Type: STRING
      - Nullable: YES
      - SCD Behavior: Type 1
      - Description: Denormalized Category Name
      - Transformation: `silver.categories.category_name`
    - **brand_name**:
      - Data Type: STRING
      - Nullable: YES
      - SCD Behavior: Type 1
      - Description: Denormalized Brand Name
      - Transformation: `silver.brands.brand_name`
    - **unit_cost**:
      - Data Type: DECIMAL(15,2)
      - Nullable: YES
      - SCD Behavior: Type 1
      - Description: Standard cost of item
      - Transformation: `silver.mtl_system_items_b.unit_cost`
  - **Audit Columns**:
    - `_created_date` (TIMESTAMP)
    - `_last_modified_date` (TIMESTAMP)
    - `_pipeline_run_id` (STRING)
- **Special Members**:
  - Unknown Member: `dim_product_key` = -1, `inventory_item_id` = -1, `product_name` = 'UNKNOWN'
- **Hierarchies**:
  - Level 1: `brand_name`
  - Level 2: `category_name`
  - Level 3: `product_name`
- **Data Quality Rules**:
  - DQ-G-D-003: UNIQUENESS - `inventory_item_id` - `COUNT(*) OVER (PARTITION BY inventory_item_id) = 1` - ERROR
- **Gold Layer Target**:
  - Path: `gold/dimensions/dim_product/`
  - Format: Delta Lake
  - Z-Order: `inventory_item_id`, `category_name`

### Dimension: dim_location
- **Source Details**:
  - Silver Tables: `silver.addresses` (Base), `silver.city_tier_master`
  - Business Key: `address_id`
  - SCD Type: 1 (Overwrite)
- **Schema Definition**:
  - **Surrogate Key**:
    - `dim_location_key` (BIGINT, NOT NULL, PK, AUTO-GENERATED)
  - **Natural/Business Key**:
    - `address_id` (BIGINT, NOT NULL)
  - **Descriptive Attributes**:
    - **city**:
      - Data Type: STRING
      - Nullable: YES
      - SCD Behavior: Type 1
      - Description: City name
      - Transformation: `silver.addresses.city`
    - **state**:
      - Data Type: STRING
      - Nullable: YES
      - SCD Behavior: Type 1
      - Description: State name
      - Transformation: `silver.addresses.state`
    - **postal_code**:
      - Data Type: STRING
      - Nullable: YES
      - SCD Behavior: Type 1
      - Description: Postal/Zip code
      - Transformation: `silver.addresses.postal_code`
    - **country**:
      - Data Type: STRING
      - Nullable: YES
      - SCD Behavior: Type 1
      - Description: Country name
      - Transformation: `silver.addresses.country`
    - **city_tier**:
      - Data Type: STRING
      - Nullable: YES
      - SCD Behavior: Type 1
      - Description: Denormalized City Tier (Tier 1, Tier 2, etc.)
      - Transformation: `COALESCE(silver.city_tier_master.tier, 'UNKNOWN')`
  - **Audit Columns**:
    - `_created_date` (TIMESTAMP)
    - `_last_modified_date` (TIMESTAMP)
    - `_pipeline_run_id` (STRING)
- **Special Members**:
  - Unknown Member: `dim_location_key` = -1, `address_id` = -1, `city` = 'UNKNOWN'
- **Data Quality Rules**:
  - DQ-G-D-004: UNIQUENESS - `address_id` - `COUNT(*) OVER (PARTITION BY address_id) = 1` - ERROR
- **Gold Layer Target**:
  - Path: `gold/dimensions/dim_location/`
  - Format: Delta Lake
  - Z-Order: `address_id`, `city`

### Dimension: dim_campaign
- **Source Details**:
  - Silver Table: `silver.marketing_campaigns`
  - Business Key: `campaign_id`
  - SCD Type: 1 (Overwrite - Blueprint specifies standard SCD1 for campaigns)
- **Schema Definition**:
  - **Surrogate Key**:
    - `dim_campaign_key` (BIGINT, NOT NULL, PK, AUTO-GENERATED)
  - **Natural/Business Key**:
    - `campaign_id` (INT, NOT NULL)
  - **Descriptive Attributes**:
    - **campaign_name**:
      - Data Type: STRING
      - Nullable: YES
      - SCD Behavior: Type 1
      - Description: Name of campaign
      - Transformation: `silver.marketing_campaigns.campaign_name`
    - **channel**:
      - Data Type: STRING
      - Nullable: YES
      - SCD Behavior: Type 1
      - Description: Marketing channel (e.g., Social, Email)
      - Transformation: `silver.marketing_campaigns.channel`
    - **sub_channel**:
      - Data Type: STRING
      - Nullable: YES
      - SCD Behavior: Type 1
      - Description: Specific sub-channel
      - Transformation: `silver.marketing_campaigns.sub_channel`
    - **status**:
      - Data Type: STRING
      - Nullable: YES
      - SCD Behavior: Type 1
      - Description: Campaign status
      - Transformation: `silver.marketing_campaigns.status`
    - **start_date**:
      - Data Type: DATE
      - Nullable: YES
      - SCD Behavior: Type 1
      - Description: Campaign start date
      - Transformation: `silver.marketing_campaigns.start_date`
    - **end_date**:
      - Data Type: DATE
      - Nullable: YES
      - SCD Behavior: Type 1
      - Description: Campaign end date
      - Transformation: `silver.marketing_campaigns.end_date`
  - **Audit Columns**:
    - `_created_date` (TIMESTAMP)
    - `_last_modified_date` (TIMESTAMP)
    - `_pipeline_run_id` (STRING)
- **Special Members**:
  - Unknown Member: `dim_campaign_key` = -1, `campaign_id` = -1, `campaign_name` = 'UNKNOWN'
  - Organic Member: `dim_campaign_key` = -2, `campaign_id` = -2, `campaign_name` = 'ORGANIC' (Used for CAC Attribution Flow)
- **Data Quality Rules**:
  - DQ-G-D-005: UNIQUENESS - `campaign_id` - `COUNT(*) OVER (PARTITION BY campaign_id) = 1` - ERROR
- **Gold Layer Target**:
  - Path: `gold/dimensions/dim_campaign/`
  - Format: Delta Lake
  - Z-Order: `campaign_id`, `channel`

### Dimension: dim_registration_source
- **Source Details**:
  - Silver Table: `silver.customer_registration_source`
  - Business Key: `registration_source_id`
  - SCD Type: 1 (Overwrite)
- **Schema Definition**:
  - **Surrogate Key**:
    - `dim_registration_source_key` (BIGINT, NOT NULL, PK, AUTO-GENERATED)
  - **Natural/Business Key**:
    - `registration_source_id` (BIGINT, NOT NULL)
  - **Descriptive Attributes**:
    - **channel**:
      - Data Type: STRING
      - Nullable: YES
      - SCD Behavior: Type 1
      - Description: Acquisition channel
      - Transformation: `silver.customer_registration_source.channel`
    - **utm_source**:
      - Data Type: STRING
      - Nullable: YES
      - SCD Behavior: Type 1
      - Description: UTM Source
      - Transformation: `silver.customer_registration_source.utm_source`
    - **utm_medium**:
      - Data Type: STRING
      - Nullable: YES
      - SCD Behavior: Type 1
      - Description: UTM Medium
      - Transformation: `silver.customer_registration_source.utm_medium`
    - **utm_campaign**:
      - Data Type: STRING
      - Nullable: YES
      - SCD Behavior: Type 1
      - Description: UTM Campaign
      - Transformation: `silver.customer_registration_source.utm_campaign`
    - **device_type**:
      - Data Type: STRING
      - Nullable: YES
      - SCD Behavior: Type 1
      - Description: Device used during registration
      - Transformation: `silver.customer_registration_source.device_type`
  - **Audit Columns**:
    - `_created_date` (TIMESTAMP)
    - `_last_modified_date` (TIMESTAMP)
    - `_pipeline_run_id` (STRING)
- **Special Members**:
  - Unknown Member: `dim_registration_source_key` = -1, `registration_source_id` = -1, `channel` = 'UNKNOWN'
- **Data Quality Rules**:
  - DQ-G-D-006: UNIQUENESS - `registration_source_id` - `COUNT(*) OVER (PARTITION BY registration_source_id) = 1` - ERROR
- **Gold Layer Target**:
  - Path: `gold/dimensions/dim_registration_source/`
  - Format: Delta Lake
  - Z-Order: `registration_source_id`

---

## 7. Fact Table Design

### 7.1 Fact Table Design Principles
- Grain is strictly defined to prevent row-multiplication ("fan-out").
- All dimension foreign keys reference Gold surrogate keys.
- Degenerate dimensions (e.g., `order_number`) are kept in the fact table.
- Referential integrity is enforced via dimension lookups; missing keys default to `-1` (Unknown).

### 7.2 KPI & Measure Calculation SQL Specifications

**KPI: Average Order Value (AOV)**
- **Business Definition**: Total revenue divided by the number of unique orders.
- **Measure Type**: Non-Additive
- **Calculation Frequency**: Daily/Monthly (via Aggregates)
- **Source Columns**: `silver.oe_order_headers_all.total_amount`, `silver.oe_order_headers_all.order_id`
- **Formula (Math)**: SUM(total_amount) / COUNT(DISTINCT order_id)
- **Complete SQL Expression**:
```sql
SUM(total_amount) / NULLIF(COUNT(DISTINCT order_number), 0) AS avg_order_value
```
- **Aggregation Constraints**: Cannot be summed across dimensions. Must be recalculated at the required grain.

**KPI: Customer Acquisition Cost (CAC)**
- **Business Definition**: Total marketing spend divided by customers acquired. If channel is 'Organic', CAC is explicitly 0.
- **Measure Type**: Non-Additive
- **Calculation Frequency**: Monthly (via Aggregates)
- **Source Columns**: `silver.marketing_campaigns.total_spend`, `silver.marketing_campaigns.customers_acquired`
- **Formula (Math)**: SUM(total_spend) / SUM(customers_acquired)
- **Complete SQL Expression**:
```sql
CASE 
    WHEN channel = 'ORGANIC' THEN 0.00
    ELSE SUM(total_spend) / NULLIF(SUM(customers_acquired), 0) 
END AS customer_acquisition_cost
```
- **Aggregation Constraints**: Cannot be summed.

**KPI: Purchase Frequency**
- **Business Definition**: Number of orders placed by a customer over a specific period.
- **Measure Type**: Additive (Count)
- **Calculation Frequency**: Daily/Monthly
- **Source Columns**: `silver.oe_order_headers_all.order_id`
- **Formula (Math)**: COUNT(DISTINCT order_id)
- **Complete SQL Expression**:
```sql
COUNT(DISTINCT order_number) AS purchase_frequency
```

**KPI: Customer Lifespan (Days)**
- **Business Definition**: Days between registration and either their last order or their churn date.
- **Measure Type**: Semi-Additive
- **Calculation Frequency**: Daily
- **Source Columns**: `dim_customer.registration_date`, `dim_customer.status`, `dim_customer._valid_from`, `fact_sales.order_date`
- **Formula (Math)**: DATEDIFF(End Date, Registration Date)
- **Complete SQL Expression**:
```sql
DATEDIFF(
    COALESCE(
        CASE WHEN c.status = 'CHURNED' THEN c._valid_from ELSE NULL END, 
        MAX(f.order_date)
    ), 
    c.registration_date
) AS customer_lifespan_days
```

---

## 8. Fact Table Specifications

### Fact: fact_sales
- **Business Process**:
  - Description: Order fulfillment and revenue tracking.
  - Grain: One row represents a single product line item within a customer order.
  - Fact Type: TRANSACTION
- **Source Details**:
  - Silver Tables: `silver.oe_order_lines_all` (Base), `silver.oe_order_headers_all` (Joined for header attributes)
  - Source Grain: Order Line
  - Transformation: Inner join lines to headers to flatten transaction details.
- **Schema Definition**:
  - **Dimension Foreign Keys**:
    - `dim_date_key` (INT, NOT NULL, FK → dim_date)
    - `dim_customer_key` (BIGINT, NOT NULL, FK → dim_customer)
    - `dim_product_key` (BIGINT, NOT NULL, FK → dim_product)
    - `dim_location_key` (BIGINT, NOT NULL, FK → dim_location)
    - `dim_campaign_key` (BIGINT, NOT NULL, FK → dim_campaign)
  - **Degenerate Dimensions**:
    - `order_id` (BIGINT): Source system order ID
    - `order_number` (STRING): Source system order number
    - `line_id` (BIGINT): Source system line ID
    - `order_status` (STRING): Status of the order
  - **Measures (Additive)**:
    - `quantity` (INT): Number of units purchased - Aggregation: SUM
      - Source: `silver.oe_order_lines_all.quantity`
    - `line_total` (DECIMAL(15,2)): Total amount for the line - Aggregation: SUM
      - Source: `silver.oe_order_lines_all.line_total`
    - `allocated_discount_amount` (DECIMAL(15,2)): Header discount allocated to line - Aggregation: SUM
      - Source: `(silver.oe_order_lines_all.line_total / silver.oe_order_headers_all.subtotal_amount) * silver.oe_order_headers_all.discount_amount`
    - `allocated_tax_amount` (DECIMAL(15,2)): Header tax allocated to line - Aggregation: SUM
      - Source: `(silver.oe_order_lines_all.line_total / silver.oe_order_headers_all.subtotal_amount) * silver.oe_order_headers_all.tax_amount`
  - **Measures (Non-Additive)**:
    - `unit_price` (DECIMAL(15,2)): Price per unit - Aggregation: AVG
      - Source: `silver.oe_order_lines_all.unit_price`
  - **Audit Columns**:
    - `_created_date` (TIMESTAMP)
    - `_last_modified_date` (TIMESTAMP)
    - `_pipeline_run_id` (STRING)
- **Dimension Key Lookup Logic**:
  - `dim_date_key`: `CAST(DATE_FORMAT(h.order_date, 'yyyyMMdd') AS INT)`
  - `dim_customer_key`: `JOIN dim_customer c ON h.customer_id = c.customer_id AND h.order_date >= c._valid_from AND h.order_date < COALESCE(c._valid_to, '2099-12-31')`
  - `dim_product_key`: `JOIN dim_product p ON l.product_id = p.inventory_item_id`
  - `dim_location_key`: `JOIN dim_location loc ON h.shipping_address_id = loc.address_id`
  - `dim_campaign_key`: `LEFT JOIN silver.customer_registration_source crs ON h.customer_id = crs.customer_id LEFT JOIN dim_campaign cam ON crs.campaign_id = cam.campaign_id` (Fallback to -2 'ORGANIC' if crs.channel = 'Organic' and campaign_id is null).
- **Data Quality Rules**:
  - DQ-G-F-001: RANGE_CHECK - `line_total` - `line_total >= 0` - SKIP_ROW
  - DQ-G-F-002: RANGE_CHECK - `quantity` - `quantity > 0` - SKIP_ROW
- **Gold Layer Target**:
  - Path: `gold/facts/fact_sales/`
  - Format: Delta Lake
  - Partition: `order_year_month` (Derived from order_date)
  - Z-Order: `dim_customer_key`, `dim_product_key`

### Fact: fact_interactions
- **Business Process**:
  - Description: Customer service interactions and incident tracking.
  - Grain: One row represents a single interaction tied to an incident.
  - Fact Type: TRANSACTION
- **Source Details**:
  - Silver Tables: `silver.interactions` (Base), `silver.incidents` (Joined for incident context)
  - Source Grain: Interaction
- **Schema Definition**:
  - **Dimension Foreign Keys**:
    - `dim_date_key` (INT, NOT NULL, FK → dim_date)
    - `dim_customer_key` (BIGINT, NOT NULL, FK → dim_customer)
  - **Degenerate Dimensions**:
    - `interaction_id` (BIGINT): Source interaction ID
    - `incident_id` (BIGINT): Source incident ID
    - `interaction_type` (STRING): Type of interaction (Call, Email, etc.)
    - `incident_status` (STRING): Status of the parent incident
    - `incident_priority` (STRING): Priority of the parent incident
  - **Measures (Additive)**:
    - `interaction_count` (INT): Always 1, used for counting - Aggregation: SUM
      - Source: Hardcoded `1`
  - **Audit Columns**:
    - `_created_date` (TIMESTAMP)
    - `_last_modified_date` (TIMESTAMP)
    - `_pipeline_run_id` (STRING)
- **Dimension Key Lookup Logic**:
  - `dim_date_key`: `CAST(DATE_FORMAT(int.interaction_date, 'yyyyMMdd') AS INT)`
  - `dim_customer_key`: `JOIN dim_customer c ON inc.customer_id = c.customer_id AND int.interaction_date >= c._valid_from AND int.interaction_date < COALESCE(c._valid_to, '2099-12-31')`
- **Data Quality Rules**:
  - DQ-G-F-003: NULL_CHECK - `interaction_id` - `interaction_id IS NOT NULL` - SKIP_ROW
- **Gold Layer Target**:
  - Path: `gold/facts/fact_interactions/`
  - Format: Delta Lake
  - Partition: `interaction_year_month`
  - Z-Order: `dim_customer_key`

### Fact: fact_surveys
- **Business Process**:
  - Description: Customer feedback and satisfaction tracking.
  - Grain: One row represents a single survey response.
  - Fact Type: TRANSACTION
- **Source Details**:
  - Silver Table: `silver.surveys`
  - Source Grain: Survey Response
- **Schema Definition**:
  - **Dimension Foreign Keys**:
    - `dim_date_key` (INT, NOT NULL, FK → dim_date)
    - `dim_customer_key` (BIGINT, NOT NULL, FK → dim_customer)
  - **Degenerate Dimensions**:
    - `survey_id` (BIGINT): Source survey ID
    - `order_id` (BIGINT): Associated order ID (if applicable)
    - `incident_id` (BIGINT): Associated incident ID (if applicable)
    - `survey_type` (STRING): Type of survey (NPS, CSAT)
    - `nps_category` (STRING): Promoter, Passive, Detractor
  - **Measures (Non-Additive)**:
    - `nps_score` (INT): Net Promoter Score (0-10) - Aggregation: AVG
      - Source: `silver.surveys.nps_score`
    - `csat_score` (INT): Customer Satisfaction Score (1-5) - Aggregation: AVG
      - Source: `silver.surveys.csat_score`
  - **Audit Columns**:
    - `_created_date` (TIMESTAMP)
    - `_last_modified_date` (TIMESTAMP)
    - `_pipeline_run_id` (STRING)
- **Dimension Key Lookup Logic**:
  - `dim_date_key`: `CAST(DATE_FORMAT(s.response_date, 'yyyyMMdd') AS INT)`
  - `dim_customer_key`: `JOIN dim_customer c ON s.customer_id = c.customer_id AND s.response_date >= c._valid_from AND s.response_date < COALESCE(c._valid_to, '2099-12-31')`
- **Data Quality Rules**:
  - DQ-G-F-004: RANGE_CHECK - `nps_score` - `nps_score BETWEEN 0 AND 10 OR nps_score IS NULL` - SKIP_ROW
  - DQ-G-F-005: RANGE_CHECK - `csat_score` - `csat_score BETWEEN 1 AND 5 OR csat_score IS NULL` - SKIP_ROW
- **Gold Layer Target**:
  - Path: `gold/facts/fact_surveys/`
  - Format: Delta Lake
  - Partition: `response_year_month`
  - Z-Order: `dim_customer_key`

---

## 9. Aggregate Table Specifications

### Aggregate: agg_monthly_campaign_roi
- **Purpose**:
  - Base Fact: `fact_sales` (joined with `dim_campaign` and `silver.marketing_campaigns`)
  - Aggregation Grain: Month, Campaign
  - Use Case: Marketing dashboard to track CAC and Campaign ROI.
  - Refresh Frequency: DAILY
- **Grain Definition**:
  - One row represents: Total performance of a specific campaign in a specific month.
  - GROUP BY columns: `year_month`, `dim_campaign_key`
- **Schema Definition**:
  - **Grain Columns**:
    - `year_month` (INT): YYYYMM format - From `dim_date`
    - `dim_campaign_key` (BIGINT): Campaign SK - From `dim_campaign`
  - **Aggregated Measures**:
    - `total_revenue` (DECIMAL(15,2)): `SUM(line_total)` - Additive
    - `total_orders` (INT): `COUNT(DISTINCT order_number)` - Additive
    - `total_spend` (DECIMAL(15,2)): `MAX(campaign_total_spend)` - Non-Additive (Spend is at campaign level, not order level)
    - `customers_acquired` (INT): `MAX(campaign_customers_acquired)` - Non-Additive
  - **Calculated KPIs**:
    - `customer_acquisition_cost` (DECIMAL(15,2)): `MAX(campaign_total_spend) / NULLIF(MAX(campaign_customers_acquired), 0)` - Non-Additive
    - `return_on_ad_spend` (DECIMAL(15,2)): `SUM(line_total) / NULLIF(MAX(campaign_total_spend), 0)` - Non-Additive
- **Complete Aggregation SQL**:
```sql
CREATE OR REPLACE TABLE gold.agg_monthly_campaign_roi AS
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
FROM gold.fact_sales f
JOIN gold.dim_date d ON f.dim_date_key = d.dim_date_key
JOIN gold.dim_campaign c ON f.dim_campaign_key = c.dim_campaign_key
LEFT JOIN silver.marketing_campaigns mc ON c.campaign_id = mc.campaign_id
GROUP BY 
    d.year * 100 + d.month_number,
    f.dim_campaign_key,
    c.campaign_name,
    c.channel
```
- **Gold Layer Target**:
  - Path: `gold/aggregates/agg_monthly_campaign_roi/`
  - Partition: None
  - Z-Order: `year_month`, `dim_campaign_key`

### Aggregate: agg_customer_clv_metrics
- **Purpose**:
  - Base Fact: `fact_sales`
  - Aggregation Grain: Customer
  - Use Case: Input for ML predictive models and RFM segmentation dashboards.
  - Refresh Frequency: DAILY
- **Grain Definition**:
  - One row represents: Lifetime aggregated metrics for a single customer.
  - GROUP BY columns: `dim_customer_key`
- **Schema Definition**:
  - **Grain Columns**:
    - `dim_customer_key` (BIGINT): Customer SK - From `dim_customer`
  - **Aggregated Measures**:
    - `lifetime_revenue` (DECIMAL(15,2)): `SUM(line_total)` - Additive
    - `total_orders` (INT): `COUNT(DISTINCT order_number)` - Additive
    - `total_items_purchased` (INT): `SUM(quantity)` - Additive
    - `first_order_date` (DATE): `MIN(order_date)` - Non-Additive
    - `last_order_date` (DATE): `MAX(order_date)` - Non-Additive
  - **Calculated KPIs**:
    - `average_order_value` (DECIMAL(15,2)): `SUM(line_total) / NULLIF(COUNT(DISTINCT order_number), 0)` - Non-Additive
    - `customer_lifespan_days` (INT): `DATEDIFF(COALESCE(churn_date, MAX(order_date)), registration_date)` - Non-Additive
- **Complete Aggregation SQL**:
```sql
CREATE OR REPLACE TABLE gold.agg_customer_clv_metrics AS
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
FROM gold.fact_sales f
JOIN gold.dim_customer c ON f.dim_customer_key = c.dim_customer_key
JOIN gold.dim_date d ON f.dim_date_key = d.dim_date_key
GROUP BY 
    f.dim_customer_key,
    c.customer_id,
    c.status,
    c._valid_from,
    c.registration_date
```
- **Gold Layer Target**:
  - Path: `gold/aggregates/agg_customer_clv_metrics/`
  - Partition: None
  - Z-Order: `dim_customer_key`

---

## 10. Pipeline Architecture Specifications

### 10.1 Gold Orchestrator Pipeline
- **Name**: `PL_Gold_Orchestrator`
- **Parameters**: `pipeline_run_id`
- **Execution Order**:
  - **Phase 1 — Initial Load (`gold_initial_load_completed = 0`)**:
    1. Execute `PL_Gold_Load_Dim_Date` (Generates static date dimension).
    2. Lookup pending dimensions. Execute `PL_Gold_Initial_Dimension_Load` in parallel.
       - Generates SKs, inserts Unknown members (-1).
       - Sets `gold_initial_load_completed = 1`.
    3. Lookup pending facts. Execute `PL_Gold_Initial_Fact_Load` sequentially after dimensions.
       - Resolves FKs, inserts full dataset.
       - Sets `gold_initial_load_completed = 1`.
  - **Phase 2 — Incremental Refresh (`gold_initial_load_completed = 1`)**:
    4. Load active dimensions (`WHERE updated_at > gold_last_load_timestamp`).
       - Apply SCD1 (UPDATE) or SCD2 (Close old, Insert new) via MERGE.
    5. Load active facts (`WHERE record_updated_timestamp > gold_last_load_timestamp`).
       - Resolve FKs using Event Time for SCD2 dimensions. MERGE into Fact.
    6. Load all aggregate tables (Full overwrite or partition overwrite).
    7. Update `gold_last_load_timestamp` for all processed tables.

### 10.2 Dimension Load Pipeline
- **Name**: `PL_Gold_Load_Dimension`
- **Parameters**: `pipeline_run_id`, `dimension_name`, `source_table`, `business_key`, `scd_type`
- **Activities**:
  1. Read Silver source table (Incremental changes).
  2. Generate SKs for new records using `MAX(surrogate_key) + ROW_NUMBER()`.
  3. Apply SCD logic via Delta `MERGE INTO`.
  4. Update control metadata.

### 10.3 Fact Load Pipeline
- **Name**: `PL_Gold_Load_Fact`
- **Parameters**: `pipeline_run_id`, `fact_name`, `source_table`
- **Activities**:
  1. Read Silver source table (Incremental changes).
  2. Lookup ALL dimension keys (COALESCE to -1 if missing).
  3. Calculate derived measures (e.g., allocated discounts).
  4. Execute Data Quality filter (Route invalid to DLQ).
  5. `MERGE INTO` Gold fact table (Partition overwrite or PK merge).
  6. Update control metadata.

### 10.4 Aggregate Load Pipeline
- **Name**: `PL_Gold_Load_Aggregate`
- **Parameters**: `pipeline_run_id`, `aggregate_name`, `source_fact`
- **Activities**:
  1. Read Gold fact and dimension tables.
  2. Execute GROUP BY aggregations.
  3. `INSERT OVERWRITE` into Gold aggregate table.
  4. Update control metadata.

---

## 11. Data Flow Diagrams

### 11.1 Dimension Load Flow (SCD Type 2)
```text
Silver Table (silver.customers)
    │
    │ Read incremental records
    ▼
[Lookup Existing dim_customer]
    │
    ├── NEW customer_id
    │   └── Generate new dim_customer_key
    │       └── INSERT with _is_current=true, _valid_from=last_update_date
    │
    └── EXISTING customer_id
        │
        ├── Hash matches (No Change) → Skip
        │
        └── Hash differs (Changed)
            ├── UPDATE old row: _valid_to = new.last_update_date, _is_current = false
            └── INSERT new row: _valid_from = new.last_update_date, _is_current = true, _version = old+1
    │
    ▼
Gold Dimension Table (gold.dim_customer)
```

### 11.2 Fact Load Flow
```text
Silver Table (silver.oe_order_lines_all + headers)
    │
    │ Read incremental records
    ▼
[Dimension Key Lookups]
    │
    ├── dim_date: CAST(DATE_FORMAT(order_date, 'yyyyMMdd') AS INT)
    ├── dim_customer: Lookup by customer_id WHERE order_date BETWEEN _valid_from AND _valid_to
    ├── dim_product: Lookup by inventory_item_id
    │   (COALESCE all lookups to -1 if not found)
    │
    ▼
[Calculate Derived Measures]
    - allocated_discount = (line_total / subtotal) * header_discount
    │
    ▼
[Data Quality & RI Check]
    │
    ├── Valid → Continue
    └── Invalid (e.g., line_total < 0) → Log to control.dq_exception_log
    │
    ▼
[MERGE into Fact Table]
    │
    ▼
Gold Fact Table (gold.fact_sales)
```

---

## 12. Gold Layer Data Quality Rules - SQL Specifications

### 12.1 Referential Integrity Rules (Dimension Lookups)
- **DQ-G-RI-001**
  - **Fact Table**: `gold.fact_sales`
  - **Dimension**: `gold.dim_customer`
  - **Source Key**: `customer_id`
  - **Lookup Logic**: `COALESCE(c.dim_customer_key, -1)`
  - **Validation SQL**: `SELECT COUNT(*) FROM gold.fact_sales WHERE dim_customer_key = -1`
  - **Unknown Usage SQL**: `SELECT (COUNT(CASE WHEN dim_customer_key = -1 THEN 1 END) * 100.0) / COUNT(*) FROM gold.fact_sales`
- **DQ-G-RI-002**
  - **Fact Table**: `gold.fact_sales`
  - **Dimension**: `gold.dim_product`
  - **Source Key**: `product_id`
  - **Lookup Logic**: `COALESCE(p.dim_product_key, -1)`
  - **Validation SQL**: `SELECT COUNT(*) FROM gold.fact_sales WHERE dim_product_key = -1`
  - **Unknown Usage SQL**: `SELECT (COUNT(CASE WHEN dim_product_key = -1 THEN 1 END) * 100.0) / COUNT(*) FROM gold.fact_sales`

### 12.2 Measure Validation Rules
- **DQ-G-M-001**
  - **Entity**: `gold.fact_sales`
  - **Measure**: `line_total`
  - **Business Requirement**: Line total must be positive.
  - **Valid Row Filter**: `line_total >= 0`
  - **Invalid Row Check**: `line_total < 0 OR line_total IS NULL`
- **DQ-G-M-002**
  - **Entity**: `gold.fact_surveys`
  - **Measure**: `nps_score`
  - **Business Requirement**: NPS must be 0-10.
  - **Valid Row Filter**: `nps_score BETWEEN 0 AND 10 OR nps_score IS NULL`
  - **Invalid Row Check**: `nps_score < 0 OR nps_score > 10`

### 12.3 Aggregate Consistency Rules
- **DQ-G-A-001**
  - **Aggregate Table**: `gold.agg_monthly_campaign_roi`
  - **Source Fact**: `gold.fact_sales`
  - **Reconciliation Type**: SUM_MATCH
  - **Reconciliation SQL**: 
    ```sql
    SELECT 
      ABS(
        (SELECT SUM(total_revenue) FROM gold.agg_monthly_campaign_roi) - 
        (SELECT SUM(line_total) FROM gold.fact_sales)
      ) AS variance
    ```
  - **Tolerance**: `< 1.00` (Accounting for minor rounding)
  - **Alert Condition**: `variance >= 1.00`

### 12.4 Combined Gold DQ Filter Generation (fact_sales)
```sql
-- Valid Rows (Proceed to MERGE)
SELECT * FROM source_with_lookups
WHERE line_total >= 0 
  AND quantity > 0

-- Invalid Rows (Route to DLQ)
SELECT * FROM source_with_lookups
WHERE line_total < 0 
   OR line_total IS NULL 
   OR quantity <= 0 
   OR quantity IS NULL
```

---

## 13. Error Handling & Recovery

### 13.1 Error Categories
- **Dimension Lookup Failure**: Handled gracefully by `COALESCE(key, -1)`. Record is loaded to Fact table. Warning logged if Unknown usage > 5%.
- **Data Quality Violation**: Row fails measure validation (e.g., negative quantity). Row is dropped from Fact load and written to `control.dq_exception_log`.
- **Pipeline Timeout**: ADF automated retry (3 retries, exponential backoff). Idempotency guaranteed via Delta `MERGE`.

### 13.2 Unknown Member Handling
- Explicitly inserted during Phase 1 Initial Load.
- `dim_customer`: `-1`, `UNKNOWN`
- `dim_product`: `-1`, `UNKNOWN`
- `dim_campaign`: `-1`, `UNKNOWN`, and `-2`, `ORGANIC` (Specific business logic for CAC).

### 13.3 Recovery Procedures
- **Fact Load Failed**: Re-run pipeline. Delta `MERGE` ensures no duplicates are created.
- **Aggregate Refresh Failed**: Re-run pipeline. `INSERT OVERWRITE` completely replaces the aggregate state based on the underlying Fact table.

---

## 14. Storage Design

### 14.1 Gold Layer Storage
- **Container/Path**: `gold/`
- **Structure**:
  - `gold/dimensions/dim_[name]/`
  - `gold/facts/fact_[name]/`
  - `gold/aggregates/agg_[name]/`
- **Format**: Delta Lake
- **Catalog**: Unity Catalog (`catalog.gold.table_name`)

### 14.2 Delta Lake Configuration
- **Auto Optimize**: Enabled (`spark.databricks.delta.optimizeWrite.enabled = true`)
- **Auto Compaction**: Enabled (`spark.databricks.delta.autoCompact.enabled = true`)
- **VACUUM Retention**: 168 hours (7 days) for Facts/Aggregates. 30 days for Dimensions (to protect SCD history).

### 14.3 Partitioning Strategy
- **Dimensions**: Unpartitioned (Avoids small file problem for small tables).
- **Facts**: 
  - `fact_sales`: Partitioned by `order_year_month`.
  - `fact_interactions`: Partitioned by `interaction_year_month`.
  - `fact_surveys`: Partitioned by `response_year_month`.
- **Aggregates**: Unpartitioned (Highly aggregated, small row counts).

### 14.4 Z-Ordering Strategy
- **Dimensions**: Z-Order by `business_key`.
- **Facts**: Z-Order by high-cardinality FKs (`dim_customer_key`, `dim_product_key`).
- **Aggregates**: Z-Order by grain columns (`dim_customer_key` or `year_month`).

---

## 15. Security Specifications

### 15.1 Authentication
- Databricks accesses ADLS Gen2 via Azure Service Principal.
- ADF triggers Databricks jobs via Managed Identity.

### 15.2 Data Security
- **Dynamic Data Masking**: Configured in Unity Catalog for `gold.dim_customer`.
  - Masked Columns: `email`, `phone`.
  - Policy: Only users in the `HR_Support` Entra ID group can view unmasked data. Analytics users see obfuscated values.
- **Encryption**: Data encrypted at rest using ADLS Gen2 AES-256.

### 15.3 Access Patterns
- **Power BI**: Read-only access to `gold` schema via Databricks SQL Serverless.
- **Data Scientists**: Read access to `gold.fact_*` and `gold.dim_*` for ML feature engineering.
- **ETL Service**: Write access to `gold` tables.

---

## 16. Monitoring & Alerting

### 16.1 Key Metrics
- **Unknown Member Usage Percentage**: Tracked per Fact table load.
- **Fact Load Duration**: Tracked vs historical baseline.
- **Records Processed**: Insert/Update counts logged to `control.table_metadata`.

### 16.2 Alerting Rules
- **Pipeline Failure**: Immediate email/Teams alert via Azure Logic Apps.
- **Unknown Member Usage > 5%**: Warning alert to Data Stewards (indicates Silver/Bronze synchronization issues).
- **Aggregate Reconciliation Variance > 1.00**: Critical alert; indicates Fact/Aggregate mismatch.

---

## 17. BI Consumption Patterns

### 17.1 Standard Query Patterns
- **Star Schema Joins**: Power BI datasets should be modeled with 1-to-Many relationships from Dimensions to Facts using the Surrogate Keys (`dim_*_key`).
- **Time Intelligence**: All time-based filtering must use `dim_date`.
- **Pre-Aggregated Usage**: Dashboards requiring Monthly Campaign ROI or Customer CLV should query the `agg_*` tables directly rather than aggregating `fact_sales` on the fly.

### 17.2 Sample Analytical Queries
- **Query 1: Churn Prediction Feature Extraction**
  ```sql
  SELECT 
      c.customer_id,
      c.status,
      a.lifetime_revenue,
      a.average_order_value,
      a.customer_lifespan_days
  FROM gold.agg_customer_clv_metrics a
  JOIN gold.dim_customer c ON a.dim_customer_key = c.dim_customer_key
  WHERE c._is_current = true
  ```
- **Query 2: Monthly CAC vs ROAS**
  ```sql
  SELECT 
      year_month,
      campaign_name,
      customer_acquisition_cost,
      return_on_ad_spend
  FROM gold.agg_monthly_campaign_roi
  ORDER BY year_month DESC
  ```

---

## 18. Deployment Specifications

### 18.1 Deployment Order
1. Execute DDL scripts to extend `control.table_metadata` and create Gold config tables.
2. Deploy Unity Catalog `gold` schema and table definitions (with masking policies).
3. Execute `PL_Gold_Load_Dim_Date` to populate the Date dimension.
4. Deploy Databricks processing notebooks (`NB_Gold_Dim_Load`, `NB_Gold_Fact_Load`, `NB_Gold_Agg_Load`).
5. Deploy ADF Pipelines (`PL_Gold_Orchestrator`, etc.).
6. Execute Phase 1 Initial Load for all Dimensions.
7. Execute Phase 1 Initial Load for all Facts.
8. Execute Phase 1 Initial Load for all Aggregates.

### 18.2 Validation Checklist
- [ ] Gold schema created in Unity Catalog.
- [ ] `dim_date` populated with date range (2015-2030).
- [ ] All dimensions loaded with Unknown member (-1).
- [ ] `dim_campaign` loaded with Organic member (-2).
- [ ] All facts loaded with valid dimension keys.
- [ ] Unknown member usage < 5% for all facts.
- [ ] Aggregate reconciliation queries pass (Variance < 1.00).
- [ ] Power BI DirectQuery connection successful.

---

## 19. Appendix

### 19.1 Surrogate Key Generation Method
- **Method**: `COALESCE(MAX(surrogate_key), 0) + ROW_NUMBER() OVER (ORDER BY business_key)`
- **Starting value**: 1
- **Reserved keys**: 
  - `-1` (Unknown)
  - `-2` (Organic - specific to Campaign dimension)

### 19.2 Bus Matrix (Complete)
- **Fact: fact_sales**
  - dim_date: YES (required for all facts)
  - dim_customer: YES
  - dim_product: YES
  - dim_location: YES
  - dim_campaign: YES
  - dim_registration_source: NO (Registration source is tied to customer, not individual sales)
- **Fact: fact_interactions**
  - dim_date: YES (required for all facts)
  - dim_customer: YES
  - dim_product: NO (Interactions are at incident level, not product level)
  - dim_location: NO
  - dim_campaign: NO
  - dim_registration_source: NO
- **Fact: fact_surveys**
  - dim_date: YES (required for all facts)
  - dim_customer: YES
  - dim_product: NO
  - dim_location: NO
  - dim_campaign: NO
  - dim_registration_source: NO

### 19.3 Measure Classification Summary
- **Fact: fact_sales**
  - **Measure: quantity**
    - Data Type: INT
    - Classification: Additive
    - Aggregation: SUM
  - **Measure: line_total**
    - Data Type: DECIMAL(15,2)
    - Classification: Additive
    - Aggregation: SUM
  - **Measure: allocated_discount_amount**
    - Data Type: DECIMAL(15,2)
    - Classification: Additive
    - Aggregation: SUM
  - **Measure: unit_price**
    - Data Type: DECIMAL(15,2)
    - Classification: Non-Additive
    - Aggregation: AVG
- **Fact: fact_interactions**
  - **Measure: interaction_count**
    - Data Type: INT
    - Classification: Additive
    - Aggregation: SUM
- **Fact: fact_surveys**
  - **Measure: nps_score**
    - Data Type: INT
    - Classification: Non-Additive
    - Aggregation: AVG
  - **Measure: csat_score**
    - Data Type: INT
    - Classification: Non-Additive
    - Aggregation: AVG

### 19.4 Data Quality Rules Summary (COMPLETE - PRODUCTION READY)
- **Rule ID**: DQ-G-D-001
  - **Table**: gold.dim_customer
  - **Column**: customer_id
  - **Rule Type**: UNIQUENESS
  - **Expression**: `COUNT(*) OVER (PARTITION BY customer_id WHERE _is_current = true) = 1`
  - **Severity**: ERROR
- **Rule ID**: DQ-G-D-002
  - **Table**: gold.dim_customer
  - **Column**: _valid_from
  - **Rule Type**: SCD_VALIDITY
  - **Expression**: `_valid_from < COALESCE(_valid_to, '2099-12-31')`
  - **Severity**: ERROR
- **Rule ID**: DQ-G-D-003
  - **Table**: gold.dim_product
  - **Column**: inventory_item_id
  - **Rule Type**: UNIQUENESS
  - **Expression**: `COUNT(*) OVER (PARTITION BY inventory_item_id) = 1`
  - **Severity**: ERROR
- **Rule ID**: DQ-G-D-004
  - **Table**: gold.dim_location
  - **Column**: address_id
  - **Rule Type**: UNIQUENESS
  - **Expression**: `COUNT(*) OVER (PARTITION BY address_id) = 1`
  - **Severity**: ERROR
- **Rule ID**: DQ-G-D-005
  - **Table**: gold.dim_campaign
  - **Column**: campaign_id
  - **Rule Type**: UNIQUENESS
  - **Expression**: `COUNT(*) OVER (PARTITION BY campaign_id) = 1`
  - **Severity**: ERROR
- **Rule ID**: DQ-G-D-006
  - **Table**: gold.dim_registration_source
  - **Column**: registration_source_id
  - **Rule Type**: UNIQUENESS
  - **Expression**: `COUNT(*) OVER (PARTITION BY registration_source_id) = 1`
  - **Severity**: ERROR
- **Rule ID**: DQ-G-F-001
  - **Table**: gold.fact_sales
  - **Column**: line_total
  - **Rule Type**: RANGE_CHECK
  - **Expression**: `line_total >= 0`
  - **Severity**: SKIP_ROW
- **Rule ID**: DQ-G-F-002
  - **Table**: gold.fact_sales
  - **Column**: quantity
  - **Rule Type**: RANGE_CHECK
  - **Expression**: `quantity > 0`
  - **Severity**: SKIP_ROW
- **Rule ID**: DQ-G-F-003
  - **Table**: gold.fact_interactions
  - **Column**: interaction_id
  - **Rule Type**: NULL_CHECK
  - **Expression**: `interaction_id IS NOT NULL`
  - **Severity**: SKIP_ROW
- **Rule ID**: DQ-G-F-004
  - **Table**: gold.fact_surveys
  - **Column**: nps_score
  - **Rule Type**: RANGE_CHECK
  - **Expression**: `nps_score BETWEEN 0 AND 10 OR nps_score IS NULL`
  - **Severity**: SKIP_ROW
- **Rule ID**: DQ-G-F-005
  - **Table**: gold.fact_surveys
  - **Column**: csat_score
  - **Rule Type**: RANGE_CHECK
  - **Expression**: `csat_score BETWEEN 1 AND 5 OR csat_score IS NULL`
  - **Severity**: SKIP_ROW

### 19.5 Glossary
- **Surrogate Key**: System-generated unique identifier for dimension records (e.g., `dim_customer_key`).
- **Natural/Business Key**: Real-world identifier from source system (e.g., `customer_id`).
- **Grain**: Level of detail in a fact table (what one row represents).
- **Conformed Dimension**: Dimension shared across multiple fact tables (e.g., `dim_date`, `dim_customer`).
- **SCD**: Slowly Changing Dimension. Type 1 overwrites history; Type 2 preserves history via effective dates.
- **Additive Measure**: Can be summed across all dimensions (e.g., `quantity`).
- **Semi-Additive**: Can be summed across some dimensions, but not time (e.g., `customer_lifespan_days`).
- **Non-Additive**: Cannot be meaningfully summed; requires recalculation at grain (e.g., `average_order_value`).