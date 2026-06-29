# Data Dictionary: Customer Lifetime Value (CLV) Analytics

## Overview
This document provides the business definitions for the Gold layer (Business/Analytics) of the CLV Data Platform. The data is stored in Delta Lake on ADLS Gen2 and governed by Unity Catalog.

All tables are optimized for Power BI DirectQuery and Databricks SQL Serverless consumption.

---

## 1. Key Performance Indicators (KPIs)

These metrics are pre-calculated in the Gold aggregate tables to ensure a single source of truth across the enterprise.

| KPI Name | Business Definition | Technical Formula | Grain |
| :--- | :--- | :--- | :--- |
| **Average Order Value (AOV)** | The average amount spent by a customer per order. | `SUM(TOTAL_AMOUNT) / COUNT(DISTINCT ORDER_ID)` | Customer / Monthly |
| **Purchase Frequency** | The average number of orders placed by a customer per 30-day period during their active lifespan. | `COUNT(DISTINCT ORDER_ID) / (DATEDIFF(day, MIN(ORDER_DATE), MAX(ORDER_DATE)) / 30.0)` | Customer |
| **Customer Acquisition Cost (CAC)** | The average marketing spend required to acquire a new customer. | `SUM(TOTAL_SPEND) / NULLIF(SUM(CUSTOMERS_ACQUIRED), 0)` | Campaign / Monthly |

---

## 2. Dimension Tables (Conformed)

Dimensions provide the filtering, grouping, and descriptive attributes for business processes.

### `dim_customer`
Contains customer demographic and lifecycle data. Implements Slowly Changing Dimension (SCD) Type 2 to track historical changes, specifically the `status` column for churn analysis.
*   **`customer_sk`**: Surrogate Key (Primary Key).
*   **`customer_id`**: Natural Key from Oracle CRM.
*   **`first_name` / `last_name` / `email`**: PII fields. Dynamically masked in Unity Catalog based on user role.
*   **`status`**: Current lifecycle status (`Active`, `Churned`, `Suspended`).
*   **`_valid_from` / `_valid_to`**: Event-time based validity timestamps for the record state.
*   **`_is_current`**: Boolean flag indicating the active record.

### `dim_campaign`
Contains marketing campaign metadata from the Marketing Platform API.
*   **`campaign_sk`**: Surrogate Key.
*   **`campaign_id`**: Natural Key.
*   **`campaign_name`**: Name of the marketing initiative.
*   **`channel`**: Marketing channel (e.g., `Social`, `Email`, `Organic`). *Note: If channel is 'Organic', CAC is explicitly set to 0.*

### `dim_date`
Standard enterprise date dimension.
*   **`date_key`**: Integer representation (e.g., 20231025).
*   **`full_date`**: Date type.
*   **`calendar_year` / `calendar_month` / `calendar_quarter`**: Standard calendar attributes.

---

## 3. Fact Tables

Fact tables contain the measurable, quantitative data about a business event.

### `fact_sales`
Records individual product line items within customer orders from Oracle ERP.
*   **Grain**: One row per order line item.
*   **Partitioning**: Partitioned by `order_date` (Month/Year). Z-Ordered by `customer_sk` and `campaign_sk`.
*   **`order_line_id`**: Primary Key.
*   **`customer_sk`**: Foreign key to `dim_customer`.
*   **`order_date`**: Date the order was placed.
*   **`total_amount`**: Monetary value. *Note: Data Quality rules ensure this is always > 0. Cancelled/Returned orders are filtered out in the Silver layer.*

### `fact_surveys`
Records customer feedback and Net Promoter Score (NPS) data.
*   **Grain**: One row per survey response.
*   **`survey_id`**: Primary Key.
*   **`customer_sk`**: Foreign key to `dim_customer`.
*   **`nps_score`**: Integer from 0-10. *Note: Only joined to orders where `ORDER_STATUS` = 'Delivered'.*

---

## 4. Aggregate Tables

### `agg_customer_clv_metrics`
Customer-level rollups used directly by Data Science for predictive modeling and BI for RFM segmentation.
*   **`customer_id`**: Natural Key.
*   **`average_order_value`**: See KPI definitions.
*   **`purchase_frequency`**: See KPI definitions.
*   **`total_lifetime_revenue`**: Sum of all valid order amounts.

### `agg_monthly_campaign_roi`
Campaign-level rollups for marketing performance analysis.
*   **`campaign_id`**: Natural Key.
*   **`reporting_month`**: First day of the month.
*   **`customer_acquisition_cost`**: See KPI definitions.