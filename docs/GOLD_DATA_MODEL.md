# Gold Layer Data Model

## Overview
The Gold layer implements an Enterprise Data Warehouse Bus Architecture utilizing a Star Schema optimized for Power BI DirectQuery and Databricks SQL Serverless.

## Dimension Tables
All dimensions utilize surrogate keys (`dim_*_key`) and include an Unknown member (`-1`).

1. **dim_customer** (SCD Type 2)
   - Tracks historical changes to `status`, `customer_type`, and `marketing_opt_in`.
   - Validity windows based on event time (`_valid_from`, `_valid_to`).
2. **dim_product** (SCD Type 1)
   - Denormalized hierarchy: Brand -> Category -> Product.
3. **dim_location** (SCD Type 1)
   - Denormalized geography: City Tier -> State -> City.
4. **dim_campaign** (SCD Type 1)
   - Includes special Organic member (`-2`) for CAC attribution.
5. **dim_date** (Static)
   - Conformed date dimension (2015-2030).

## Fact Tables
1. **fact_sales**
   - Grain: Order Line Item.
   - Partitioned by: `order_year_month`.
   - Measures: `quantity`, `line_total`, `allocated_discount_amount`.
2. **fact_interactions**
   - Grain: Customer Service Interaction.
3. **fact_surveys**
   - Grain: Survey Response.

## Aggregate Tables
1. **agg_monthly_campaign_roi**
   - Grain: Month, Campaign.
   - KPIs: Customer Acquisition Cost (CAC), Return on Ad Spend (ROAS).
2. **agg_customer_clv_metrics**
   - Grain: Customer.
   - KPIs: Average Order Value (AOV), Purchase Frequency, Customer Lifespan Days.

## Sample BI Queries

**1. Monthly CAC vs ROAS**