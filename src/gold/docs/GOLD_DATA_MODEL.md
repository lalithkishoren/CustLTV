# Gold Layer Data Model

## Overview
The Gold layer implements an Enterprise Data Warehouse Bus Architecture using a Star Schema optimized for BI querying, CLV analytics, and RFM segmentation.

## Dimensions (Conformed)
- **dim_customer** (SCD Type 2): Tracks customer history, churn status, and segmentation.
- **dim_product** (SCD Type 1): Product hierarchy (Brand -> Category -> Product).
- **dim_location** (SCD Type 1): Geographic data and city tiers.
- **dim_campaign** (SCD Type 1): Marketing campaign details.
- **dim_registration_source** (SCD Type 1): Acquisition channel details.
- **dim_date**: Standard calendar dimension.

## Facts
- **fact_sales**: Transaction grain (Order Line). Contains additive measures (`quantity`, `line_total`) and allocated discounts.
- **fact_interactions**: Transaction grain (Customer Support Interaction).
- **fact_surveys**: Transaction grain (Survey Response). Contains non-additive measures (`nps_score`, `csat_score`).

## Aggregates (KPIs)
- **agg_monthly_campaign_roi**: Monthly grain. Calculates Customer Acquisition Cost (CAC) and Return on Ad Spend (ROAS).
- **agg_customer_clv_metrics**: Customer grain. Calculates Average Order Value (AOV), Purchase Frequency, and Customer Lifespan.

## Key Business Rules Implemented
1. **CAC Calculation**: `SUM(total_spend) / NULLIF(SUM(customers_acquired), 0)`. Organic channels explicitly have 0 CAC.
2. **AOV Calculation**: `SUM(line_total) / COUNT(DISTINCT order_number)`.
3. **SCD2 Event Time**: Customer history validity windows are based on the source system's `last_update_date` (Event Time), not pipeline processing time.
4. **Referential Integrity**: Missing dimension keys default to `-1` (Unknown). Organic campaigns default to `-2`.