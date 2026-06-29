# Gold Layer Data Model

## Overview
The Gold layer implements an Enterprise Data Warehouse Bus Architecture utilizing conformed dimensions and a Star Schema design optimized for Power BI DirectQuery and ML feature extraction.

## Dimension Tables
| Table Name | SCD Type | Business Key | Description |
|------------|----------|--------------|-------------|
| `dim_customer` | Type 2 | `customer_id` | Customer master with history tracking for churn status. |
| `dim_product` | Type 1 | `inventory_item_id` | Product hierarchy (Brand -> Category -> Product). |
| `dim_location` | Type 1 | `address_id` | Geographic data and city tiers. |
| `dim_campaign` | Type 1 | `campaign_id` | Marketing campaigns. Includes `-2` Organic member. |
| `dim_registration_source` | Type 1 | `registration_source_id` | Acquisition channel data. |

## Fact Tables
| Table Name | Grain | Partitioning | Description |
|------------|-------|--------------|-------------|
| `fact_sales` | Order Line | `order_year_month` | Revenue, discounts, and product sales. |
| `fact_interactions` | Interaction | `interaction_year_month` | Customer service touchpoints. |
| `fact_surveys` | Survey Response | `response_year_month` | NPS and CSAT scores. |

## Aggregate Tables (KPIs)
| Table Name | Grain | Key Metrics |
|------------|-------|-------------|
| `agg_monthly_campaign_roi` | Month, Campaign | CAC, ROAS, Total Revenue, Total Spend |
| `agg_customer_clv_metrics` | Customer | AOV, Purchase Frequency, Lifespan Days, Lifetime Revenue |

## Data Quality & Governance
- **Referential Integrity**: Enforced via `COALESCE(key, -1)` during Fact loads.
- **Idempotency**: Guaranteed via Delta `MERGE` (Dimensions) and dynamic partition overwrite (Facts).
- **Authentication**: Strictly Unity Catalog Managed Identity. No secrets in code.