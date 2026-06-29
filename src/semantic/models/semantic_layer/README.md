# Myntra Data Platform: Governed Semantic & Metric Layer

## Overview
This directory contains the single source of truth for Myntra's business metrics, implemented using the **dbt Semantic Layer (MetricFlow)**. It sits directly on top of the Gold layer (Databricks Unity Catalog) and provides a governed, code-based definition of entities, dimensions, measures, and derived metrics. 

By defining metrics here, we ensure that downstream consumers—whether Power BI DirectQuery datasets, Databricks AI/BI Genie, or custom ML models—always query the exact same business logic, eliminating metric drift and "fan-out" calculation errors.

## Metric Catalogue

| Metric Name | Business Description | Grain | Expression |
| :--- | :--- | :--- | :--- |
| **Customer Lifetime Value (CLV)** | The net revenue value of a customer, accounting for the cost to acquire them. | Customer | `SUM(fact_sales.line_total) - MAX(dim_campaign.acquisition_cost)` |
| **Average Order Value (AOV)** | The average revenue generated per unique order placed. | Customer / Monthly | `SUM(fact_sales.line_total) / COUNT(DISTINCT fact_sales.order_id)` |
| **Purchase Frequency** | The average number of orders placed by a customer per month of their tenure. | Customer | `COUNT(DISTINCT fact_sales.order_id) / NULLIF(DATEDIFF(MONTH, MIN(fact_sales.order_date), CURRENT_DATE()), 0)` |

## Architecture Principles Enforced
1. **Correctness (Join Cardinality)**: Entities and relationships are explicitly defined (`primary`, `foreign`). The semantic engine automatically aggregates to the join grain *before* joining, preventing row-multiplication explosions across the Star Schema.
2. **Business Meaning**: Technical columns (`line_total`, `_is_current`) are abstracted into governed business terms (`total_sales_amount`, `churn_status`).
3. **Scalability**: Push-down aggregations ensure that Databricks SQL Serverless only processes the required grouped data, minimizing shuffle and optimizing Power BI DirectQuery performance.