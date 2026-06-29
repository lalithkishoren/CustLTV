# Enterprise Semantic & Metric Layer Catalogue

## Overview
This repository contains the governed Semantic Layer for the Myntra Customer Lifetime Value (CLV) Data Platform. It acts as the **Single Source of Truth** for all business logic, ensuring that both Artificial Intelligence (Databricks SQL / LLMs via dbt MetricFlow) and Business Intelligence (Power BI via TMDL) consume identical, mathematically consistent definitions.

By decoupling metric definitions from the presentation layer, we prevent "metric drift" and ensure that a KPI calculated in a Databricks Notebook matches the exact value shown on an Executive Power BI Dashboard.

## Core KPIs

### 1. Average Order Value (AOV)
* **Description**: Measures the average revenue generated per unique order.
* **Expression**: `SUM(TOTAL_AMOUNT) / COUNT(DISTINCT ORDER_ID)`
* **Grain**: Customer / Monthly
* **Domain**: Sales
* **Source Entities**: `fact_sales`

### 2. Purchase Frequency
* **Description**: Measures the average number of purchases per 30-day period for a customer. Used heavily in RFM segmentation and CLV prediction.
* **Expression**: `COUNT(DISTINCT ORDER_ID) / (DATEDIFF(day, MIN(ORDER_DATE), MAX(ORDER_DATE)) / 30.0)`
* **Grain**: Customer
* **Domain**: Customer Retention
* **Source Entities**: `fact_sales`

### 3. Customer Acquisition Cost (CAC)
* **Description**: Measures the cost to acquire a new customer via marketing campaigns. Explicitly handles organic traffic (where CAC = $0).
* **Expression**: `SUM(TOTAL_SPEND) / NULLIF(SUM(CUSTOMERS_ACQUIRED), 0)`
* **Grain**: Campaign / Monthly
* **Domain**: Marketing
* **Source Entities**: `agg_monthly_campaign_roi`

## Architecture & Consumption

### AI & Ad-Hoc Analytics (dbt MetricFlow)
Located in `dbt/semantic_models/` and `dbt/metrics/`. 
Data Scientists and Analysts can query these metrics directly in Databricks SQL using the MetricFlow integration: