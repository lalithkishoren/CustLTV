# Orchestration & Scheduling: CLV Analytics Platform

This repository contains the orchestration configuration for the Customer Lifetime Value (CLV) Analytics platform on Azure. The orchestration strategy utilizes a **Dual-Engine Approach**:
1. **Azure Data Factory (ADF):** Acts as the macro-orchestrator. It handles cross-platform scheduling, external API ingestion (Marketing/Surveys), and triggers the downstream data processing.
2. **Databricks Delta Live Tables (DLT):** Acts as the micro-orchestrator (Data DAG). It natively manages the complex dependencies between Bronze, Silver, and Gold tables, ensuring data quality and idempotent state management.

## Dependency DAG (Bronze -> Silver -> Gold)

The data flow is split into two distinct SLA tracks, managed by separate pipelines to optimize cost and performance:

### 1. Continuous CDC Track (SLA: < 5 Minutes)
* **Sources:** Oracle ERP (Sales) & Oracle CRM (Customers, Incidents).
* **Ingestion:** Debezium writes CDC events to ADLS Gen2.
* **Orchestration:** ADF pipeline `PL_CDC_Streaming_Process` ensures the DLT pipeline is running.
* **DLT DAG (`dlt_cdc_pipeline.json`):**
  * **Bronze:** Auto Loader streams raw CDC JSON/Parquet into `bronze.erp_*` and `bronze.crm_*`.
  * **Silver:** `APPLY CHANGES INTO` (SCD Type 1 & 2) merges data into `silver.oe_order_headers_all`, `silver.customers`, etc. Data Quality expectations quarantine bad rows (e.g., negative order amounts).
  * **Gold:** Materialized views incrementally update `gold.fact_sales` and `gold.dim_customer`.

### 2. Daily Batch Track (SLA: 24 Hours, runs at 02:00 UTC)
* **Sources:** Marketing Platform (Campaigns) & Survey Platform.
* **Orchestration:** ADF pipeline `PL_Batch_Ingest_and_Process` triggered by `TRG_Daily_Batch`.
* **ADF DAG:**
  * `Copy_Marketing_Data` & `Copy_Survey_Data` (Parallel execution).
  * On Success -> `Run_DLT_Batch_Pipeline`.
* **DLT DAG (`dlt_batch_pipeline.json`):**
  * **Bronze:** Reads daily batch files into `bronze.marketing_campaigns`.
  * **Silver:** Cleanses and standardizes campaign data.
  * **Gold:** Computes complex KPIs (AOV, Purchase Frequency, CAC, CLV) by joining the fresh batch data with the continuous CDC data.

## Failure Handling & Retries

1. **ADF Level (Macro):**
   * **Retries:** Copy activities and DLT triggers are configured with a retry count of 3 and a 300-second interval to handle transient network or compute-startup failures.
   * **Alerting:** Failure of the DLT activity triggers a Web Activity to log the error to the Azure SQL Control DB and send an alert via Azure Monitor.
2. **DLT Level (Micro):**
   * **Idempotency:** All Silver and Gold transformations use Delta `MERGE` or `APPLY CHANGES INTO`. If a DLT pipeline fails mid-run, restarting it will safely resume without duplicating data.
   * **Data Quality (DLQ):** Row-level failures (e.g., missing `CUSTOMER_ID`) do *not* fail the pipeline. They are quarantined into a Dead Letter Queue via DLT `expect_or_drop` and `expect_or_fail` (configured to quarantine in this architecture) for later review, ensuring the <5 min SLA is met for healthy records.

## Security & Authentication
* **Zero Secrets in Code:** ADF authenticates to ADLS Gen2 and Databricks using its System Assigned Managed Identity.
* **Unity Catalog:** Databricks accesses ADLS Gen2 via Unity Catalog External Locations backed by a Managed Identity (Access Connector for Azure Databricks). No storage keys are used.