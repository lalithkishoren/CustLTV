# Operational Runbook: CLV Data Platform

## 1. Architecture & Operational Context
The CLV Data Platform is a Medallion architecture built on Azure:
*   **Orchestration**: Azure Data Factory (ADF)
*   **Compute**: Databricks Delta Live Tables (DLT)
*   **Storage**: ADLS Gen2 (Delta Lake)
*   **State Management**: Azure SQL Control DB (Watermarks, Pipeline execution logs)

**Core Principle**: All pipelines are **idempotent**. Re-running a failed pipeline will safely overwrite or MERGE data without creating duplicates. Do not manually delete files in ADLS to "reset" a load.

---

## 2. Failure Handling & Triage

### 2.1 ADF Pipeline Failures (Bronze Ingestion)
**Symptom**: ADF Master Pipeline fails during the `Copy Activity` from Oracle ERP/CRM.
**Common Causes**: Source database timeout, credential rotation, network transient errors.
**Resolution Steps**:
1.  Check the ADF Monitor tab for the specific error message.
2.  If it is a transient network/timeout error, the pipeline is configured with 3 retries (exponential backoff). If it exhausted retries, manually trigger a re-run from the failed activity.
3.  Because ingestion uses `SYS_CHANGE_VERSION` (CDC) or `LAST_UPDATE_DATE` watermarks stored in Azure SQL, a re-run will automatically pick up from the last successful batch.
4.  **Escalation**: If source connection is persistently refused, escalate to the Oracle DBA team.

### 2.2 DLT Pipeline Failures (Silver/Gold Processing)
**Symptom**: Databricks DLT pipeline fails or halts.
**Common Causes**: Schema drift from source, cluster out-of-memory (OOM).
**Resolution Steps**:
1.  Open the Databricks Workspace -> Delta Live Tables -> `clv_dlt_pipeline`.
2.  Review the event log.
3.  If OOM: Temporarily increase the cluster worker size in the DLT settings and restart the pipeline.
4.  If Schema Drift: The contract policy is `breaking_requires_signoff`. If a source column was dropped or changed type, the pipeline will fail safely. Escalate to Data Engineering Core to update the schema contract.

### 2.3 Data Quality Exceptions (Quarantine)
**Symptom**: Business users report missing orders in Power BI.
**Cause**: Records violated Silver layer Data Quality expectations (e.g., `TOTAL_AMOUNT <= 0` or `CUSTOMER_ID IS NULL`) and were routed to the Dead Letter Queue (DLQ).
**Resolution Steps**:
1.  Query the quarantine table: `SELECT * FROM silver.dq_exception_log WHERE _ingest_date = CURRENT_DATE()`
2.  Identify the source system providing bad data.
3.  If the data is genuinely bad, no action is required (the platform successfully protected the Gold layer). Notify the source system owners to fix the data at the origin.
4.  Once fixed at the source, the CDC process will pick up the corrected row as an UPDATE, which will flow through Silver and MERGE into Gold automatically.

---

## 3. Reprocessing & Backfills

Because the architecture relies on Delta Lake MERGE and partition-overwrite semantics, backfilling is straightforward.

### 3.1 Full Historical Reload (Bronze to Gold)
If business logic changes drastically (e.g., a new way to calculate CAC historically):
1.  **Do not drop tables.**
2.  Trigger the ADF pipeline with the parameter `LoadType = 'FULL'`.
3.  This will reset the watermark in the Azure SQL Control DB to `0`.
4.  ADF will extract all historical data.
5.  DLT will process the data. `APPLY CHANGES INTO` (SCD2) and `MERGE` statements will safely update the existing Delta tables without duplicating records.

---

## 4. Routine Maintenance

These tasks are automated via Databricks Workflows but should be monitored.

*   **OPTIMIZE & Z-ORDER**: Runs weekly on Sunday at 02:00 UTC.
    *   *Command*: `OPTIMIZE gold.fact_sales ZORDER BY (customer_sk, campaign_sk)`
    *   *Purpose*: Combines small files and sorts data to ensure sub-second Power BI DirectQuery performance.
*   **VACUUM**: Runs weekly on Sunday at 04:00 UTC.
    *   *Command*: `VACUUM gold.fact_sales RETAIN 168 HOURS`
    *   *Purpose*: Removes old data files no longer referenced by the Delta log to save ADLS storage costs. Time travel is limited to 7 days.

---

## 5. Escalation Matrix

| Severity | Condition | Primary Contact | Secondary Contact |
| :--- | :--- | :--- | :--- |
| **SEV-1** | Gold tables unavailable / Power BI refresh failing | Data Eng On-Call (PagerDuty) | Platform Architect |
| **SEV-2** | Data freshness SLA missed (Data > 24h old) | Data Eng On-Call (Slack: #data-ops-clv) | Data Eng Lead |
| **SEV-3** | High volume of records routed to DLQ/Quarantine | Data Stewards / Source System Owners | Data Eng On-Call |