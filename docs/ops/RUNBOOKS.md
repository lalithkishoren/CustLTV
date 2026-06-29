# Operational Runbooks: Myntra CLV Data Platform

## 1. Overview
This runbook outlines standard operating procedures (SOPs) for monitoring, troubleshooting, and reprocessing data within the Azure-based Myntra CLV Data Platform.

## 2. Escalation Matrix
| Severity | Condition | Primary Contact | SLA for Acknowledgement |
| :--- | :--- | :--- | :--- |
| **SEV-1** | Gold layer data missing/stale for > 4 hours; Power BI reports failing. | Data Platform On-Call (PagerDuty) | 15 Minutes |
| **SEV-2** | Silver DLT pipeline failed; Bronze ingestion delayed. | Data Engineering Team (Slack: #de-alerts) | 1 Hour |
| **SEV-3** | High volume of records routed to DLQ (Quarantine). | Data Stewards / Governance Team | 24 Hours |

## 3. Common Failure Scenarios & Resolution

### 3.1 ADF Pipeline Failure: Source API/DB Timeout
* **Symptom**: ADF `Copy_Oracle_ERP` or `Copy_Marketing_API` activity fails with `TimeoutException` or `SqlException`.
* **Root Cause**: Source system under heavy load or network transient issue.
* **Resolution**:
  1. ADF is configured with 3 automatic retries (exponential backoff). Verify if all retries exhausted in Azure Monitor.
  2. If exhausted, check source system health (Oracle DB / Marketing API).
  3. **Idempotent Rerun**: Once the source is healthy, simply click **"Rerun from failed activity"** in ADF Monitor. The pipeline uses `LAST_UPDATE_DATE` watermarks and Delta `MERGE`, so partial loads will not cause duplicates.

### 3.2 Databricks DLT Pipeline Failure: Schema Mismatch
* **Symptom**: DLT pipeline fails with `Flow_Update_Failed` due to `AnalysisException: Cannot resolve column`.
* **Root Cause**: Source system added, dropped, or renamed a column without prior contract sign-off.
* **Resolution**:
  1. Identify the offending column in the DLT event logs.
  2. If the change is additive (new column), update the DLT schema definition and restart the pipeline. DLT will automatically evolve the schema (`schema_evolution: allow_additive`).
  3. If the change is breaking (dropped/renamed column), escalate to the Source System Owner (SEV-2) to revert, or update the Silver/Gold transformation logic to handle the missing column gracefully.

### 3.3 Data Quality Violations (High DLQ Volume)
* **Symptom**: Alert triggered: `> 5% of records routed to silver.dq_exception_log`.
* **Root Cause**: Upstream data entry issues (e.g., negative order amounts, null customer IDs).
* **Resolution**:
  1. Query the DLQ table in Databricks SQL: