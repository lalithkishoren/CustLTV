# Silver Layer Transformations & Data Quality

## Architecture Principles
- **Idempotency**: All Silver loads use Delta `MERGE` (SCD1 or SCD2). Re-running a pipeline will never duplicate data.
- **Progressive Trust**: Bad rows are quarantined, not dropped silently. The pipeline continues processing valid rows.
- **One Writer Per Table**: Only the Silver Orchestrator writes to the Silver tables.
- **Authentication**: Zero secrets in code. All storage access is governed by Unity Catalog Managed Identities.

## Transformation Rules Engine
Transformations are driven dynamically by `control.silver_transformation_rules`. 

### Supported Rule Types
| Rule Type | Description | Example Expression |
|-----------|-------------|--------------------|
| `CAST` | Changes data type | `BIGINT`, `DECIMAL(15,2)` |
| `TRANSFORM` | Applies SQL function | `UPPER(TRIM(STATUS))` |
| `RENAME` | Renames column | N/A (Uses target_column) |
| `FILTER` | Drops rows pre-merge | `ORDER_DATE >= '2020-01-01'` |

## Data Quality (DQ) Implementation
Based on the Approved Project Decisions, DQ is enforced strictly in the PySpark code:

1. **Error Severity (Quarantine)**:
   - `TOTAL_AMOUNT > 0`: Orders with 0 or negative amounts are routed to the quarantine path.
   - `CUSTOMER_ID IS NOT NULL`: Orphaned records are quarantined.
   - *Action*: Row is removed from the main DataFrame and written to `abfss://silver@.../quarantine/<table_name>`.

2. **Warn Severity (Tagging)**:
   - `STATUS IS NOT NULL`: If status is missing, the row is kept but tagged.
   - *Action*: The string `'STATUS IS NULL'` is appended to the `_dq_warnings` array column in the Silver table.

## Slowly Changing Dimensions (SCD)
- **SCD Type 1 (Overwrite)**: Used for Orders, Products, Campaigns. Updates overwrite existing non-key columns.
- **SCD Type 2 (History)**: Used for `customers`. Tracks changes to `status`, `customer_type`, and `marketing_opt_in`. Maintains `_valid_from`, `_valid_to`, and `_is_current` flags.

## Deduplication Strategy
Deduplication is performed *before* transformations using the approved strategy:
`source_primary_key + keep_latest_by_event_time`
Implemented via PySpark Window functions partitioning by PK and ordering by `_cdc_version` or `LAST_UPDATE_DATE` descending.