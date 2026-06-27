# Silver Layer Transformations & Data Quality

## Overview
The Silver layer applies business rules, standardizes data types, and enforces data quality. It reads from the immutable Bronze layer and writes to the cleansed Silver layer using Delta Lake `MERGE` operations to ensure idempotency.

## Transformation Rules Engine
Transformations are driven dynamically by the `control.silver_transformation_rules` table. 

### Supported Rule Types:
1. **CAST**: Changes data type.
   - *Example*: `CAST(TOTAL_AMOUNT AS DECIMAL(15,2))`
2. **TRANSFORM**: Applies SQL functions (TRIM, UPPER, COALESCE).
   - *Example*: `UPPER(TRIM(STATUS))`
3. **RENAME**: Renames a column.
   - *Example*: `CUSTOMER_ID` -> `customer_id`
4. **FILTER**: Drops rows before processing (Note: Prefer DQ Quarantine for bad rows).

## Data Quality & Quarantine Policy
Per enterprise architecture decisions:
- **Bad Row Policy**: `quarantine`. Rows failing critical checks are written to `silver/quarantine/{table_name}/` and logged in `control.dq_exception_log`. They are NOT written to the main Silver table.
- **Null Handling**: `leave`. Nulls are preserved unless explicitly handled by a `COALESCE` transformation rule.

### Hardcoded Enterprise Rules:
- `oe_order_headers_all`: `TOTAL_AMOUNT > 0` (Error/Quarantine)
- `customers`: `CUSTOMER_ID IS NOT NULL` (Error/Quarantine)
- `customers`: `STATUS IS NOT NULL` (Warning/Tag only)

## Slowly Changing Dimensions (SCD)
- **SCD Type 1 (Overwrite)**: Used for transactional tables (Orders, Lines) and standard dimensions (Products, Campaigns). Updates overwrite existing non-key columns.
- **SCD Type 2 (History)**: Used for `silver.customers` to track changes in `status`, `customer_type`, and `marketing_opt_in`. Maintains `_valid_from`, `_valid_to`, and `_is_current` metadata columns.

## Performance Optimization
- **Z-Ordering**: Applied automatically post-merge based on `z_order_columns` in the control table (e.g., `customer_id`, `campaign_id`).
- **Partitioning**: Applied to large fact tables (e.g., `oe_order_headers_all` partitioned by `_order_year_month`).