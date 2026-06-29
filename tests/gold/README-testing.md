# Gold Layer Test Harness

This test suite validates the Gold layer Databricks notebooks against the approved project decisions and reference standards. Because the source code consists of procedural Databricks notebooks executing Spark SQL, the test harness dynamically parses and executes the notebook code within a local, session-scoped Delta-configured SparkSession.

## Test Pyramid Coverage
- **Unit & SCD**: Validates SCD Type 1 and Type 2 logic (event-time validity, `_is_current` flags).
- **Data Quality**: Ensures bad rows (e.g., negative quantities) are dropped per the `WHERE` clause filters.
- **Idempotency**: Proves that running the `MERGE` operations multiple times yields the exact same row counts and state.
- **Reconciliation**: Validates source-to-target row counts after joins and DQ filters.
- **Integration / KPIs**: Validates the exact KPI expressions (AOV, Purchase Frequency, CAC) defined in the semantic layer contract.
- **Contract**: Asserts the final schema matches the expected Gold contract.

## Prerequisites
Ensure you have the required dependencies installed: