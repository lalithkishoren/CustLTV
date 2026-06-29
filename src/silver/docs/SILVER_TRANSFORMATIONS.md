# Silver Layer Transformations & Data Quality

## Overview
The Silver layer applies business rules, standardizes data types, and enforces data quality. It operates on a **Progressive Trust Model**: valid rows are merged into Silver, while invalid rows (ERROR severity) are quarantined.

## Transformation Rules
Transformations are driven dynamically by `control.silver_transformation_rules`.

### Supported Rule Types
- **CAST**: Changes data type (e.g., `BIGINT`, `TIMESTAMP`).
- **EXPRESSION**: Applies a PySpark SQL expression (e.g., `LOWER(TRIM(EMAIL))`).
- **RENAME**: Renames a column.

### Adding a New Rule
Insert a new record into `control.silver_transformation_rules`: