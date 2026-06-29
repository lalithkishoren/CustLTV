# Data Governance & Security Blueprint

## 1. Executive Summary
This document outlines the Governance and Security implementation for the Myntra Customer Lifetime Value (CLV) Data Platform on Azure. The security model enforces a **Zero-Trust Architecture** using Microsoft Entra ID, Azure Key Vault, and Databricks Unity Catalog. 

Data is progressively governed across the Medallion architecture, ensuring that raw data is immutable and restricted, while business-ready data is democratized securely.

## 2. Role-Based Access Control (RBAC) Archetypes
Access is managed via Entra ID groups synced to Databricks Account/Workspace level, mapped to Unity Catalog privileges.

| Archetype / Entra ID Group | Description | Unity Catalog Privileges |
| :--- | :--- | :--- |
| `sp_adf_etl` | Service Principal for Azure Data Factory orchestration. | `USE CATALOG`, `USE SCHEMA`, `CREATE TABLE`, `MODIFY`, `SELECT` across all layers. |
| `data_engineers` | Platform engineers building and debugging DLT pipelines. | `ALL PRIVILEGES` on Dev/Test. `USE CATALOG`, `USE SCHEMA`, `SELECT` on Prod. |
| `data_stewards` | Governance team responsible for data quality and PII access. | `USE CATALOG`, `USE SCHEMA`, `SELECT` (Unmasked) across all layers. |
| `data_analysts` | BI Developers and Data Scientists consuming Gold data. | `USE CATALOG`, `USE SCHEMA`, `SELECT` (Masked) on Gold layer only. |

## 3. Data Classification & Masking Strategy
Data is classified into three tiers using Unity Catalog Tags. Microsoft Purview is configured to scan Unity Catalog to maintain the enterprise data map.

*   **Public**: Non-sensitive data (e.g., Date dimensions, City Tiers).
*   **Confidential**: Business-sensitive data (e.g., Revenue, Order Amounts, CAC). Restricted to authorized business units.
*   **Restricted_PII**: Personally Identifiable Information (e.g., Customer Name, Email, Phone). 
    *   *Policy*: Dynamic Column Masking is applied at the Silver and Gold layers. Only `data_stewards` and `sp_adf_etl` can view unmasked PII. `data_analysts` see masked values (e.g., `j***@email.com`).

## 4. Lineage & Catalog Registration
*   **Lineage**: Unity Catalog automatically captures table, column, and notebook-level lineage for all DLT pipelines and Spark SQL executions.
*   **Catalog Registration**: All tables are registered with `COMMENT` metadata defining the business semantic meaning, ensuring the Gold layer acts as a governed semantic layer for Power BI and AI consumers.