**CUSTOMER LIFETIME VALUE (CLV) ANALYTICS - DATA PLATFORM BLUEPRINT**

**1. Executive Summary**

This blueprint outlines the architecture and design for the Customer Lifetime Value (CLV) Analytics platform. The solution is designed to predict future customer revenue, identify high-value segments, optimize acquisition spend, and reduce churn, targeting an 85% CLV prediction accuracy and 100% acquisition cost visibility. 

The architecture leverages a modern, cloud-native Azure data stack centered around Azure Databricks and Delta Live Tables (DLT) for scalable data processing, Azure Data Factory (ADF) for orchestration, ADLS Gen2 for storage, Unity Catalog for governance, and Power BI for visualization. 

**Well-Architected Alignment Summary:**
*   **Reliability:** Idempotent Delta Live Tables pipelines, automated checkpointing, and robust retry mechanisms in ADF ensure consistent state and fault tolerance.
*   **Security:** Unity Catalog provides centralized, fine-grained access control, while Azure Key Vault secures credentials. Network isolation is enforced via Private Endpoints.
*   **Cost Optimization:** Databricks serverless/auto-scaling compute, ADLS lifecycle policies, and Delta Lake optimization prevent resource waste.
*   **Operational Excellence:** Infrastructure as Code (IaC), CI/CD deployment of DLT pipelines, and centralized Azure Monitor alerting ensure maintainability.
*   **Performance Efficiency:** Delta Lake Z-ordering, partition pruning, and DLT Enhanced Autoscaling guarantee sub-5-second dashboard response times and meet the <5 minute CDC latency SLA.

---

**2. Architecture Design (with Reliability & Performance)**

The platform follows a Medallion Architecture (Bronze, Silver, Gold) implemented on ADLS Gen2 and processed via Databricks Delta Live Tables.

*   **Raw/Landing Layer (Bronze)**
    *   **Design:** Immutable append-only storage in ADLS Gen2. Captures raw CDC streams from Oracle Fusion ERP (SRC-001) and Oracle Service Cloud CRM (SRC-002), and batch extracts from the Marketing Platform (SRC-003).
    *   **Reliability:** Utilizes Databricks Auto Loader for robust, stateful streaming ingestion with schema evolution tracking. Dead-letter queues capture unparseable files.
    *   **Performance:** Asynchronous, parallel file reading. Batch sizing is optimized for continuous CDC streams to meet the <5 minute latency requirement.

*   **Curated/Processed Layer (Silver)**
    *   **Design:** Cleansed, conformed, and validated data. Enforces Data Quality (DQ) rules using DLT Expectations.
    *   **Reliability:** Idempotent MERGE operations ensure safe re-runs. DLT `APPLY CHANGES INTO` handles out-of-order CDC updates for entities like OE_ORDER_HEADERS_ALL and CUSTOMERS.
    *   **Performance:** Tables are partitioned by date (e.g., ORDER_DATE) and Z-ordered by high-cardinality join keys like CUSTOMER_ID.

*   **Business/Analytics Layer (Gold)**
    *   **Design:** Dimensional models and aggregated facts tailored for CLV calculations.
    *   **Reliability:** Materialized views ensure read consistency for Power BI. Strict referential integrity checks (e.g., DQ-R-001) are enforced before aggregation.
    *   **Performance:** Pre-calculated RFM segments, AOV, and CAC metrics eliminate complex runtime calculations, ensuring Power BI dashboards load in <5 seconds.

---

**3. Data Pipeline Design**

*   **Ingestion Patterns:**
    *   **Streaming (CDC):** Oracle ERP (OE_ORDER_HEADERS_ALL, OE_ORDER_LINES_ALL) and CRM (CUSTOMERS, INCIDENTS) changes are captured via Debezium and ingested continuously using Databricks Auto Loader to meet the <5 minute SLA.
    *   **Batch:** MARKETING_CAMPAIGNS and SURVEYS are ingested daily via ADF Copy Activities, triggering downstream DLT batch pipelines.

*   **Transformation & Data Quality Logic (Silver):**
    *   **DQ-001 & DQ-V-001:** DLT Expectation on OE_ORDER_HEADERS_ALL ensures TOTAL_AMOUNT > 0. Violations are dropped (`expect_or_drop`).
    *   **DQ-002 & DQ-R-001:** DLT Expectation ensures CUSTOMER_ID in orders exists in the CUSTOMERS table. Violations are quarantined into a dead-letter table for review.
    *   **DQ-V-004:** Regex validation on CUSTOMERS.EMAIL. Invalid formats are flagged (`expect`).
    *   **BR-ORD-002:** Filter applied to exclude ORDER_STATUS IN ('Cancelled', 'Returned') from revenue-generating downstream tables.

*   **Analytics Flows & KPI Calculations (Gold):**
    *   **AOV Calculation:** Aggregation of OE_ORDER_HEADERS_ALL (SUM(TOTAL_AMOUNT) / COUNT(ORDER_ID)) grouped by CUSTOMER_ID.
    *   **Purchase Frequency:** COUNT(ORDER_ID) divided by DATEDIFF(MONTH, REGISTRATION_DATE, MAX(ORDER_DATE)).
    *   **CAC Attribution:** Join CUSTOMER_REGISTRATION_SOURCE.CAMPAIGN_ID to MARKETING_CAMPAIGNS. Calculate CAC as TOTAL_SPEND / CUSTOMERS_ACQUIRED.
    *   **CLV Calculation:** Final materialized view combining the above: (AOV * Purchase Frequency * Predicted Lifespan) - CAC.
    *   **SCD Handling:** DIM_CUSTOMER_SEGMENT and DIM_LOYALTY_TIER use SCD Type 2 to track historical tier changes. DIM_ACQUISITION_CHANNEL uses SCD Type 1.

---

**4. Security Implementation**

*   **Identity & Access Management:** Unity Catalog acts as the central governance layer. Access is granted via RBAC using Azure Entra ID (Active Directory) groups. Service Principals are used for ADF-to-Databricks authentication.
*   **Data Protection:** ADLS Gen2 is configured with AES-256 encryption at rest. All data in transit between ADF, Databricks, and Power BI uses TLS 1.2+.
*   **Network Security:** Azure Private Endpoints are deployed for ADLS Gen2, Azure Key Vault, and the Databricks workspace. Databricks is deployed with VNet injection to ensure no public IP exposure.
*   **Secrets Management:** Azure Key Vault stores Oracle JDBC connection strings, Marketing API tokens, and Service Principal secrets. Databricks Secret Scopes are backed by Key Vault.
*   **Audit & Compliance:** Unity Catalog system tables capture all data access logs. Azure Monitor diagnostic settings route platform logs to a central Log Analytics workspace for compliance auditing.
*   **Data Classification & Masking:** Unity Catalog Dynamic Data Masking is applied to PII columns (CUSTOMERS.EMAIL, CUSTOMERS.PHONE, CUSTOMERS.FIRST_NAME, CUSTOMERS.LAST_NAME) to obscure data for non-privileged analytical users.

---

**5. Operational Excellence**

*   **Infrastructure as Code (IaC):** All Azure resources (ADF, Databricks, Key Vault, ADLS) are provisioned using Bicep templates.
*   **CI/CD Pipelines:** Azure DevOps pipelines manage the deployment lifecycle. Databricks Asset Bundles (DABs) are used to package, test, and deploy DLT pipelines across Dev, Test, and Prod environments.
*   **Monitoring & Alerting:** Azure Monitor tracks ADF pipeline runs. Databricks SQL dashboards monitor DLT pipeline health, data quality expectation pass/fail rates, and CDC latency. Alerts are configured for SLA breaches (e.g., latency > 5 mins).
*   **Logging & Tracing:** Correlation IDs are passed from ADF to Databricks to trace end-to-end execution. Logs are centralized in Azure Log Analytics.
*   **Runbooks:** Documented procedures are maintained for handling DQ quarantine table processing, resolving schema evolution conflicts, and manual pipeline restarts.
*   **Change Management:** All schema changes are managed via Unity Catalog and deployed through automated, version-controlled pull requests.

---

**6. Cost Optimization Strategy**

*   **Resource Right-sizing:** DLT pipelines utilize Enhanced Autoscaling, dynamically allocating compute based on the incoming CDC volume and scaling down to zero during idle periods.
*   **Storage Tiering:** ADLS Gen2 lifecycle management policies automatically transition raw Bronze data older than 30 days to the Cool tier, and older than 1 year to the Archive tier.
*   **Compute Efficiency:** Spot instances are utilized for the daily batch processing of MARKETING_CAMPAIGNS and SURVEYS, reducing compute costs for non-time-sensitive workloads.
*   **Cost Monitoring:** Azure Cost Management budgets are set per resource group. Unity Catalog system tables (billing logs) are analyzed via Databricks SQL to provide chargeback visibility by pipeline and business domain.

---

**7. Reliability & Disaster Recovery**

*   **High Availability:** ADLS Gen2 is configured with Zone-Redundant Storage (ZRS) to protect against datacenter-level failures. Databricks clusters span multiple availability zones.
*   **Backup Strategy:** ADLS Gen2 soft delete is enabled with a 14-day retention period. Delta Lake time travel is configured for 7 days on Silver/Gold tables to allow point-in-time recovery from accidental logical corruptions.
*   **Failover Mechanisms:** In the event of a regional outage, IaC templates can rapidly provision the compute environment in a secondary region, pointing to geo-replicated storage (if upgraded to GZRS based on business criticality).
*   **RTO/RPO Targets:** RPO is < 5 minutes for CDC data and 24 hours for batch data. RTO is targeted at < 4 hours for full platform restoration.
*   **Data Validation:** Automated reconciliation queries run daily to compare Gold layer TOTAL_AMOUNT aggregates against source ERP control totals.

---

**8. Performance Optimization**

*   **Data Layout:** All Silver and Gold tables use Delta Lake format. OE_ORDER_HEADERS_ALL and OE_ORDER_LINES_ALL are partitioned by Year/Month of ORDER_DATE.
*   **Z-Ordering:** High-cardinality columns frequently used in joins and filters, such as CUSTOMER_ID and CAMPAIGN_ID, are Z-ordered to maximize data skipping.
*   **Caching Strategy:** Databricks disk caching is enabled on compute clusters. Power BI utilizes Import mode for the high-level "CLV Executive Summary" dashboard for instant load times, and DirectQuery for granular, real-time operational drill-downs.
*   **Query Optimization:** Unity Catalog Predictive Optimization automatically schedules OPTIMIZE (compaction) and VACUUM operations during off-peak hours to maintain optimal file sizes.
*   **Scaling Configuration:** DLT pipelines are configured with strict maximum cluster sizes to prevent runaway costs while allowing sufficient horizontal scaling to handle peak order volumes (e.g., during sales events).

---

**9. Orchestration Design**

*   **Pipeline Patterns:** Azure Data Factory serves as the master orchestrator. It triggers Databricks DLT pipelines via the Databricks REST API/Linked Service.
*   **Dependencies:** 
    *   Continuous CDC pipelines run independently.
    *   The daily batch pipeline enforces dependencies: MARKETING_CAMPAIGNS must complete ingestion before the Gold CAC attribution and CLV calculation pipelines execute.
*   **Retry Policies:** ADF activities are configured with a 3-retry policy and a 5-minute exponential backoff for transient API or network failures.
*   **Error Handling:** Pipeline failures in ADF trigger a Webhook activity that sends formatted alerts to an Azure Monitor Action Group, notifying the Data Engineering team via Microsoft Teams and Email.

---

**10. Incremental Loading Strategy**

*   **Change Detection:** Debezium captures row-level changes (Inserts, Updates, Deletes) from Oracle databases. Databricks Auto Loader uses directory listing and file notification to incrementally process new JSON/Avro files.
*   **Late-Arriving Data:** Delta Lake MERGE operations handle late-arriving updates seamlessly. For example, if an order status changes to 'Delivered' days later, the Silver OE_ORDER_HEADERS_ALL table is updated based on the ORDER_ID business key.
*   **Upsert Strategies:** DLT `APPLY CHANGES INTO` is used for the CUSTOMERS and OE_ORDER_HEADERS_ALL tables. It automatically handles upserts, deduplication, and sequencing based on the source system's commit timestamp, ensuring the Silver layer accurately reflects the source state.

---

**11. Implementation Roadmap**

*   **Phase 1: Foundation & Ingestion (Weeks 1-4)**
    *   Deploy Azure infrastructure via Bicep (ADLS, Key Vault, Databricks, ADF).
    *   Configure Unity Catalog and RBAC.
    *   Implement CDC and Batch ingestion to the Bronze layer.
*   **Phase 2: Silver Layer & Data Quality (Weeks 5-8)**
    *   Develop DLT pipelines for Silver layer cleansing.
    *   Implement DQ rules (DQ-001 to DQ-005) and quarantine handling.
    *   Establish cross-system keys (CUSTOMER_ID mapping).
*   **Phase 3: Gold Layer & CLV Logic (Weeks 9-16)**
    *   Develop dimensional models (RFM Segment, Loyalty Tier).
    *   Implement complex business rules for AOV, Purchase Frequency, and CAC.
    *   Finalize the CLV calculation materialized views.
*   **Phase 4: Visualization & Operationalization (Weeks 17-24)**
    *   Develop Power BI dashboards (Executive Summary, Acquisition ROI).
    *   Implement CI/CD pipelines and Azure Monitor alerting.
    *   Conduct Well-Architected review and performance load testing.

---

**12. Well-Architected Framework Checklist**

**Azure Data Factory (ADF)**
*   **Reliability:** Configured with retry logic, exponential backoff, and dependency chaining.
*   **Security:** Authenticates to Databricks and ADLS via Managed Identities/Service Principals.
*   **Cost Optimization:** Triggers are scheduled efficiently; avoids unnecessary polling.
*   **Operational Excellence:** Pipelines deployed via ARM/Bicep; integrated with Azure Monitor.
*   **Performance Efficiency:** Uses parallel copy activities for batch data extraction.

**Azure Databricks & Delta Live Tables (DLT)**
*   **Reliability:** DLT provides declarative, idempotent pipelines with built-in checkpointing and state management.
*   **Security:** VNet injected; utilizes Secret Scopes backed by Key Vault.
*   **Cost Optimization:** Enhanced Autoscaling and Serverless compute minimize idle time.
*   **Operational Excellence:** Deployed via Databricks Asset Bundles; built-in DQ expectations.
*   **Performance Efficiency:** Photon engine enabled; automatic partition pruning and broadcast joins for dimensional lookups.

**ADLS Gen2 (Storage)**
*   **Reliability:** ZRS replication and soft-delete enabled for data durability.
*   **Security:** AES-256 encryption at rest; Private Endpoint network isolation.
*   **Cost Optimization:** Lifecycle management policies move stale Bronze data to Cool/Archive tiers.
*   **Operational Excellence:** Hierarchical namespace organizes Medallion layers cleanly.
*   **Performance Efficiency:** Optimized directory structures prevent file-listing bottlenecks.

**Unity Catalog**
*   **Reliability:** Centralized metadata prevents schema drift and synchronization issues.
*   **Security:** Enforces fine-grained RBAC and Dynamic Data Masking on PII (Email, Phone).
*   **Cost Optimization:** System tables provide visibility into compute and storage usage for chargeback.
*   **Operational Excellence:** Centralized data lineage tracking from ERP/CRM to Power BI.
*   **Performance Efficiency:** Predictive Optimization automates VACUUM and OPTIMIZE maintenance tasks.

**Azure Key Vault**
*   **Reliability:** Highly available regional deployment.
*   **Security:** Eliminates hardcoded credentials; strict access policies applied.
*   **Cost Optimization:** Minimal cost footprint; billed per transaction.
*   **Operational Excellence:** Centralized secret rotation and management.
*   **Performance Efficiency:** Secrets cached by Databricks Secret Scopes to prevent API throttling.

**Azure Monitor**
*   **Reliability:** Independent monitoring plane ensures visibility even during platform degradation.
*   **Security:** Diagnostic logs are immutable and retained for compliance auditing.
*   **Cost Optimization:** Log retention policies configured to balance cost and compliance.
*   **Operational Excellence:** Centralized dashboards, SLI/SLO tracking, and automated Action Groups.
*   **Performance Efficiency:** Near real-time metric ingestion for rapid incident response.

**Power BI**
*   **Reliability:** Published to Premium workspaces with scheduled refresh retries.
*   **Security:** Row-Level Security (RLS) applied based on Entra ID groups (e.g., Regional Managers only see their region).
*   **Cost Optimization:** Balances Import mode (free compute) vs. DirectQuery (Databricks compute) based on use case.
*   **Operational Excellence:** Version controlled via PBIP formats and deployed via deployment pipelines.
*   **Performance Efficiency:** Aggregated Gold tables ensure visuals render in <5 seconds without complex DAX overhead.