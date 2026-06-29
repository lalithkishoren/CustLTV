# Requirements Analysis Document

## 1. Executive Summary
- **Project Overview:** Implementation of a Customer Lifetime Value (CLV) analytics solution for the Fashion E-Commerce domain to predict future customer revenue, identify high-value segments, optimize acquisition spend, and reduce churn.
- **Analysis Scope:** Assessment of the CLV KPI Requirements Specification (REQ-CLV-001) against the Source System Metadata Catalog (META-CLV-001), covering Oracle Fusion ERP, Oracle Service Cloud CRM, and a custom Marketing Platform.
- **Key Findings Summary:**
  - **Coverage Score:** 88%
  - **Total Elements Analyzed:** 54
  - **Matched Elements:** 51
  - **Gaps Found:** 8
  - **Blockers:** 0
- **Overall Readiness Assessment:** READY_WITH_CLARIFICATIONS. All identified gaps have been successfully addressed and resolved by the business and technical owners.
- **Recommendation:** The project is cleared to proceed to the Technical Design and Data Pipeline Development phases, incorporating the agreed-upon resolutions for date dimensions, historical tracking, and formula adjustments.

## 2. Document Control
- **Document ID:** RAD-20260627-001
- **Version:** 1.0
- **Status:** Draft
- **Created Date:** 2026-06-27
- **Analysis Session:** 7509da7b

## 3. Requirements Summary
### 3.1 KPIs Analyzed
- **Customer Lifetime Value (CLV):** Primary KPI. Status: Ready (Formulas and components validated).
- **Average Order Value (AOV):** Component Metric. Status: Ready (Mapped to `TOTAL_AMOUNT`).
- **Purchase Frequency:** Component Metric. Status: Ready (Formula adjusted to use `MIN()` and `MAX()` on `ORDER_DATE`).
- **Customer Lifespan:** Component Metric. Status: Ready (Formula adjusted to use `MAX(ORDER_DATE)` and SCD Type 2 tracking for churn).
- **Customer Acquisition Cost (CAC):** Component Metric. Status: Ready (Mapped to `TOTAL_SPEND`).
- **Recency:** Component Metric. Status: Ready.

### 3.2 Dimensions Analyzed
- **Customer Segment (RFM-Based):** Derived dimension. Status: Ready (Includes 'New' customers with predicted values).
- **Acquisition Channel:** Conformed dimension. Status: Ready.
- **Geographic Region:** Conformed dimension. Status: Ready (Country and Region levels explicitly ignored per business resolution).
- **Loyalty Tier:** Derived dimension. Status: Ready.
- **Time:** Conformed dimension. Status: Ready (To be generated in the analytics layer).
- **Product Category:** Conformed dimension. Status: Ready.

### 3.3 Data Sources
- **Oracle Fusion ERP (SRC-001):** Source for Orders, Inventory, and Finance data.
- **Oracle Service Cloud CRM (SRC-002):** Source for Customer Master, Support, and Survey data.
- **Marketing Platform (SRC-003):** Source for Campaign Management and Spend data.

## 4. Metadata Summary
### 4.1 Source Systems
- **SRC-001 (Oracle Fusion ERP):** Extracted via CDC (Debezium).
- **SRC-002 (Oracle Service Cloud CRM):** Extracted via CDC + Batch.
- **SRC-003 (Marketing Platform):** Extracted via API (Batch Pull).

### 4.2 Entity Coverage
- **CRM.CUSTOMERS:**
  - Key Columns: `CUSTOMER_ID`, `EMAIL`, `REGISTRATION_DATE`, `STATUS`, `CUSTOMER_TYPE`
- **CRM.CUSTOMER_REGISTRATION_SOURCE:**
  - Key Columns: `CUSTOMER_ID`, `CHANNEL`, `CAMPAIGN_ID`
- **ERP.OE_ORDER_HEADERS_ALL:**
  - Key Columns: `ORDER_ID`, `CUSTOMER_ID`, `ORDER_DATE`, `ORDER_STATUS`, `TOTAL_AMOUNT`
- **MARKETING.MARKETING_CAMPAIGNS:**
  - Key Columns: `CAMPAIGN_ID`, `TOTAL_SPEND`, `CUSTOMERS_ACQUIRED`
- **ERP.CITY_TIER_MASTER:**
  - Key Columns: `CITY`, `STATE`, `TIER`
- **CRM.SURVEYS:**
  - Key Columns: `CUSTOMER_ID`, `ORDER_ID`, `NPS_SCORE`, `NPS_CATEGORY`
- **CRM.INCIDENTS:**
  - Key Columns: `INCIDENT_ID`, `CUSTOMER_ID`, `ORDER_ID`
- **ERP.ADDRESSES:**
  - Key Columns: `SHIPPING_ADDRESS_ID`, `CITY`, `STATE`

## 5. Traceability Matrix
- **Requirement:** KPI: Customer Lifetime Value (CLV)
  - **Source Match:** CLV Calculation Lineage
  - **Match Status:** EXACT
  - **Confidence:** 100%
  - **Notes:** All components available
- **Requirement:** Metric: Average Order Value (AOV)
  - **Source Match:** TOTAL_AMOUNT
  - **Match Status:** PARTIAL
  - **Confidence:** 80%
  - **Notes:** Naming discrepancy resolved (Order Total -> TOTAL_AMOUNT)
- **Requirement:** Metric: Purchase Frequency
  - **Source Match:** ORDER_DATE
  - **Match Status:** PARTIAL
  - **Confidence:** 70%
  - **Notes:** Resolved to use MIN() and MAX() aggregations
- **Requirement:** Metric: Customer Lifespan
  - **Source Match:** REGISTRATION_DATE, ORDER_DATE
  - **Match Status:** PARTIAL
  - **Confidence:** 70%
  - **Notes:** Resolved to use MAX(ORDER_DATE) for last activity
- **Requirement:** Metric: Customer Acquisition Cost (CAC)
  - **Source Match:** TOTAL_SPEND
  - **Match Status:** PARTIAL
  - **Confidence:** 90%
  - **Notes:** Naming discrepancy resolved (Campaign_Total_Spend -> TOTAL_SPEND)
- **Requirement:** Metric: Recency
  - **Source Match:** ORDER_DATE
  - **Match Status:** EXACT
  - **Confidence:** 100%
  - **Notes:** Can be calculated from ORDER_DATE
- **Requirement:** Table: ERP.OE_ORDER_HEADERS_ALL
  - **Source Match:** ERP.OE_ORDER_HEADERS_ALL
  - **Match Status:** EXACT
  - **Confidence:** 100%
  - **Notes:** Table exists
- **Requirement:** Table: CRM.CUSTOMERS
  - **Source Match:** CRM.CUSTOMERS
  - **Match Status:** EXACT
  - **Confidence:** 100%
  - **Notes:** Table exists
- **Requirement:** Table: MARKETING.MARKETING_CAMPAIGNS
  - **Source Match:** MARKETING.MARKETING_CAMPAIGNS
  - **Match Status:** EXACT
  - **Confidence:** 100%
  - **Notes:** Table exists
- **Requirement:** Table: CRM.CUSTOMER_REGISTRATION_SOURCE
  - **Source Match:** CRM.CUSTOMER_REGISTRATION_SOURCE
  - **Match Status:** EXACT
  - **Confidence:** 100%
  - **Notes:** Table exists
- **Requirement:** Table: ERP.CITY_TIER_MASTER
  - **Source Match:** ERP.CITY_TIER_MASTER
  - **Match Status:** EXACT
  - **Confidence:** 100%
  - **Notes:** Table exists
- **Requirement:** Table: CRM.SURVEYS
  - **Source Match:** CRM.SURVEYS
  - **Match Status:** EXACT
  - **Confidence:** 100%
  - **Notes:** Table exists
- **Requirement:** Table: CRM.INCIDENTS
  - **Source Match:** CRM.INCIDENTS
  - **Match Status:** EXACT
  - **Confidence:** 100%
  - **Notes:** Table exists
- **Requirement:** Table: ERP.ADDRESSES
  - **Source Match:** ERP.ADDRESSES
  - **Match Status:** EXACT
  - **Confidence:** 100%
  - **Notes:** Table exists
- **Requirement:** Column: OE_ORDER_HEADERS_ALL.TOTAL_AMOUNT
  - **Source Match:** TOTAL_AMOUNT
  - **Match Status:** EXACT
  - **Confidence:** 100%
  - **Notes:** Column exists
- **Requirement:** Column: OE_ORDER_HEADERS_ALL.ORDER_STATUS
  - **Source Match:** ORDER_STATUS
  - **Match Status:** EXACT
  - **Confidence:** 100%
  - **Notes:** Column exists
- **Requirement:** Column: OE_ORDER_HEADERS_ALL.CUSTOMER_ID
  - **Source Match:** CUSTOMER_ID
  - **Match Status:** EXACT
  - **Confidence:** 100%
  - **Notes:** Column exists
- **Requirement:** Column: OE_ORDER_HEADERS_ALL.ORDER_DATE
  - **Source Match:** ORDER_DATE
  - **Match Status:** EXACT
  - **Confidence:** 100%
  - **Notes:** Column exists
- **Requirement:** Column: OE_ORDER_HEADERS_ALL.ORDER_ID
  - **Source Match:** ORDER_ID
  - **Match Status:** EXACT
  - **Confidence:** 100%
  - **Notes:** Column exists
- **Requirement:** Column: CUSTOMERS.REGISTRATION_DATE
  - **Source Match:** REGISTRATION_DATE
  - **Match Status:** EXACT
  - **Confidence:** 100%
  - **Notes:** Column exists
- **Requirement:** Column: CUSTOMERS.STATUS
  - **Source Match:** STATUS
  - **Match Status:** EXACT
  - **Confidence:** 100%
  - **Notes:** Column exists
- **Requirement:** Column: MARKETING_CAMPAIGNS.TOTAL_SPEND
  - **Source Match:** TOTAL_SPEND
  - **Match Status:** EXACT
  - **Confidence:** 100%
  - **Notes:** Column exists
- **Requirement:** Column: MARKETING_CAMPAIGNS.CUSTOMERS_ACQUIRED
  - **Source Match:** CUSTOMERS_ACQUIRED
  - **Match Status:** EXACT
  - **Confidence:** 100%
  - **Notes:** Column exists
- **Requirement:** Column: MARKETING_CAMPAIGNS.CAMPAIGN_ID
  - **Source Match:** CAMPAIGN_ID
  - **Match Status:** EXACT
  - **Confidence:** 100%
  - **Notes:** Column exists
- **Requirement:** Column: CUSTOMER_REGISTRATION_SOURCE.CHANNEL
  - **Source Match:** CHANNEL
  - **Match Status:** EXACT
  - **Confidence:** 100%
  - **Notes:** Column exists
- **Requirement:** Column: CUSTOMER_REGISTRATION_SOURCE.CAMPAIGN_ID
  - **Source Match:** CAMPAIGN_ID
  - **Match Status:** EXACT
  - **Confidence:** 100%
  - **Notes:** Column exists
- **Requirement:** Column: CITY_TIER_MASTER.CITY
  - **Source Match:** CITY
  - **Match Status:** EXACT
  - **Confidence:** 100%
  - **Notes:** Column exists
- **Requirement:** Column: CITY_TIER_MASTER.STATE
  - **Source Match:** STATE
  - **Match Status:** EXACT
  - **Confidence:** 100%
  - **Notes:** Column exists
- **Requirement:** Column: CITY_TIER_MASTER.TIER
  - **Source Match:** TIER
  - **Match Status:** EXACT
  - **Confidence:** 100%
  - **Notes:** Column exists
- **Requirement:** Column: CITY_TIER_MASTER.COUNTRY
  - **Source Match:** NOT_FOUND
  - **Match Status:** NOT_FOUND
  - **Confidence:** 0%
  - **Notes:** Ignored per business resolution
- **Requirement:** Column: CITY_TIER_MASTER.REGION
  - **Source Match:** NOT_FOUND
  - **Match Status:** NOT_FOUND
  - **Confidence:** 0%
  - **Notes:** Ignored per business resolution
- **Requirement:** Column: CUSTOMERS.EMAIL
  - **Source Match:** EMAIL
  - **Match Status:** EXACT
  - **Confidence:** 100%
  - **Notes:** Column exists
- **Requirement:** Column: CUSTOMERS.CUSTOMER_TYPE
  - **Source Match:** CUSTOMER_TYPE
  - **Match Status:** EXACT
  - **Confidence:** 100%
  - **Notes:** Column exists
- **Requirement:** Column: SURVEYS.NPS_SCORE
  - **Source Match:** NPS_SCORE
  - **Match Status:** EXACT
  - **Confidence:** 100%
  - **Notes:** Column exists
- **Requirement:** Column: SURVEYS.NPS_CATEGORY
  - **Source Match:** NPS_CATEGORY
  - **Match Status:** EXACT
  - **Confidence:** 100%
  - **Notes:** Column exists
- **Requirement:** Column: INCIDENTS.INCIDENT_ID
  - **Source Match:** INCIDENT_ID
  - **Match Status:** EXACT
  - **Confidence:** 100%
  - **Notes:** Column exists
- **Requirement:** Column: INCIDENTS.CUSTOMER_ID
  - **Source Match:** CUSTOMER_ID
  - **Match Status:** EXACT
  - **Confidence:** 100%
  - **Notes:** Column exists
- **Requirement:** Dimension: Customer Segment
  - **Source Match:** RFM Segment Derivation
  - **Match Status:** EXACT
  - **Confidence:** 100%
  - **Notes:** Can be derived from available data
- **Requirement:** Dimension: Acquisition Channel
  - **Source Match:** CHANNEL
  - **Match Status:** EXACT
  - **Confidence:** 100%
  - **Notes:** Available in CUSTOMER_REGISTRATION_SOURCE
- **Requirement:** Dimension: Geographic Region
  - **Source Match:** CITY_TIER_MASTER
  - **Match Status:** PARTIAL
  - **Confidence:** 60%
  - **Notes:** Missing hierarchy levels resolved by ignoring them
- **Requirement:** Dimension: Loyalty Tier
  - **Source Match:** Loyalty Tier Derivation
  - **Match Status:** EXACT
  - **Confidence:** 100%
  - **Notes:** Can be derived from cumulative spend
- **Requirement:** Dimension: Time
  - **Source Match:** NOT_FOUND
  - **Match Status:** NOT_FOUND
  - **Confidence:** 0%
  - **Notes:** Resolved by generating standard Date dimension in analytics layer
- **Requirement:** Dimension: Product Category
  - **Source Match:** ERP.CATEGORIES
  - **Match Status:** EXACT
  - **Confidence:** 100%
  - **Notes:** Available via OE_ORDER_LINES_ALL joins
- **Requirement:** Rule: BR-001
  - **Source Match:** ORDER_STATUS
  - **Match Status:** EXACT
  - **Confidence:** 100%
  - **Notes:** Can filter by ORDER_STATUS
- **Requirement:** Rule: BR-002
  - **Source Match:** ORDER_STATUS
  - **Match Status:** EXACT
  - **Confidence:** 100%
  - **Notes:** Can filter by ORDER_STATUS
- **Requirement:** Rule: BR-003
  - **Source Match:** ORDER_STATUS, SURVEYS.ORDER_ID
  - **Match Status:** EXACT
  - **Confidence:** 100%
  - **Notes:** Can join and filter
- **Requirement:** Rule: BR-004
  - **Source Match:** ORDER_ID
  - **Match Status:** EXACT
  - **Confidence:** 100%
  - **Notes:** Can count orders. Clarified to include 'New' segment.
- **Requirement:** Rule: BR-005
  - **Source Match:** STATUS
  - **Match Status:** PARTIAL
  - **Confidence:** 50%
  - **Notes:** Resolved by implementing SCD Type 2 history on STATUS
- **Requirement:** Rule: BR-006
  - **Source Match:** CHANNEL, CAMPAIGN_ID
  - **Match Status:** EXACT
  - **Confidence:** 100%
  - **Notes:** Can implement logic
- **Requirement:** Rule: DQ-001
  - **Source Match:** TOTAL_AMOUNT
  - **Match Status:** PARTIAL
  - **Confidence:** 90%
  - **Notes:** Naming discrepancy resolved
- **Requirement:** Rule: DQ-002
  - **Source Match:** CUSTOMER_ID FK
  - **Match Status:** EXACT
  - **Confidence:** 100%
  - **Notes:** FK relationship exists
- **Requirement:** Rule: DQ-003
  - **Source Match:** EMAIL
  - **Match Status:** EXACT
  - **Confidence:** 100%
  - **Notes:** Can validate format
- **Requirement:** Rule: DQ-004
  - **Source Match:** ORDER_DATE
  - **Match Status:** EXACT
  - **Confidence:** 100%
  - **Notes:** Can validate date
- **Requirement:** Rule: DQ-005
  - **Source Match:** NPS_SCORE
  - **Match Status:** EXACT
  - **Confidence:** 100%
  - **Notes:** Can validate range

## 6. Gap Analysis
### 6.1 Summary by Severity
- **BLOCKER:** 0 gaps
- **HIGH:** 2 gaps
- **MEDIUM:** 4 gaps
- **LOW:** 2 gaps

### 6.2 Detailed Gaps
- **GAP-001** (HIGH) - MISSING_COLUMN
  - **Description:** The Geographic Region dimension requires Country and Region levels, but the CITY_TIER_MASTER table only contains CITY, STATE, and TIER.
  - **Resolution:** Ignore country and region.
- **GAP-002** (HIGH) - MISSING_SOURCE_TABLE
  - **Description:** A conformed Time dimension is required for trend and cohort analysis, but no date/time master table is defined in the source metadata.
  - **Resolution:** Generate a standard Date dimension table in the analytics layer with the required hierarchy (Year -> Quarter -> Month -> Week -> Day).
- **GAP-003** (MEDIUM) - INCOMPLETE_FORMULA
  - **Description:** The formula references 'First_Order' and 'Last_Order' which are not explicit columns in the source tables.
  - **Resolution:** Use MIN() and MAX() aggregations on ORDER_DATE.
- **GAP-004** (MEDIUM) - INCOMPLETE_FORMULA
  - **Description:** The formula references 'Last_Activity_Date' which is not an explicit column in the CUSTOMERS table.
  - **Resolution:** Use MAX(ORDER_DATE).
- **GAP-005** (MEDIUM) - MISSING_COLUMN
  - **Description:** The business rule references a 'churn date' to cap customer lifespan, but no such column exists in the CUSTOMERS table.
  - **Resolution:** Implement logic to track the timestamp of STATUS changes to 'Churned' (e.g., using SCD Type 2 history).
- **GAP-006** (LOW) - NAMING_DISCREPANCY
  - **Description:** Requirements refer to 'Order Total' and 'ORDER_TOTAL', but the physical column in the ERP system is named 'TOTAL_AMOUNT'.
  - **Resolution:** TOTAL_AMOUNT is the correct column to use for Order Total.
- **GAP-007** (LOW) - NAMING_DISCREPANCY
  - **Description:** The CAC formula refers to 'Campaign_Total_Spend', but the physical column is named 'TOTAL_SPEND'.
  - **Resolution:** Map Campaign_Total_Spend to TOTAL_SPEND.
- **GAP-008** (MEDIUM) - AMBIGUOUS_REQUIREMENT
  - **Description:** BR-004 states customers need at least 1 completed order for CLV calculation, but the RFM segmentation includes a 'New' segment with 0 orders and a 'Predicted' expected CLV.
  - **Resolution:** 'New' customers (0 orders) will be included in the CLV pipeline with a predicted value.

## 7. Clarifications Collected
- **Gap ID:** GAP-001
  - **Question:** Should Country and Region be derived from State, or is there another reference table for the full geographic hierarchy?
  - **User Response:** ignore country and region
- **Gap ID:** GAP-002
  - **Question:** Is there an existing Date/Time dimension in the Data Warehouse that can be used, or does it need to be generated?
  - **User Response:** Generate a standard Date dimension table in the analytics layer with the required hierarchy (Year -> Quarter -> Month -> Week -> Day).
- **Gap ID:** GAP-003
  - **Question:** Should First_Order and Last_Order be calculated dynamically using MIN(ORDER_DATE) and MAX(ORDER_DATE) per customer?
  - **User Response:** use MIN() and MAX() aggregations on ORDER_DATE.
- **Gap ID:** GAP-004
  - **Question:** How should Last_Activity_Date be defined? Is it the MAX(ORDER_DATE) or does it include CRM interactions/logins?
  - **User Response:** MAX(ORDER_DATE)
- **Gap ID:** GAP-005
  - **Question:** Is churn date recorded in another system, or should it be inferred based on the date the customer STATUS changed to 'Churned'?
  - **User Response:** Implement logic to track the timestamp of STATUS changes to 'Churned' (e.g., using SCD Type 2 history).
- **Gap ID:** GAP-006
  - **Question:** Can we confirm that TOTAL_AMOUNT is the correct column to use for Order Total?
  - **User Response:** TOTAL_AMOUNT is the correct column to use for Order Total
- **Gap ID:** GAP-007
  - **Question:** Can we confirm that TOTAL_SPEND in the MARKETING_CAMPAIGNS table represents the Campaign_Total_Spend?
  - **User Response:** Map Campaign_Total_Spend to TOTAL_SPEND.
- **Gap ID:** GAP-008
  - **Question:** Should 'New' customers (0 orders) be included in the CLV pipeline with a predicted value, or excluded entirely as per BR-004?
  - **User Response:** 'New' customers (0 orders) be included in the CLV pipeline with a predicted value

## 8. Technical Specifications
### 8.1 Confirmed Source-to-Target Mappings
- **AOV Numerator:** Maps to `ERP.OE_ORDER_HEADERS_ALL.TOTAL_AMOUNT`
- **CAC Numerator:** Maps to `MARKETING.MARKETING_CAMPAIGNS.TOTAL_SPEND`
- **First Order Date:** Maps to `MIN(ERP.OE_ORDER_HEADERS_ALL.ORDER_DATE)` grouped by `CUSTOMER_ID`
- **Last Order Date / Last Activity Date:** Maps to `MAX(ERP.OE_ORDER_HEADERS_ALL.ORDER_DATE)` grouped by `CUSTOMER_ID`
- **Churn Date:** Maps to the `valid_from` timestamp of the SCD Type 2 record where `CRM.CUSTOMERS.STATUS` becomes 'Churned'

### 8.2 Validated Business Rules
- **BR-001 & BR-002:** Filter `ERP.OE_ORDER_HEADERS_ALL` where `ORDER_STATUS NOT IN ('Cancelled', 'Returned')` before calculating AOV and Monetary values.
- **BR-004 (Updated):** Customers with 0 orders are included in the pipeline specifically for the 'New' segment to receive a predicted CLV. Historical CLV calculations require ≥ 1 order.
- **BR-005 (Updated):** Customer lifespan calculation uses `DATEDIFF(MONTH, REGISTRATION_DATE, MAX(ORDER_DATE))` capped at the inferred churn date derived from SCD Type 2 history.
- **DQ-001:** Validate `TOTAL_AMOUNT > 0`.

### 8.3 Join Specifications
- **Customer to Orders:** `CRM.CUSTOMERS.CUSTOMER_ID = ERP.OE_ORDER_HEADERS_ALL.CUSTOMER_ID`
- **Customer to Acquisition:** `CRM.CUSTOMERS.CUSTOMER_ID = CRM.CUSTOMER_REGISTRATION_SOURCE.CUSTOMER_ID`
- **Acquisition to Campaign:** `CRM.CUSTOMER_REGISTRATION_SOURCE.CAMPAIGN_ID = MARKETING.MARKETING_CAMPAIGNS.CAMPAIGN_ID`
- **Orders to Geography:** `ERP.OE_ORDER_HEADERS_ALL.SHIPPING_ADDRESS_ID = ERP.ADDRESSES.ADDRESS_ID` (Implicit via Address ID) followed by `ERP.ADDRESSES.CITY = ERP.CITY_TIER_MASTER.CITY AND ERP.ADDRESSES.STATE = ERP.CITY_TIER_MASTER.STATE`
- **Customer to Surveys:** `CRM.CUSTOMERS.CUSTOMER_ID = CRM.SURVEYS.CUSTOMER_ID`

## 9. Recommendations
### 9.1 Immediate Next Steps
1. **Data Engineering:** Develop a script to generate the conformed Date Dimension table (Year -> Quarter -> Month -> Week -> Day) in the analytics layer.
2. **Data Engineering:** Implement SCD Type 2 tracking on the `CRM.CUSTOMERS` table specifically to capture the exact timestamp when `STATUS` changes to 'Churned'.
3. **Data Modeling:** Update the logical data model to reflect the removal of Country and Region from the Geographic hierarchy.
4. **Analytics:** Adjust the CLV pipeline logic to ensure 'New' customers (0 orders) bypass historical calculation steps and are routed directly to the predictive model.

### 9.2 Prerequisites Before Development
- Ensure CDC (Debezium) is properly configured to capture historical changes for `CRM.CUSTOMERS` to support the SCD Type 2 requirement.
- Confirm the target analytics platform (e.g., Snowflake, Databricks) has the necessary role-based access controls for the Marketing, ERP, and CRM schemas.

### 9.3 Risk Areas to Monitor
- **SCD Type 2 Reliance:** The accuracy of the Customer Lifespan metric now heavily relies on the CRM system updating the `STATUS` field promptly and the CDC pipeline capturing it without data loss.
- **Predictive CLV for 'New' Customers:** Since 'New' customers have no order history, the predictive model will rely entirely on acquisition channel, demographic data, and campaign metadata. Model accuracy for this specific segment should be monitored closely against the 85% target.

## 10. Sign-Off
- **Business Owner:** _________________ Date: _______
- **Technical Lead:** _________________ Date: _______
- **Data Architect:** _________________ Date: _______

---
*Document generated by Ascend AI Requirements Analysis Agent*