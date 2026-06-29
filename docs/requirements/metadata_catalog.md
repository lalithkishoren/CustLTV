# Customer Lifetime Value (CLV) Analytics
## Source System Metadata Catalog

---

## Document Control

| Attribute | Value |
|-----------|-------|
| Document ID | META-CLV-001 |
| Version | 1.0 |
| Created Date | November 2025 |
| Last Updated | November 2025 |
| Status | Draft / In Review / Approved |
| Data Steward | |
| Technical Owner | VP Data Engineering & Analytics |

---

## 1. Source System Registry

### 1.1 System Overview

| System ID | System Name | Platform | Business Domain | Data Steward | Technical Contact |
|-----------|-------------|----------|-----------------|--------------|-------------------|
| SRC-001 | Oracle Fusion ERP | Oracle Cloud | Orders, Inventory, Finance | Finance Team | ERP Admin |
| SRC-002 | Oracle Service Cloud | Oracle CRM | Customer Master, Support | CX Team | CRM Admin |
| SRC-003 | Marketing Platform | Custom | Campaign Management | Marketing Team | Marketing Ops |

### 1.2 System Connectivity

| System ID | Connection Type | Protocol | Extraction Method | Credentials Store |
|-----------|-----------------|----------|-------------------|-------------------|
| SRC-001 | Database | JDBC/ODBC | CDC (Debezium) | Azure Key Vault |
| SRC-002 | Database | JDBC/ODBC | CDC + Batch | Azure Key Vault |
| SRC-003 | API | REST/HTTPS | Batch Pull | Azure Key Vault |

---

## 2. Entity Catalog

### 2.1 Entity Summary for CLV

| Entity ID | Entity Name | Source System | Schema.Table | Record Count | CLV Relevance |
|-----------|-------------|---------------|--------------|--------------|---------------|
| ENT-001 | Customer | SRC-002 | CRM.CUSTOMERS | ~10,000 | Identity, Lifespan |
| ENT-002 | Customer Registration | SRC-002 | CRM.CUSTOMER_REGISTRATION_SOURCE | ~10,000 | Acquisition Channel |
| ENT-003 | Order Header | SRC-001 | ERP.OE_ORDER_HEADERS_ALL | ~56,000 | Revenue, Frequency |
| ENT-004 | Order Line | SRC-001 | ERP.OE_ORDER_LINES_ALL | ~116,000 | Basket Analysis |
| ENT-005 | Address | SRC-001 | ERP.ADDRESSES | ~14,000 | Geography |
| ENT-006 | City Tier | SRC-001 | ERP.CITY_TIER_MASTER | ~100 | Geographic Tier |
| ENT-007 | Product | SRC-001 | ERP.MTL_SYSTEM_ITEMS_B | ~2,000 | Category Affinity |
| ENT-008 | Category | SRC-001 | ERP.CATEGORIES | ~100 | Product Hierarchy |
| ENT-009 | Brand | SRC-001 | ERP.BRANDS | ~100 | Brand Affinity |
| ENT-010 | Campaign | SRC-003 | MARKETING.MARKETING_CAMPAIGNS | ~50 | Acquisition Cost |
| ENT-011 | Incident | SRC-002 | CRM.INCIDENTS | ~8,000 | Churn Signal |
| ENT-012 | Interaction | SRC-002 | CRM.INTERACTIONS | ~25,000 | Engagement |
| ENT-013 | Survey | SRC-002 | CRM.SURVEYS | ~11,000 | NPS Score |

---

## 3. Detailed Entity Metadata

### 3.1 CRM.CUSTOMERS (ENT-001)

#### Entity Description
| Attribute | Value |
|-----------|-------|
| **Entity Name** | CUSTOMERS |
| **Business Name** | Customer Master |
| **Description** | Golden record for customer identity and profile. Single source of truth for customer data across the enterprise. |
| **Source System** | SRC-002 (Oracle Service Cloud CRM) |
| **Schema** | CRM |
| **Owner** | Customer Experience Team |
| **Data Steward** | |
| **Update Frequency** | Real-time (CDC) |
| **Retention Period** | 7 years post-last activity |

#### Column Specifications

| Column Name | Data Type | Length | Nullable | Default | PK | FK | Business Definition |
|-------------|-----------|--------|----------|---------|----|----|---------------------|
| CUSTOMER_ID | BIGINT | - | No | Identity | ✓ | - | Unique system-generated customer identifier |
| EMAIL | NVARCHAR | 255 | No | - | - | - | Primary email address (unique, used for login) |
| PHONE | NVARCHAR | 20 | Yes | - | - | - | Primary contact phone with country code |
| FIRST_NAME | NVARCHAR | 100 | No | - | - | - | Customer's given name |
| LAST_NAME | NVARCHAR | 100 | Yes | - | - | - | Customer's family name |
| GENDER | NVARCHAR | 10 | Yes | - | - | - | Customer's gender identity |
| DATE_OF_BIRTH | DATE | - | Yes | - | - | - | Customer's birth date |
| REGISTRATION_DATE | DATETIME2 | - | No | - | - | - | Account creation timestamp |
| CUSTOMER_TYPE | NVARCHAR | 20 | No | 'Regular' | - | - | Customer classification (VIP/Regular/New) |
| STATUS | NVARCHAR | 20 | No | 'Active' | - | - | Account status (Active/Inactive/Churned/Blocked) |
| EMAIL_VERIFIED | BIT | - | No | 0 | - | - | Email verification flag |
| PHONE_VERIFIED | BIT | - | No | 0 | - | - | Phone verification flag |
| MARKETING_OPT_IN | BIT | - | No | 1 | - | - | Marketing consent flag |
| PREFERRED_LANGUAGE | NVARCHAR | 20 | No | 'English' | - | - | Communication language preference |
| CREATED_BY | NVARCHAR | 50 | No | 'SYSTEM' | - | - | Record creator |
| CREATED_DATE | DATETIME2 | - | No | GETDATE() | - | - | Record creation timestamp |
| LAST_UPDATE_DATE | DATETIME2 | - | No | GETDATE() | - | - | Last modification timestamp |

#### Valid Values

| Column | Valid Values | Description |
|--------|--------------|-------------|
| GENDER | Male, Female, Other, Prefer not to say | Gender options |
| CUSTOMER_TYPE | VIP, Regular, New | Customer tier classification |
| STATUS | Active, Inactive, Churned, Blocked | Account lifecycle status |

#### CLV Usage

| Column | CLV Component | Usage |
|--------|---------------|-------|
| CUSTOMER_ID | All | Primary key for joining |
| REGISTRATION_DATE | Lifespan | Customer tenure start date |
| STATUS | Lifespan | Determines if customer is active |
| CUSTOMER_TYPE | Segment | Input for segmentation |

#### Sample Data

| CUSTOMER_ID | EMAIL | FIRST_NAME | REGISTRATION_DATE | CUSTOMER_TYPE | STATUS |
|-------------|-------|------------|-------------------|---------------|--------|
| 1001 | aarav.sharma@gmail.com | Aarav | 2023-03-15 10:30:00 | Regular | Active |
| 1002 | priya.patel@yahoo.com | Priya | 2023-01-22 14:45:00 | VIP | Active |
| 1003 | rahul.kumar@outlook.com | Rahul | 2024-06-10 09:15:00 | New | Active |

---

### 3.2 CRM.CUSTOMER_REGISTRATION_SOURCE (ENT-002)

#### Entity Description
| Attribute | Value |
|-----------|-------|
| **Entity Name** | CUSTOMER_REGISTRATION_SOURCE |
| **Business Name** | Acquisition Attribution |
| **Description** | Captures the acquisition channel and campaign attribution for each customer at the time of registration |
| **Source System** | SRC-002 (Oracle Service Cloud CRM) |
| **Schema** | CRM |
| **Owner** | Marketing Team |
| **Update Frequency** | At registration (one-time) |

#### Column Specifications

| Column Name | Data Type | Length | Nullable | Default | PK | FK | Business Definition |
|-------------|-----------|--------|----------|---------|----|----|---------------------|
| REGISTRATION_SOURCE_ID | BIGINT | - | No | Identity | ✓ | - | Unique record identifier |
| CUSTOMER_ID | BIGINT | - | No | - | - | ✓ → CUSTOMERS | Link to customer master |
| CHANNEL | NVARCHAR | 50 | No | - | - | - | Acquisition channel |
| CAMPAIGN_ID | INT | - | Yes | - | - | ✓ → CAMPAIGNS | Associated marketing campaign |
| UTM_SOURCE | NVARCHAR | 100 | Yes | - | - | - | UTM source parameter |
| UTM_MEDIUM | NVARCHAR | 100 | Yes | - | - | - | UTM medium parameter |
| UTM_CAMPAIGN | NVARCHAR | 200 | Yes | - | - | - | UTM campaign parameter |
| UTM_CONTENT | NVARCHAR | 200 | Yes | - | - | - | UTM content parameter |
| REFERRER_URL | NVARCHAR | 500 | Yes | - | - | - | HTTP referrer URL |
| LANDING_PAGE | NVARCHAR | 500 | Yes | - | - | - | First page visited |
| DEVICE_TYPE | NVARCHAR | 20 | Yes | - | - | - | Registration device |
| REGISTRATION_DATE | DATETIME2 | - | No | - | - | - | Registration timestamp |
| CREATED_DATE | DATETIME2 | - | No | GETDATE() | - | - | Record creation timestamp |

#### Valid Values

| Column | Valid Values | Description |
|--------|--------------|-------------|
| CHANNEL | Paid Social, Organic, Affiliate, Direct, Email, Search, Display | Acquisition channel |
| DEVICE_TYPE | Mobile, Desktop, Tablet, App-Android, App-iOS | Device used for registration |

#### CLV Usage

| Column | CLV Component | Usage |
|--------|---------------|-------|
| CHANNEL | Acquisition Channel Dimension | Primary dimension for channel analysis |
| CAMPAIGN_ID | CAC | Links to campaign for cost attribution |

---

### 3.3 ERP.OE_ORDER_HEADERS_ALL (ENT-003)

#### Entity Description
| Attribute | Value |
|-----------|-------|
| **Entity Name** | OE_ORDER_HEADERS_ALL |
| **Business Name** | Order Header |
| **Description** | Master record for each customer order containing header-level information including totals, status, and payment details |
| **Source System** | SRC-001 (Oracle Fusion ERP) |
| **Schema** | ERP |
| **Owner** | Finance / Operations |
| **Update Frequency** | Real-time (CDC) |
| **Retention Period** | 7 years |

#### Column Specifications

| Column Name | Data Type | Length | Nullable | Default | PK | FK | Business Definition |
|-------------|-----------|--------|----------|---------|----|----|---------------------|
| ORDER_ID | BIGINT | - | No | Identity | ✓ | - | Unique system order identifier |
| ORDER_NUMBER | NVARCHAR | 50 | No | - | UK | - | Customer-facing order reference |
| CUSTOMER_ID | BIGINT | - | No | - | - | ✓ → CUSTOMERS | Customer who placed order |
| ORDER_DATE | DATETIME2 | - | No | - | - | - | Order placement timestamp |
| ORDER_STATUS | NVARCHAR | 30 | No | 'Booked' | - | - | Current order lifecycle status |
| PAYMENT_METHOD | NVARCHAR | 30 | No | - | - | - | Payment instrument used |
| PAYMENT_STATUS | NVARCHAR | 20 | No | 'Pending' | - | - | Payment processing status |
| SUBTOTAL_AMOUNT | DECIMAL | 15,2 | No | - | - | - | Order subtotal before discounts |
| DISCOUNT_AMOUNT | DECIMAL | 15,2 | No | 0 | - | - | Total discount applied |
| TAX_AMOUNT | DECIMAL | 15,2 | No | 0 | - | - | Tax amount (GST) |
| SHIPPING_AMOUNT | DECIMAL | 15,2 | No | 0 | - | - | Shipping charges |
| TOTAL_AMOUNT | DECIMAL | 15,2 | No | - | - | - | Final order total |
| CURRENCY_CODE | NVARCHAR | 3 | No | 'INR' | - | - | Transaction currency |
| SHIPPING_ADDRESS_ID | BIGINT | - | No | - | - | ✓ → ADDRESSES | Delivery address |
| BILLING_ADDRESS_ID | BIGINT | - | Yes | - | - | ✓ → ADDRESSES | Billing address |
| PROMISED_DATE | DATE | - | Yes | - | - | - | Promised delivery date |
| SHIPPED_DATE | DATETIME2 | - | Yes | - | - | - | Actual ship timestamp |
| DELIVERED_DATE | DATETIME2 | - | Yes | - | - | - | Actual delivery timestamp |
| CANCELLATION_REASON | NVARCHAR | 200 | Yes | - | - | - | Reason if cancelled |
| ORDER_SOURCE | NVARCHAR | 20 | No | 'Web' | - | - | Order channel |
| ORG_ID | INT | - | No | 101 | - | - | Organization identifier |
| CREATED_BY | NVARCHAR | 50 | No | 'ECOM_API' | - | - | System/user that created |
| CREATED_DATE | DATETIME2 | - | No | GETDATE() | - | - | Record creation timestamp |
| LAST_UPDATE_DATE | DATETIME2 | - | No | GETDATE() | - | - | Last modification timestamp |

#### Valid Values

| Column | Valid Values | Description |
|--------|--------------|-------------|
| ORDER_STATUS | Draft, Booked, Processing, Shipped, Delivered, Cancelled, Returned, Refunded | Order lifecycle |
| PAYMENT_METHOD | UPI, Credit Card, Debit Card, Net Banking, COD, Wallet, EMI | Payment type |
| PAYMENT_STATUS | Pending, Authorized, Captured, Failed, Refunded | Payment state |
| ORDER_SOURCE | Web, Mobile-App, M-Site, Call-Center | Channel |

#### CLV Usage

| Column | CLV Component | Usage |
|--------|---------------|-------|
| TOTAL_AMOUNT | AOV, Monetary | Revenue calculation |
| ORDER_ID | Frequency | Order count |
| ORDER_DATE | Recency, Frequency | Date calculations |
| ORDER_STATUS | All | Filter out cancelled/returned |
| CUSTOMER_ID | All | Join key |

#### Business Rules

| Rule ID | Rule | Description |
|---------|------|-------------|
| BR-ORD-001 | TOTAL_AMOUNT = SUBTOTAL - DISCOUNT + TAX + SHIPPING | Amount calculation |
| BR-ORD-002 | Exclude ORDER_STATUS IN ('Cancelled', 'Returned') from revenue | Revenue filter |
| BR-ORD-003 | DELIVERED_DATE required when ORDER_STATUS = 'Delivered' | Status validation |

---

### 3.4 MARKETING.MARKETING_CAMPAIGNS (ENT-010)

#### Entity Description
| Attribute | Value |
|-----------|-------|
| **Entity Name** | MARKETING_CAMPAIGNS |
| **Business Name** | Marketing Campaign |
| **Description** | Marketing campaign master with spend and performance metrics for customer acquisition cost calculation |
| **Source System** | SRC-003 (Marketing Platform) |
| **Schema** | MARKETING |
| **Owner** | Marketing Team |
| **Update Frequency** | Daily batch |

#### Column Specifications

| Column Name | Data Type | Length | Nullable | Default | PK | FK | Business Definition |
|-------------|-----------|--------|----------|---------|----|----|---------------------|
| CAMPAIGN_ID | INT | - | No | Identity | ✓ | - | Unique campaign identifier |
| CAMPAIGN_NAME | NVARCHAR | 200 | No | - | - | - | Campaign display name |
| CAMPAIGN_CODE | NVARCHAR | 50 | No | - | UK | - | Unique campaign code |
| CHANNEL | NVARCHAR | 50 | No | - | - | - | Marketing channel |
| SUB_CHANNEL | NVARCHAR | 50 | Yes | - | - | - | Channel subdivision |
| START_DATE | DATE | - | No | - | - | - | Campaign start date |
| END_DATE | DATE | - | Yes | - | - | - | Campaign end date |
| TOTAL_SPEND | DECIMAL | 15,2 | No | 0 | - | - | Total campaign expenditure |
| CUSTOMERS_ACQUIRED | INT | - | No | 0 | - | - | Customers attributed |
| STATUS | NVARCHAR | 20 | No | 'Active' | - | - | Campaign status |
| CREATED_DATE | DATETIME2 | - | No | GETDATE() | - | - | Record creation timestamp |
| LAST_UPDATE_DATE | DATETIME2 | - | No | GETDATE() | - | - | Last modification timestamp |

#### CLV Usage

| Column | CLV Component | Usage |
|--------|---------------|-------|
| TOTAL_SPEND | CAC | Numerator in CAC calculation |
| CUSTOMERS_ACQUIRED | CAC | Denominator in CAC calculation |
| CHANNEL | Acquisition Channel | Channel-level CAC analysis |

#### Derived Metric

```sql
CAC = TOTAL_SPEND / NULLIF(CUSTOMERS_ACQUIRED, 0)
```

---

### 3.5 CRM.SURVEYS (ENT-013)

#### Entity Description
| Attribute | Value |
|-----------|-------|
| **Entity Name** | SURVEYS |
| **Business Name** | Customer Survey Response |
| **Description** | NPS and CSAT survey responses linked to customers and orders for satisfaction analysis |
| **Source System** | SRC-002 (Oracle Service Cloud CRM) |
| **Schema** | CRM |
| **Owner** | Customer Experience Team |
| **Update Frequency** | Daily batch |

#### Column Specifications

| Column Name | Data Type | Length | Nullable | Default | PK | FK | Business Definition |
|-------------|-----------|--------|----------|---------|----|----|---------------------|
| SURVEY_ID | BIGINT | - | No | Identity | ✓ | - | Unique survey response ID |
| CUSTOMER_ID | BIGINT | - | No | - | - | ✓ → CUSTOMERS | Responding customer |
| ORDER_ID | BIGINT | - | Yes | - | - | ✓ → ORDERS | Associated order |
| INCIDENT_ID | BIGINT | - | Yes | - | - | ✓ → INCIDENTS | Associated support ticket |
| SURVEY_TYPE | NVARCHAR | 20 | No | - | - | - | Type of survey |
| NPS_SCORE | TINYINT | - | Yes | - | - | - | Net Promoter Score (0-10) |
| CSAT_SCORE | TINYINT | - | Yes | - | - | - | Customer Satisfaction (1-5) |
| NPS_CATEGORY | NVARCHAR | 20 | Yes | - | - | - | NPS classification |
| FEEDBACK_TEXT | NVARCHAR | MAX | Yes | - | - | - | Open-ended feedback |
| FEEDBACK_CATEGORY | NVARCHAR | 50 | Yes | - | - | - | Feedback topic |
| RESPONSE_DATE | DATETIME2 | - | No | - | - | - | Survey completion timestamp |
| SURVEY_SENT_DATE | DATETIME2 | - | No | - | - | - | Survey sent timestamp |
| CREATED_DATE | DATETIME2 | - | No | GETDATE() | - | - | Record creation timestamp |

#### Valid Values

| Column | Valid Values | Description |
|--------|--------------|-------------|
| SURVEY_TYPE | NPS, CSAT, CES, Post-Delivery, Post-Support | Survey type |
| NPS_SCORE | 0-10 | Net Promoter Score |
| NPS_CATEGORY | Promoter (9-10), Passive (7-8), Detractor (0-6) | NPS classification |
| CSAT_SCORE | 1-5 | Satisfaction score |

#### CLV Usage

| Column | CLV Component | Usage |
|--------|---------------|-------|
| NPS_SCORE | Predicted Lifespan | Satisfaction impacts churn prediction |
| NPS_CATEGORY | Churn Risk | Detractors have higher churn probability |

---

## 4. Data Relationships

### 4.1 Entity Relationship Diagram (Textual)

```
                                    MARKETING.MARKETING_CAMPAIGNS
                                              │
                                              │ CAMPAIGN_ID
                                              ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              CRM SCHEMA (Customer Domain)                        │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│    CRM.CUSTOMERS (Golden Master)                                                │
│         │                                                                        │
│         ├──────────► CRM.CUSTOMER_REGISTRATION_SOURCE (1:1)                     │
│         │                                                                        │
│         ├──────────► CRM.INCIDENTS (1:N)                                        │
│         │                   │                                                    │
│         │                   └──────► CRM.INTERACTIONS (1:N)                     │
│         │                                                                        │
│         └──────────► CRM.SURVEYS (1:N)                                          │
│                                                                                  │
└───────────────────────────────────┬─────────────────────────────────────────────┘
                                    │
                                    │ CUSTOMER_ID
                                    ▼
┌─────────────────────────────────────────────────────────────────────────────────┐
│                              ERP SCHEMA (Transaction Domain)                     │
├─────────────────────────────────────────────────────────────────────────────────┤
│                                                                                  │
│    ERP.ADDRESSES ◄────────── ERP.OE_ORDER_HEADERS_ALL ──────────► CRM.CUSTOMERS │
│         │                          │                                             │
│         │                          │ ORDER_ID                                    │
│         ▼                          ▼                                             │
│    ERP.CITY_TIER_MASTER      ERP.OE_ORDER_LINES_ALL                             │
│                                    │                                             │
│                                    │ PRODUCT_ID                                  │
│                                    ▼                                             │
│                          ERP.MTL_SYSTEM_ITEMS_B                                 │
│                                    │                                             │
│                       ┌────────────┴────────────┐                                │
│                       ▼                         ▼                                │
│               ERP.CATEGORIES              ERP.BRANDS                             │
│                                                                                  │
└─────────────────────────────────────────────────────────────────────────────────┘
```

### 4.2 Relationship Cardinality

| Parent Entity | Child Entity | Relationship | Cardinality | Join Key |
|---------------|--------------|--------------|-------------|----------|
| CUSTOMERS | CUSTOMER_REGISTRATION_SOURCE | Has | 1:1 | CUSTOMER_ID |
| CUSTOMERS | ADDRESSES | Has | 1:N | CUSTOMER_ID |
| CUSTOMERS | OE_ORDER_HEADERS_ALL | Places | 1:N | CUSTOMER_ID |
| CUSTOMERS | INCIDENTS | Raises | 1:N | CUSTOMER_ID |
| CUSTOMERS | SURVEYS | Responds | 1:N | CUSTOMER_ID |
| OE_ORDER_HEADERS_ALL | OE_ORDER_LINES_ALL | Contains | 1:N | ORDER_ID |
| OE_ORDER_HEADERS_ALL | INCIDENTS | Related To | 1:N | ORDER_ID |
| OE_ORDER_HEADERS_ALL | SURVEYS | Triggers | 1:N | ORDER_ID |
| INCIDENTS | INTERACTIONS | Has | 1:N | INCIDENT_ID |
| MTL_SYSTEM_ITEMS_B | OE_ORDER_LINES_ALL | In | 1:N | PRODUCT_ID |
| CATEGORIES | MTL_SYSTEM_ITEMS_B | Contains | 1:N | CATEGORY_ID |
| BRANDS | MTL_SYSTEM_ITEMS_B | Makes | 1:N | BRAND_ID |
| MARKETING_CAMPAIGNS | CUSTOMER_REGISTRATION_SOURCE | Attributes | 1:N | CAMPAIGN_ID |
| CITY_TIER_MASTER | ADDRESSES | Classifies | 1:N | CITY, STATE |

---

## 5. Cross-System Integration Keys

### 5.1 Key Mapping

| Business Entity | Natural Key (Business) | Surrogate Key (Technical) | Master System | Usage |
|-----------------|------------------------|---------------------------|---------------|-------|
| Customer | EMAIL, PHONE | CUSTOMER_ID | CRM | All systems |
| Order | ORDER_NUMBER | ORDER_ID | ERP | ERP, CRM, Analytics |
| Product | SKU | INVENTORY_ITEM_ID | ERP | ERP, Analytics |
| Campaign | CAMPAIGN_CODE | CAMPAIGN_ID | Marketing | Marketing, CRM |
| City | CITY + STATE | Composite PK | ERP | ERP, Analytics |

### 5.2 Key Usage in CLV Queries

```sql
-- Customer joins
CRM.CUSTOMERS.CUSTOMER_ID = ERP.OE_ORDER_HEADERS_ALL.CUSTOMER_ID
CRM.CUSTOMERS.CUSTOMER_ID = CRM.CUSTOMER_REGISTRATION_SOURCE.CUSTOMER_ID
CRM.CUSTOMERS.CUSTOMER_ID = CRM.SURVEYS.CUSTOMER_ID

-- Order context joins
ERP.OE_ORDER_HEADERS_ALL.ORDER_ID = CRM.INCIDENTS.ORDER_ID
ERP.OE_ORDER_HEADERS_ALL.ORDER_ID = CRM.SURVEYS.ORDER_ID

-- Geographic joins
ERP.ADDRESSES.CITY = ERP.CITY_TIER_MASTER.CITY 
AND ERP.ADDRESSES.STATE = ERP.CITY_TIER_MASTER.STATE

-- Acquisition cost joins
CRM.CUSTOMER_REGISTRATION_SOURCE.CAMPAIGN_ID = MARKETING.MARKETING_CAMPAIGNS.CAMPAIGN_ID
```

---

## 6. Data Quality Rules

### 6.1 Completeness Rules

| Rule ID | Entity | Column | Rule | Threshold |
|---------|--------|--------|------|-----------|
| DQ-C-001 | CUSTOMERS | EMAIL | Not Null | 100% |
| DQ-C-002 | CUSTOMERS | REGISTRATION_DATE | Not Null | 100% |
| DQ-C-003 | OE_ORDER_HEADERS_ALL | CUSTOMER_ID | Not Null | 100% |
| DQ-C-004 | OE_ORDER_HEADERS_ALL | TOTAL_AMOUNT | Not Null | 100% |
| DQ-C-005 | CUSTOMER_REGISTRATION_SOURCE | CHANNEL | Not Null | 100% |

### 6.2 Validity Rules

| Rule ID | Entity | Column | Rule | Valid Range |
|---------|--------|--------|------|-------------|
| DQ-V-001 | OE_ORDER_HEADERS_ALL | TOTAL_AMOUNT | Range Check | > 0 |
| DQ-V-002 | SURVEYS | NPS_SCORE | Range Check | 0-10 |
| DQ-V-003 | SURVEYS | CSAT_SCORE | Range Check | 1-5 |
| DQ-V-004 | CUSTOMERS | EMAIL | Format Check | Valid email regex |
| DQ-V-005 | OE_ORDER_HEADERS_ALL | ORDER_DATE | Date Check | ≤ CURRENT_DATE |

### 6.3 Referential Integrity Rules

| Rule ID | Child Entity | Parent Entity | FK Column | Action on Violation |
|---------|--------------|---------------|-----------|---------------------|
| DQ-R-001 | OE_ORDER_HEADERS_ALL | CUSTOMERS | CUSTOMER_ID | Reject |
| DQ-R-002 | OE_ORDER_LINES_ALL | OE_ORDER_HEADERS_ALL | ORDER_ID | Reject |
| DQ-R-003 | INCIDENTS | CUSTOMERS | CUSTOMER_ID | Reject |
| DQ-R-004 | CUSTOMER_REGISTRATION_SOURCE | CUSTOMERS | CUSTOMER_ID | Reject |

---

## 7. Data Lineage for CLV

### 7.1 CLV Metric Lineage

```
┌─────────────────────────────────────────────────────────────────────────────────┐
│                           CLV CALCULATION LINEAGE                                │
└─────────────────────────────────────────────────────────────────────────────────┘

SOURCE LAYER                    TRANSFORMATION                      METRIC
─────────────────              ──────────────────                   ──────

ERP.OE_ORDER_HEADERS_ALL       ┌─────────────────┐
  └─ TOTAL_AMOUNT ────────────►│ SUM / COUNT     │────────────────► AOV
  └─ ORDER_STATUS              │ (Delivered only)│
                               └─────────────────┘

ERP.OE_ORDER_HEADERS_ALL       ┌─────────────────┐
  └─ ORDER_ID ────────────────►│ COUNT / MONTHS  │────────────────► Purchase Frequency
  └─ ORDER_DATE                │                 │
  └─ CUSTOMER_ID               └─────────────────┘

CRM.CUSTOMERS                  ┌─────────────────┐
  └─ REGISTRATION_DATE ───────►│ Date Diff +     │────────────────► Predicted Lifespan
  └─ STATUS                    │ Churn Model     │
ERP.OE_ORDER_HEADERS_ALL       │                 │
  └─ MAX(ORDER_DATE) ─────────►└─────────────────┘

MARKETING.MARKETING_CAMPAIGNS  ┌─────────────────┐
  └─ TOTAL_SPEND ─────────────►│ SPEND / COUNT   │────────────────► CAC
  └─ CUSTOMERS_ACQUIRED ──────►└─────────────────┘

                               ┌─────────────────────────────────────────────────┐
AOV ──────────────────────────►│                                                 │
Purchase Frequency ───────────►│  CLV = (AOV × Freq × Lifespan) - CAC           │────► CLV
Predicted Lifespan ───────────►│                                                 │
CAC ──────────────────────────►└─────────────────────────────────────────────────┘
```

### 7.2 Dimension Derivation Lineage

| Derived Dimension | Source Tables | Derivation Logic |
|-------------------|---------------|------------------|
| RFM Segment | OE_ORDER_HEADERS_ALL | Threshold-based on Recency, Frequency, Monetary |
| Loyalty Tier | OE_ORDER_HEADERS_ALL | Cumulative spend thresholds |
| City Tier | ADDRESSES + CITY_TIER_MASTER | Lookup join on CITY, STATE |
| Acquisition Channel | CUSTOMER_REGISTRATION_SOURCE | Direct from CHANNEL column |
| NPS Category | SURVEYS | 0-6=Detractor, 7-8=Passive, 9-10=Promoter |

---

## 8. Glossary

| Term | Definition |
|------|------------|
| CLV | Customer Lifetime Value - total predicted revenue from a customer |
| AOV | Average Order Value - mean order amount |
| CAC | Customer Acquisition Cost - cost to acquire one customer |
| RFM | Recency, Frequency, Monetary - segmentation model |
| NPS | Net Promoter Score - loyalty metric (0-10) |
| CSAT | Customer Satisfaction Score - satisfaction metric (1-5) |
| CDC | Change Data Capture - real-time data extraction |
| SCD | Slowly Changing Dimension - history tracking technique |
| Golden Record | Single source of truth for an entity |
| Natural Key | Business-meaningful identifier (e.g., EMAIL) |
| Surrogate Key | System-generated identifier (e.g., CUSTOMER_ID) |

---

## 9. Sign-Off

| Role | Name | Date | Signature |
|------|------|------|-----------|
| Data Steward | | | |
| Data Architect | | | |
| Business Analyst | | | |
| Technical Lead | | | |

---

## Appendix: Change Log

| Version | Date | Author | Changes |
|---------|------|--------|---------|
| 1.0 | Nov 2025 | | Initial draft |
| | | | |
