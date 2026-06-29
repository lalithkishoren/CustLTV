# Production Environment Configuration
environment         = "prod"
location            = "eastus2"
resource_group_name = "rg-myntra-data-prod-001"

# Storage (ADLS Gen2) - ZRS enabled for Prod Reliability
storage_account_name = "stmyntradataprod001"
bronze_container     = "bronze"
silver_container     = "silver"
gold_container       = "gold"

# Azure Data Factory
data_factory_name = "adf-myntra-data-prod-001"

# Azure Key Vault
key_vault_name = "kv-myntra-data-prod-001"

# Databricks & Unity Catalog
databricks_workspace_name = "dbw-myntra-data-prod-001"
databricks_sku            = "premium"
unity_catalog_metastore_id = "uc-metastore-prod-id"

# Compute / Cluster Policies - Scaled for Prod Performance
dlt_cluster_policy_id = "policy-prod-dlt-001"
min_workers           = 4
max_workers           = 16

# Tags
tags = {
  Environment = "Prod"
  Project     = "CLV-Analytics"
  Owner       = "DataPlatformTeam"
  CostCenter  = "CC-1001"
}