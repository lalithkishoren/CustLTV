# Development Environment Configuration
environment         = "dev"
location            = "eastus2"
resource_group_name = "rg-myntra-data-dev-001"

# Storage (ADLS Gen2)
storage_account_name = "stmyntradatadev001"
bronze_container     = "bronze"
silver_container     = "silver"
gold_container       = "gold"

# Azure Data Factory
data_factory_name = "adf-myntra-data-dev-001"

# Azure Key Vault
key_vault_name = "kv-myntra-data-dev-001"

# Databricks & Unity Catalog
databricks_workspace_name = "dbw-myntra-data-dev-001"
databricks_sku            = "premium"
unity_catalog_metastore_id = "uc-metastore-dev-id"

# Compute / Cluster Policies
dlt_cluster_policy_id = "policy-dev-dlt-001"
min_workers           = 1
max_workers           = 4

# Tags
tags = {
  Environment = "Dev"
  Project     = "CLV-Analytics"
  Owner       = "DataPlatformTeam"
  CostCenter  = "CC-1001"
}