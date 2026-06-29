# Environment: Development
environment         = "dev"
location            = "westeurope"
resource_group_name = "rg-myntra-clv-dev-weu"

# Storage (ADLS Gen2)
data_lake_name      = "dlsmyntraclvdevweu"
storage_replication = "LRS" # Cost optimization for Dev

# Azure Data Factory
adf_name            = "adf-myntra-clv-dev-weu"

# Databricks
databricks_workspace_name = "dbw-myntra-clv-dev-weu"
databricks_sku            = "premium"

# Key Vault
key_vault_name      = "kv-myntra-clv-dev-weu"

# Unity Catalog
uc_catalog_name     = "dev_catalog"
uc_metastore_id     = "primary-metastore-weu"

# Compute / Cluster Policies
cluster_autotermination_minutes = 15
min_workers                     = 1
max_workers                     = 2