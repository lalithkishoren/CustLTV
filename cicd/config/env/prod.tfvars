# Environment: Production
environment         = "prod"
location            = "westeurope"
resource_group_name = "rg-myntra-clv-prod-weu"

# Storage (ADLS Gen2)
data_lake_name      = "dlsmyntraclvprodweu"
storage_replication = "ZRS" # Reliability/HA for Prod

# Azure Data Factory
adf_name            = "adf-myntra-clv-prod-weu"

# Databricks
databricks_workspace_name = "dbw-myntra-clv-prod-weu"
databricks_sku            = "premium"

# Key Vault
key_vault_name      = "kv-myntra-clv-prod-weu"

# Unity Catalog
uc_catalog_name     = "prod_catalog"
uc_metastore_id     = "primary-metastore-weu"

# Compute / Cluster Policies
cluster_autotermination_minutes = 10
min_workers                     = 2
max_workers                     = 8