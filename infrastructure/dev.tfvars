project             = "dataplatform"
environment         = "dev"
location            = "centralindia"
storage_replication = "LRS"
sql_sku             = "S0"
databricks_sku      = "premium"

tags = {
  Environment = "dev"
  Project     = "dataplatform"
  ManagedBy   = "terraform"
}