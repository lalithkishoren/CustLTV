project             = "dataplatform"
environment         = "prod"
location            = "centralindia"
storage_replication = "GRS"
sql_sku             = "P1"
databricks_sku      = "premium"

tags = {
  Environment = "prod"
  Project     = "dataplatform"
  ManagedBy   = "terraform"
}