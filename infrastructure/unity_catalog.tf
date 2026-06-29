# =============================================================================
# Unity Catalog wiring — KEYLESS ADLS access via the Databricks Access Connector.
#
# This is what lets the regenerated notebooks read/write abfss://<layer>@<storage>/...
# WITHOUT any keys or secrets: UC authorizes the paths through a Storage Credential
# bound to the Access Connector (a managed identity that already has Storage Blob Data
# Contributor on the storage account, granted by main.tf).
#
# PREREQUISITE (one-time, account-level): a Unity Catalog METASTORE must be assigned to
# this workspace. Most Azure Databricks accounts have a regional metastore auto-created;
# if `terraform apply` errors with "metastore not assigned", assign one in the Databricks
# account console (Account → Data → assign metastore to the workspace) and re-apply.
# =============================================================================

# Storage credential backed by the Access Connector (managed identity — no secret).
resource "databricks_storage_credential" "adls" {
  name = "${local.base_name}-adls-cred"
  azure_managed_identity {
    access_connector_id = azurerm_databricks_access_connector.ext_storage.id
  }
  comment    = "Keyless ADLS access via the Databricks Access Connector"
  depends_on = [azurerm_databricks_workspace.dbw]
}

# One external location per medallion layer (bronze/silver/gold), matching the filesystems
# created in main.tf. These are exactly the paths the generated notebooks use.
resource "databricks_external_location" "medallion" {
  for_each        = toset(["bronze", "silver", "gold"])
  name            = "${local.base_name}-${each.key}"
  url             = "abfss://${each.key}@${azurerm_storage_account.adls.name}.dfs.core.windows.net/"
  credential_name = databricks_storage_credential.adls.name
  comment         = "External location for the ${each.key} medallion layer"
  depends_on      = [azurerm_storage_data_lake_gen2_filesystem.medallion]
}

output "uc_storage_credential" {
  value       = databricks_storage_credential.adls.name
  description = "UC storage credential bound to the Access Connector"
}

output "uc_external_locations" {
  value       = { for k, v in databricks_external_location.medallion : k => v.url }
  description = "UC external locations per medallion layer"
}
