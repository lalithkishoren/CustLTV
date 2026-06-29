output "resource_group_name" {
  description = "The name of the resource group."
  value       = azurerm_resource_group.rg.name
}

output "storage_account_name" {
  description = "The name of the ADLS Gen2 storage account."
  value       = azurerm_storage_account.adls.name
}

output "key_vault_uri" {
  description = "The URI of the Key Vault."
  value       = azurerm_key_vault.kv.vault_uri
}

output "sql_server_fqdn" {
  description = "The fully qualified domain name of the Azure SQL Server."
  value       = azurerm_mssql_server.sql_server.fully_qualified_domain_name
}

output "sql_database_name" {
  description = "The name of the Control SQL Database."
  value       = azurerm_mssql_database.sql_db.name
}

output "data_factory_name" {
  description = "The name of the Azure Data Factory."
  value       = azurerm_data_factory.adf.name
}

output "data_factory_identity_principal_id" {
  description = "The Principal ID of the ADF System Assigned Managed Identity."
  value       = azurerm_data_factory.adf.identity[0].principal_id
}

output "databricks_workspace_url" {
  description = "The URL of the Databricks Workspace."
  value       = azurerm_databricks_workspace.dbw.workspace_url
}

output "databricks_access_connector_id" {
  description = "The ID of the Databricks Access Connector for Unity Catalog."
  value       = azurerm_databricks_access_connector.ext_storage.id
}