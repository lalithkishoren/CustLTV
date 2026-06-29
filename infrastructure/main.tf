data "azurerm_client_config" "current" {}

resource "random_string" "suffix" {
  length  = 6
  special = false
  upper   = false
}

locals {
  base_name   = "${var.project}-${var.environment}"
  safe_name   = replace(local.base_name, "-", "")
  name_suffix = random_string.suffix.result
}

# ---------------------------------------------------------------------------
# Resource Group
# ---------------------------------------------------------------------------
resource "azurerm_resource_group" "rg" {
  name     = "rg-${local.base_name}-${var.location}"
  location = var.location
  tags     = var.tags
}

# ---------------------------------------------------------------------------
# ADLS Gen2 Storage (Medallion Architecture)
# ---------------------------------------------------------------------------
resource "azurerm_storage_account" "adls" {
  name                     = "st${local.safe_name}${local.name_suffix}"
  resource_group_name      = azurerm_resource_group.rg.name
  location                 = azurerm_resource_group.rg.location
  account_tier             = "Standard"
  account_replication_type = var.storage_replication
  is_hns_enabled           = true
  min_tls_version          = "TLS1_2"

  tags = var.tags
}

resource "azurerm_storage_data_lake_gen2_filesystem" "medallion" {
  for_each           = toset(["bronze", "silver", "gold"])
  name               = each.key
  storage_account_id = azurerm_storage_account.adls.id
}

# ---------------------------------------------------------------------------
# Azure Key Vault
# ---------------------------------------------------------------------------
resource "azurerm_key_vault" "kv" {
  name                       = substr("kv${local.safe_name}${local.name_suffix}", 0, 24)
  location                   = azurerm_resource_group.rg.location
  resource_group_name        = azurerm_resource_group.rg.name
  tenant_id                  = data.azurerm_client_config.current.tenant_id
  sku_name                   = "standard"
  enable_rbac_authorization  = true
  purge_protection_enabled   = false # Set to true in production

  tags = var.tags
}

# Grant Terraform executing identity Key Vault Administrator to create secrets
resource "azurerm_role_assignment" "tf_kv_admin" {
  scope                = azurerm_key_vault.kv.id
  role_definition_name = "Key Vault Administrator"
  principal_id         = data.azurerm_client_config.current.object_id
}

# Wait for RBAC propagation before creating secrets
resource "time_sleep" "wait_for_kv_rbac" {
  depends_on      = [azurerm_role_assignment.tf_kv_admin]
  create_duration = "30s"
}

# ---------------------------------------------------------------------------
# Azure SQL Database (Control DB for ADF / Metadata)
# ---------------------------------------------------------------------------
resource "random_password" "sql_admin" {
  length           = 16
  special          = true
  override_special = "!#$%&*()-_=+[]{}<>:?"
}

resource "azurerm_key_vault_secret" "sql_admin_pwd" {
  name         = "sql-admin-password"
  value        = random_password.sql_admin.result
  key_vault_id = azurerm_key_vault.kv.id
  depends_on   = [time_sleep.wait_for_kv_rbac]
  tags         = var.tags
}

resource "azurerm_mssql_server" "sql_server" {
  name                         = "sql-${local.base_name}-${local.name_suffix}"
  resource_group_name          = azurerm_resource_group.rg.name
  location                     = azurerm_resource_group.rg.location
  version                      = "12.0"
  administrator_login          = var.sql_admin_username
  administrator_login_password = random_password.sql_admin.result
  minimum_tls_version          = "1.2"

  tags = var.tags
}

resource "azurerm_mssql_database" "sql_db" {
  name        = "sqldb-control-${local.base_name}"
  server_id   = azurerm_mssql_server.sql_server.id
  sku_name    = var.sql_sku
  tags        = var.tags
}

# ---------------------------------------------------------------------------
# Azure Data Factory
# ---------------------------------------------------------------------------
resource "azurerm_data_factory" "adf" {
  name                = "adf-${local.base_name}-${local.name_suffix}"
  location            = azurerm_resource_group.rg.location
  resource_group_name = azurerm_resource_group.rg.name

  identity {
    type = "SystemAssigned"
  }

  tags = var.tags
}

# ADF RBAC: Read/Write to ADLS Gen2
resource "azurerm_role_assignment" "adf_storage_contributor" {
  scope                = azurerm_storage_account.adls.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_data_factory.adf.identity[0].principal_id
}

# ADF RBAC: Read Secrets from Key Vault
resource "azurerm_role_assignment" "adf_kv_secrets_user" {
  scope                = azurerm_key_vault.kv.id
  role_definition_name = "Key Vault Secrets User"
  principal_id         = azurerm_data_factory.adf.identity[0].principal_id
}

# ---------------------------------------------------------------------------
# Azure Databricks
# ---------------------------------------------------------------------------
resource "azurerm_databricks_workspace" "dbw" {
  name                        = "dbw-${local.base_name}-${local.name_suffix}"
  resource_group_name         = azurerm_resource_group.rg.name
  location                    = azurerm_resource_group.rg.location
  sku                         = var.databricks_sku
  managed_resource_group_name = "rg-${local.base_name}-dbw-managed-${local.name_suffix}"

  tags = var.tags
}

# Access Connector for Unity Catalog integration
resource "azurerm_databricks_access_connector" "ext_storage" {
  name                = "dbac-${local.base_name}-${local.name_suffix}"
  resource_group_name = azurerm_resource_group.rg.name
  location            = azurerm_resource_group.rg.location

  identity {
    type = "SystemAssigned"
  }

  tags = var.tags
}

# Databricks Access Connector RBAC: Read/Write to ADLS Gen2
resource "azurerm_role_assignment" "dbw_storage_contributor" {
  scope                = azurerm_storage_account.adls.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_databricks_access_connector.ext_storage.identity[0].principal_id
}