terraform {
  required_version = ">= 1.3.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.90"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.6"
    }
    time = {
      source  = "hashicorp/time"
      version = "~> 0.10"
    }
    databricks = {
      source  = "databricks/databricks"
      version = "~> 1.40"
    }
  }

}

provider "azurerm" {
  features {
    key_vault {
      purge_soft_delete_on_destroy    = true
      recover_soft_deleted_key_vaults = true
    }
    resource_group {
      prevent_deletion_if_contains_resources = false
    }
  }
}

# Databricks provider, authenticated to the workspace (uses your az login / ARM credentials).
provider "databricks" {
  host                        = "https://${azurerm_databricks_workspace.dbw.workspace_url}"
  azure_workspace_resource_id = azurerm_databricks_workspace.dbw.id
}