variable "project" {
  type        = string
  description = "The project name, used as a prefix for resources."
}

variable "environment" {
  type        = string
  description = "The environment name (e.g., dev, test, prod)."
}

variable "location" {
  type        = string
  description = "The Azure region to deploy resources into."
}

variable "tags" {
  type        = map(string)
  description = "A map of tags to apply to all resources."
  default     = {}
}

variable "storage_replication" {
  type        = string
  description = "The replication type for the storage account (e.g., LRS, GRS)."
  default     = "LRS"
}

variable "sql_admin_username" {
  type        = string
  description = "The administrator username for the Azure SQL Server."
  default     = "sqladmin"
}

variable "sql_sku" {
  type        = string
  description = "The SKU for the Azure SQL Database."
  default     = "S0"
}

variable "databricks_sku" {
  type        = string
  description = "The SKU for the Azure Databricks workspace (standard, premium, trial)."
  default     = "premium"
}