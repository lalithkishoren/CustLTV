# Azure Modern Data Platform Infrastructure

This repository contains the Infrastructure-as-Code (Terraform) to provision a production-grade Medallion Lakehouse on Azure.

## Architecture Components
- **Resource Group**: Logical container for all resources.
- **ADLS Gen2**: Immutable object storage acting as the system of record. Contains `bronze`, `silver`, and `gold` containers.
- **Azure Key Vault**: Centralized secret management.
- **Azure SQL Database**: Control database for metadata, orchestration state, and CDC watermarks.
- **Azure Data Factory**: Orchestration engine. Uses Managed Identity for least-privilege access to Storage and Key Vault.
- **Azure Databricks**: Compute engine for data processing. Configured with an Access Connector for Unity Catalog integration.

## Prerequisites
- [Azure CLI](https://docs.microsoft.com/en-us/cli/azure/install-azure-cli) installed and authenticated (`az login`).
- [Terraform](https://www.terraform.io/downloads.html) >= 1.3.0 installed.
- Sufficient permissions in your Azure Subscription to create Resource Groups, Role Assignments (Owner or User Access Administrator), and Key Vaults.

## Deployment Instructions

### 1. Bootstrap Remote State (One-time setup)
Terraform state must be stored remotely for team collaboration and safety.
Run the bootstrap script to create the Storage Account for the state file: