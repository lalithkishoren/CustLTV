# Myntra CLV Data Platform - CI/CD & Environments

This repository contains the Infrastructure as Code (Terraform), Orchestration (Azure Data Factory), and Data Processing (Databricks Asset Bundles / DLT) for the Myntra Customer Lifetime Value (CLV) Data Platform.

## Architecture Principles Enforced
1. **Reliability**: Deployments are idempotent. Infrastructure is managed via Terraform state; Databricks assets are managed via Databricks Asset Bundles (DABs).
2. **Security**: Zero inline secrets. All credentials are injected at runtime via GitHub Actions Secrets and Azure Key Vault.
3. **Quality**: Code is strictly linted (Ruff for Python, SQLFluff for SQL) and unit-tested (Pytest) before any deployment occurs.

## Promotion Flow

The CI/CD pipeline (`.github/workflows/data-platform-cicd.yml`) implements a progressive promotion model:

1. **Pull Request (Feature Branch -> `main`)**:
   - Triggers `lint-and-test` job.
   - Validates Terraform syntax and formatting.
   - Runs Python/SQL linters and Pytest unit tests.
   - *No deployment occurs.*

2. **Merge to `main` (Continuous Integration)**:
   - Triggers `lint-and-test`.
   - On success, triggers `deploy-dev` (deploys to the Development environment).
   - On success of Dev, triggers `deploy-test` (deploys to the Test/UAT environment).

3. **Release / Tag (Production Deployment)**:
   - Creating a GitHub Release (e.g., `v1.0.0`) triggers the `deploy-prod` job.
   - Requires manual approval via GitHub Environments (`prod`).
   - Deploys strictly using `prod.tfvars` and the `prod` DAB target.

## Configuration as Code

Environment-specific configurations are stored in `config/env/`. 
- `dev.tfvars`: Uses LRS storage, minimal cluster sizes, and `dev_catalog` in Unity Catalog.
- `prod.tfvars`: Uses ZRS storage for high availability, auto-scaling clusters, and `prod_catalog`.

Databricks pipelines (DLT) and Jobs are parameterized via `databricks.yml` targets, ensuring the code remains identical across environments while pointing to the correct Unity Catalog namespaces.

## Secret Management & Prerequisites

To run this pipeline, configure the following in your GitHub Repository Settings -> Secrets and Variables:

### GitHub Secrets (Repository Level)
- `AZURE_CLIENT_ID`: Service Principal Client ID (Federated OIDC recommended).
- `AZURE_TENANT_ID`: Azure Entra ID Tenant ID.
- `AZURE_SUBSCRIPTION_ID`: Azure Subscription ID.
- `DATABRICKS_SP_TOKEN`: Service Principal PAT or Entra ID token for Databricks deployment.

### GitHub Variables (Environment Level - dev/test/prod)
- `TF_STATE_RG`: Resource Group hosting the Terraform state storage account.
- `TF_STATE_SA`: Storage Account name for Terraform state.
- `DATABRICKS_HOST`: The URL of the Databricks workspace for that specific environment.

*Note: Application secrets (e.g., Oracle DB credentials, Marketing API keys) are NOT stored in GitHub. They are provisioned in Azure Key Vault via Terraform and referenced dynamically by Azure Data Factory and Databricks Secret Scopes.*