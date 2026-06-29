# CI/CD Deployment Guide

The GitHub Actions workflow (`.github/workflows/data-platform-cicd.yml`) deploys the whole
platform on push to `main`:

1. **lint-and-test** — Ruff / SQLFluff / pytest (non-blocking for now; tighten when green).
2. **deploy-dev** — `terraform apply` (infra) → **Databricks** notebooks (asset bundle) →
   **ADF** artifacts (44) → **SQL** control objects (21). Targets are read from `terraform output`.
3. **deploy-test / deploy-prod** — mirror dev (prod runs on a published Release).

## One-time setup (required before the first run)

### 1. Azure AD app for GitHub OIDC (no stored cloud password)
```bash
az ad app create --display-name "gh-custltv-deploy"
# note the appId (-> AZURE_CLIENT_ID) and create a service principal:
az ad sp create --id <appId>
```
Add a **federated credential** per environment so GitHub can log in without a secret:
```bash
az ad app federated-credential create --id <appId> --parameters '{
  "name": "gh-dev",
  "issuer": "https://token.actions.githubusercontent.com",
  "subject": "repo:<OWNER>/<REPO>:environment:dev",
  "audiences": ["api://AzureADTokenExchange"]
}'
```
(repeat with `:environment:test` and `:environment:prod`).

### 2. Grant the service principal roles
```bash
SUB=$(az account show --query id -o tsv)
az role assignment create --assignee <appId> --role "Contributor"                    --scope /subscriptions/$SUB
az role assignment create --assignee <appId> --role "Storage Blob Data Contributor"  --scope /subscriptions/$SUB
az role assignment create --assignee <appId> --role "Key Vault Secrets User"         --scope /subscriptions/$SUB
```
(Tighten scopes to the resource groups in production.)

### 3. Remote Terraform state (CI uses remote state, not local)
In the app: **Deployment tab → Advanced → Remote → Bootstrap remote state** (creates the
state Storage Account + container). Make sure `backend.tf` has the `backend "azurerm" {}`
block **uncommented** when you commit (Advanced → Remote does this).

### 4. Databricks service principal token
Create an SP token with workspace access → set it as `DATABRICKS_SP_TOKEN`.

## GitHub configuration

**Repo → Settings → Secrets and variables → Actions**

Secrets:
| Secret | Value |
|---|---|
| `AZURE_CLIENT_ID` | the app registration's appId |
| `AZURE_TENANT_ID` | `az account show --query tenantId -o tsv` |
| `AZURE_SUBSCRIPTION_ID` | `az account show --query id -o tsv` |
| `DATABRICKS_SP_TOKEN` | Databricks SP/PAT token |
| `SQL_ADMIN_USER` | SQL admin login (matches `sql_admin_login` in tfvars) |

Variables:
| Variable | Value |
|---|---|
| `TF_STATE_RG` | remote-state resource group (from Bootstrap / backend.hcl) |
| `TF_STATE_SA` | remote-state storage account (from Bootstrap / backend.hcl) |
| `KEY_VAULT_NAME` | the Key Vault name (holds the SQL admin password) |
| `SQL_PWD_SECRET_NAME` | the KV secret name for the SQL admin password |

**Environments** (Settings → Environments): create `dev`, `test`, `prod`. Add a
**required reviewer** on `prod` so production deploys need manual approval.

## Run it
1. Push the project to the GitHub repo (use the app's GitHub connect/push, or `git push`).
2. The workflow runs automatically on `main`. Watch it under the repo's **Actions** tab.
3. **Prod** deploys only when you publish a GitHub **Release** (and a reviewer approves).

## Validate on first run (likely tweak points)
This is a standard pipeline but should be confirmed by a first run. Common adjustments:
- **Databricks host** — the workflow uses `https://<terraform databricks_workspace_url>`. If
  your output already includes the scheme, drop the `https://` prefix in the deploy step.
- **ADF** — `deploy_adf.sh` uses `az datafactory`; if an artifact needs an Integration Runtime
  or references a managed identity, create those first (or extend the script).
- **SQL** — `deploy_sql.sh` opens a temporary firewall rule for the runner IP and removes it
  after; ensure the SP has `Key Vault Secrets User` so it can read the admin password.
