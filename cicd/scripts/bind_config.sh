#!/usr/bin/env bash
# Bind the generated code to the ACTUAL provisioned resources.
#
# The committed code is environment-agnostic: it references resources via canonical
# placeholders ({{PLACEHOLDER_*}}). This script runs AFTER `terraform apply` and BEFORE
# the code deploy steps, replacing those placeholders with the real Terraform outputs in
# the CI runner's checkout — so ADF linked services, notebooks and SQL all point at the
# resources Terraform just created (random suffixes and all).
#
# SECRET placeholders ({{PLACEHOLDER_*_KEY}}, {{PLACEHOLDER_*_PASSWORD}}, *_USERNAME) are
# intentionally NOT substituted here — secrets come from Key Vault via a Databricks secret
# scope / Key Vault references, never written into code.
#
# Requires: terraform (in ./infrastructure), jq. Run from repo root.
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
cd "$ROOT"

echo "=== Reading Terraform outputs ==="
TF=$(terraform -chdir=infrastructure output -json)
get() { echo "$TF" | jq -r ".${1}.value // empty"; }

STORAGE=$(get storage_account_name)
DBX=$(get databricks_workspace_url)
KV_URI=$(get key_vault_uri)
KV_NAME=$(echo "$KV_URI" | sed -E 's#https?://([^.]+)\..*#\1#')
SQL_FQDN=$(get sql_server_fqdn)
SQL_DB=$(get sql_database_name)
ADF=$(get data_factory_name)
RG=$(get resource_group_name)

# canonical placeholder -> value (resource identifiers only; NO secrets)
declare -A MAP=(
  ["stdataplatformdevnda0jg"]="$STORAGE"
  ["adb-7405616217745241.1.azuredatabricks.net"]="$DBX"
  ["kvdataplatformdevnda0jg"]="$KV_NAME"
  ["https://kvdataplatformdevnda0jg.vault.azure.net/"]="$KV_URI"
  ["sql-dataplatform-dev-nda0jg.database.windows.net"]="$SQL_FQDN"
  ["sqldb-control-dataplatform-dev"]="$SQL_DB"
  ["adf-dataplatform-dev-nda0jg"]="$ADF"
  ["rg-dataplatform-dev-centralindia"]="$RG"
)

echo "=== Binding placeholders across src/ and cicd/ ==="
missing=0
for key in "${!MAP[@]}"; do
  val="${MAP[$key]}"
  if [ -z "$val" ]; then echo "  ! no Terraform output for $key (left as placeholder)"; missing=1; continue; fi
  echo "  $key -> $val"
  # only touch files that actually contain the token
  while IFS= read -r f; do
    sed -i "s|${key}|${val}|g" "$f"
  done < <(grep -rlF "$key" src cicd 2>/dev/null || true)
done

# Surface any remaining NON-secret placeholders (a coverage gap), but ignore secret ones.
echo "=== Remaining placeholders (secrets are expected; resource refs are NOT) ==="
grep -rhoE "\{\{PLACEHOLDER_[A-Z_]+\}\}" src cicd 2>/dev/null | sort -u | while read -r p; do
  case "$p" in
    *KEY*|*PASSWORD*|*SECRET*|*USERNAME*) echo "  ok (secret, via Key Vault): $p" ;;
    *) echo "  !! UNBOUND resource placeholder: $p — add a Terraform output for it" ;;
  esac
done
[ "$missing" = "0" ] && echo "=== Bind complete ===" || { echo "=== Bind finished with missing outputs (see above) ==="; }
