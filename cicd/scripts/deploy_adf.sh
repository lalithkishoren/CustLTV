#!/usr/bin/env bash
# Deploy Azure Data Factory artifacts (linked services -> datasets -> pipelines) to the
# provisioned factory. Order matters: linked services first, then datasets, then pipelines.
# Requires: az CLI (logged in), jq, and the az datafactory extension (auto-installed).
# Env: ADF_NAME, ADF_RG  (from `terraform output`).
set -euo pipefail
: "${ADF_NAME:?ADF_NAME required}"
: "${ADF_RG:?ADF_RG required}"

az extension add --name datafactory --only-show-errors --yes 2>/dev/null || true

deploy() {
  local kind="$1"        # folder name under adf/ (linkedService|dataset|pipeline)
  local subcmd="$2"      # az datafactory subcommand
  local name_flag="$3"   # name flag for that subcommand
  local props_flag="$4"  # properties flag for that subcommand
  # collect from all layers: src/<layer>/adf/<kind>/*.json
  while IFS= read -r f; do
    [ -f "$f" ] || continue
    local name props
    name=$(jq -r '.name' "$f")
    props=$(jq -c '.properties' "$f")
    echo ">> ${kind}: ${name}"
    az datafactory ${subcmd} create \
      --resource-group "$ADF_RG" --factory-name "$ADF_NAME" \
      ${name_flag} "$name" ${props_flag} "$props" --only-show-errors
  done < <(find src -path "*adf/${kind}/*.json" | sort)
}

echo "=== Deploying ADF artifacts to ${ADF_NAME} (${ADF_RG}) ==="
deploy "linkedService" "linked-service" "--linked-service-name" "--properties"
deploy "dataset"       "dataset"        "--dataset-name"        "--properties"
deploy "pipeline"      "pipeline"       "--name"                "--pipeline"
echo "=== ADF deploy complete ==="
