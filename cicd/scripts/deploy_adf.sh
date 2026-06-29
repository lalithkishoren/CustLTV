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

# Pipelines must be created in DEPENDENCY order: a pipeline that ExecutePipeline-references
# another must be created AFTER it. Glob/alphabetical order breaks this (orchestrators sort
# before their children). Deploy in topological order via python3.
echo ">> deploying pipelines (topological order)"
python3 - "$ADF_RG" "$ADF_NAME" <<'PYEOF'
import json, glob, sys, subprocess, tempfile, os
rg, adf = sys.argv[1], sys.argv[2]
def pdeps(o, acc):
    if isinstance(o, dict):
        if o.get("type") == "PipelineReference" and o.get("referenceName"): acc.add(o["referenceName"])
        for v in o.values(): pdeps(v, acc)
    elif isinstance(o, list):
        for v in o: pdeps(v, acc)
items = {}
for f in glob.glob("src/**/adf/pipeline/*.json", recursive=True):
    d = json.load(open(f, encoding="utf-8")); acc = set(); pdeps(d, acc)
    items[d["name"]] = (d["properties"], acc)
order, done, rem = [], set(), dict(items)
while rem:
    prog = False
    for n, (p, deps) in list(rem.items()):
        if (deps - {n}) <= done:
            order.append(n); done.add(n); del rem[n]; prog = True
    if not prog:
        order += list(rem); break
tmp = tempfile.mkdtemp()
for n in order:
    props, _ = items[n]
    pf = os.path.join(tmp, n + ".json"); open(pf, "w", encoding="utf-8").write(json.dumps(props))
    r = subprocess.run(["az", "datafactory", "pipeline", "create", "--resource-group", rg,
                        "--factory-name", adf, "--name", n, "--pipeline", "@" + pf, "--only-show-errors"],
                       capture_output=True, text=True)
    print(f"  {'ok ' if r.returncode==0 else 'ERR'} pipeline: {n}")
    if r.returncode != 0:
        print("     " + (r.stderr or r.stdout).strip()[:200]); sys.exit(1)
PYEOF
echo "=== ADF deploy complete ==="
