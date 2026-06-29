#!/usr/bin/env bash
# Run the SQL control-table / stored-procedure scripts against the Azure SQL control DB.
# Pulls the admin password from Key Vault, opens a temporary firewall rule for the runner,
# applies every src/**/sql/**/*.sql in path order, then removes the firewall rule.
# Requires: az CLI (logged in), sqlcmd (mssql-tools).
# Env: SQL_SERVER_FQDN, SQL_DB, SQL_RG, SQL_ADMIN_USER, KV_NAME, SQL_PWD_SECRET_NAME
set -euo pipefail
: "${SQL_SERVER_FQDN:?}"; : "${SQL_DB:?}"; : "${SQL_RG:?}"; : "${SQL_ADMIN_USER:?}"
: "${KV_NAME:?}"; : "${SQL_PWD_SECRET_NAME:?}"

server_name="${SQL_SERVER_FQDN%%.*}"
pwd=$(az keyvault secret show --vault-name "$KV_NAME" --name "$SQL_PWD_SECRET_NAME" --query value -o tsv)
runner_ip=$(curl -s https://api.ipify.org)

echo "=== Opening temporary firewall rule for runner ${runner_ip} ==="
az sql server firewall-rule create -g "$SQL_RG" -s "$server_name" -n "gh-runner-temp" \
  --start-ip-address "$runner_ip" --end-ip-address "$runner_ip" --only-show-errors
cleanup() { az sql server firewall-rule delete -g "$SQL_RG" -s "$server_name" -n "gh-runner-temp" --only-show-errors --yes 2>/dev/null || true; }
trap cleanup EXIT

echo "=== Applying SQL scripts to ${SQL_DB} ==="
while IFS= read -r f; do
  [ -f "$f" ] || continue
  echo ">> $f"
  sqlcmd -S "$SQL_SERVER_FQDN" -d "$SQL_DB" -U "$SQL_ADMIN_USER" -P "$pwd" -i "$f" -b
done < <(find src -path "*sql/*" -name "*.sql" | sort)
echo "=== SQL deploy complete ==="
