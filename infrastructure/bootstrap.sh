#!/usr/bin/env bash
set -e

# Bootstrap script to create the Azure Storage Account for Terraform remote state.
# Run this ONCE before your first `terraform init`.

LOCATION="westeurope"
RESOURCE_GROUP_NAME="rg-terraform-state-core"
# Storage account names must be globally unique, lowercase, alphanumeric, 3-24 chars.
RANDOM_SUFFIX=$(LC_ALL=C tr -dc 'a-z0-9' < /dev/urandom | head -c 6)
STORAGE_ACCOUNT_NAME="sttfstatecore${RANDOM_SUFFIX}"
CONTAINER_NAME="tfstate"

echo "Creating Resource Group: $RESOURCE_GROUP_NAME in $LOCATION..."
az group create --name "$RESOURCE_GROUP_NAME" --location "$LOCATION" --tags Purpose="TerraformState"

echo "Creating Storage Account: $STORAGE_ACCOUNT_NAME..."
az storage account create \
  --name "$STORAGE_ACCOUNT_NAME" \
  --resource-group "$RESOURCE_GROUP_NAME" \
  --location "$LOCATION" \
  --sku Standard_LRS \
  --encryption-services blob \
  --min-tls-version TLS1_2 \
  --allow-blob-public-access false

echo "Creating Storage Container: $CONTAINER_NAME..."
az storage container create \
  --name "$CONTAINER_NAME" \
  --account-name "$STORAGE_ACCOUNT_NAME" \
  --auth-mode login

echo "========================================================"
echo "Bootstrap complete! Update your backend.hcl with:"
echo "resource_group_name  = \"$RESOURCE_GROUP_NAME\""
echo "storage_account_name = \"$STORAGE_ACCOUNT_NAME\""
echo "container_name       = \"$CONTAINER_NAME\""
echo "========================================================"