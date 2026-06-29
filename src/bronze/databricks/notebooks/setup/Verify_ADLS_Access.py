# Databricks notebook source
# ===============================================================================
# Verify_ADLS_Access.py
# PURPOSE: Verify Unity Catalog access to ADLS.
# CRITICAL: Under Unity Catalog, the Databricks Access Connector authorizes the 
# External Locations. DO NOT mount and DO NOT set fs.azure auth.
# ===============================================================================

dbutils.widgets.text("storage_account", "{{PLACEHOLDER_STORAGE_ACCOUNT}}", "Storage Account Name")
storage_account = dbutils.widgets.get("storage_account")

print(f"Verifying Unity Catalog access to storage account: {storage_account}")
print("NOTE: Access is granted by the UC External Location + Storage Credential (Access Connector).")
print("NO dbutils.fs.mount, NO account keys, NO service principal, NO fs.azure.* auth is used here.\n")

layers = ["bronze", "silver", "gold"]

for layer in layers:
    path = f"abfss://{layer}@{storage_account}.dfs.core.windows.net/"
    try:
        files = dbutils.fs.ls(path)
        print(f"[PASS] Successfully accessed {layer} layer at {path}")
        print(f"       Found {len(files)} items in root.")
    except Exception as e:
        print(f"[FAIL] Failed to access {layer} layer at {path}")
        print(f"       Error: {str(e)}")
        print("       Check Unity Catalog External Location and Storage Credential permissions.")

print("\nVerification complete.")