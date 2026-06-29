# ===============================================================================
# Verify ADLS Access via Unity Catalog
# ===============================================================================
# CRITICAL AUTH POLICY: 
# Databricks -> ADLS is authorized by Unity Catalog (the Databricks Access Connector 
# backs a UC Storage Credential + External Locations). 
# NEVER use storage account keys, NEVER OAuth client secrets, NEVER plaintext passwords.
# NO fs.azure.* auth is set here. We simply read/write abfss:// paths.
# Mounting is an anti-pattern under Unity Catalog. This notebook verifies direct access.

dbutils.widgets.text("storage_account", "stdataplatformdevnda0jg")
storage_account = dbutils.widgets.get("storage_account")

print(f"Verifying Unity Catalog External Location access for storage account: {storage_account}")

layers = ["bronze", "silver", "gold"]
success_count = 0

for layer in layers:
    path = f"abfss://{layer}@{storage_account}.dfs.core.windows.net/"
    try:
        # Attempt to list the directory to verify UC authorization
        files = dbutils.fs.ls(path)
        print(f"✅ SUCCESS: Access verified for {layer} layer at {path}")
        success_count += 1
    except Exception as e:
        print(f"❌ FAILED: Cannot access {layer} layer at {path}")
        print(f"Error: {str(e)}")
        print("Ensure Unity Catalog External Location and Storage Credential are correct.")

if success_count == len(layers):
    print("\nAll layers verified successfully. Unity Catalog authorization is working.")
else:
    raise Exception("One or more layers failed access verification.")