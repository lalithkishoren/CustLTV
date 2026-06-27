# Databricks notebook source
# ============================================================================
# Mount ADLS Gen2 to Databricks
# ============================================================================

dbutils.widgets.text("storage_account", "stclvbronzeprod001")
dbutils.widgets.text("storage_access_key", "{{PLACEHOLDER_STORAGE_KEY}}")
dbutils.widgets.text("container_name", "datalake")

storage_account = dbutils.widgets.get("storage_account")
storage_access_key = dbutils.widgets.get("storage_access_key")
container_name = dbutils.widgets.get("container_name")

mount_point = f"/mnt/{container_name}"

# Configure Spark to access ADLS Gen2
spark.conf.set(
    f"fs.azure.account.key.{storage_account}.dfs.core.windows.net",
    storage_access_key
)

def mount_adls(storage_account, storage_access_key, container_name, mount_point):
    # Check if already mounted
    if any(mount.mountPoint == mount_point for mount in dbutils.fs.mounts()):
        print(f"Directory {mount_point} is already mounted.")
        return True
    
    try:
        dbutils.fs.mount(
            source = f"wasbs://{container_name}@{storage_account}.blob.core.windows.net",
            mount_point = mount_point,
            extra_configs = {f"fs.azure.account.key.{storage_account}.blob.core.windows.net": storage_access_key}
        )
        print(f"Successfully mounted {container_name} to {mount_point}")
        return True
    except Exception as e:
        print(f"Error mounting {container_name}: {str(e)}")
        return False

# Execute mount
mount_adls(storage_account, storage_access_key, container_name, mount_point)

# Verify mount
display(dbutils.fs.ls(mount_point))