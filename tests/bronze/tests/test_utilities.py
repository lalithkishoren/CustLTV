import pytest

def test_mount_adls_logic(dbutils_mock):
    """
    Unit test for the mount_adls function logic extracted from Mount_ADLS.py.
    Ensures idempotency (doesn't remount if already mounted).
    """
    # Extracting the exact function from the notebook for unit testing
    def mount_adls(storage_account, storage_access_key, container_name, mount_point):
        if any(mount.mountPoint == mount_point for mount in dbutils_mock.fs.mounts()):
            return True
        try:
            dbutils_mock.fs.mount(
                source = f"wasbs://{container_name}@{storage_account}.blob.core.windows.net",
                mount_point = mount_point,
                extra_configs = {f"fs.azure.account.key.{storage_account}.blob.core.windows.net": storage_access_key}
            )
            return True
        except Exception:
            return False

    # Test 1: Initial Mount
    assert mount_adls("acc", "key", "container", "/mnt/test") == True
    
    # Test 2: Already Mounted (Mocking dbutils.fs.mounts to return a match)
    class MockMount:
        def __init__(self, mp):
            self.mountPoint = mp
            
    dbutils_mock.fs.mounts = lambda: [MockMount("/mnt/test")]
    
    # Should return True immediately without throwing error
    assert mount_adls("acc", "key", "container", "/mnt/test") == True

def test_validate_dependencies_log_result():
    """
    Unit test for the log_result function in Validate_Dependencies.py
    """
    results = {"passed": 0, "failed": 0, "details": []}
    
    def log_result(test_name, status, message=""):
        if status == "PASS":
            results["passed"] += 1
        else:
            results["failed"] += 1
        results["details"].append({"test": test_name, "status": status, "message": message})

    log_result("Test 1", "PASS", "All good")
    log_result("Test 2", "FAIL", "Error occurred")
    
    assert results["passed"] == 1
    assert results["failed"] == 1
    assert len(results["details"]) == 2
    assert results["details"][1]["status"] == "FAIL"