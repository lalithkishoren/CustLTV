#!/usr/bin/env bash
set -euo pipefail

echo "========================================"
echo "Running Unit & Data Quality Tests"
echo "========================================"

# Set PYTHONPATH so tests can import src modules
export PYTHONPATH="${PYTHONPATH:-}:$(pwd)/src"

# Run Pytest with coverage
# This executes local PySpark tests (using a local SparkSession fixture)
# to validate DLT transformation logic, SCD2 merges, and KPI calculations
# before deploying to the Databricks workspace.
echo "-> Running Pytest..."
pytest tests/ \
  -v \
  --cov=src/databricks \
  --cov-report=term-missing \
  --cov-report=xml:coverage.xml \
  --junitxml=test-results.xml

echo "========================================"
echo "All tests passed successfully!"
echo "========================================"