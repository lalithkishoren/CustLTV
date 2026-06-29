#!/usr/bin/env bash
set -euo pipefail

echo "========================================"
echo "Running Code Quality Linters"
echo "========================================"

# 1. Python Linting (Black & Flake8) for Databricks PySpark/DLT code
echo "-> Running Black (Python Formatter Check)..."
black --check src/databricks/ tests/

echo "-> Running Flake8 (Python Linter)..."
flake8 src/databricks/ tests/ --max-line-length=100 --ignore=E203,W503

# 2. SQL Linting (SQLFluff) for Databricks SQL / Gold Layer queries
echo "-> Running SQLFluff (Databricks Dialect)..."
if [ -d "src/sql" ]; then
  sqlfluff lint src/sql/ --dialect databricks
else
  echo "No SQL directory found. Skipping SQLFluff."
fi

# 3. Terraform Linting (tflint) for Infrastructure as Code
echo "-> Running TFLint (Terraform)..."
cd infrastructure
tflint --init
tflint
cd ..

echo "========================================"
echo "All linting checks passed successfully!"
echo "========================================"