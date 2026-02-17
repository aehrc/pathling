## 1. Project setup

- [x] 1.1 Create `scripts/test_databricks_release.py` with argument parsing (argparse) for all required and optional parameters
- [x] 1.2 Add helper functions for Databricks CLI/API invocation and error handling

## 2. Cluster configuration

- [x] 2.1 Implement cluster library configuration — set Maven (with snapshots repo) and PyPI packages, removing any previous Pathling libraries
- [x] 2.2 Implement cluster environment configuration — set Databricks runtime version and `JNAME=zulu21-ca-amd64` environment variable

## 3. R package handling

- [x] 3.1 Implement R package download from GitHub Actions artifacts using `gh run download`
- [x] 3.2 Implement R package upload to DBFS using the Databricks CLI

## 4. Cluster startup

- [x] 4.1 Implement cluster start with polling until RUNNING state or timeout

## 5. Notebook execution

- [x] 5.1 Implement notebook run submission via the Jobs API (`runs/submit`)
- [x] 5.2 Implement run polling with timeout, cancellation on timeout, and error reporting
- [x] 5.3 Implement summary output — print pass/fail for each notebook and exit with appropriate status

## 6. Testing and documentation

- [x] 6.1 Write tests for the script (argument validation, error cases, mocked API interactions)
- [x] 6.2 Update the release checklist to reference the new script
- [x] 6.3 Add usage documentation (inline help text and a brief section in the script header)
