## ADDED Requirements

### Requirement: Script accepts release candidate parameters

The script SHALL accept the following required parameters via CLI arguments:

- `--maven-version`: The SNAPSHOT version of the Maven artifacts (e.g., `9.4.0-SNAPSHOT`).
- `--pypi-version`: The dev release version of the Python package on PyPI (e.g., `9.4.0.dev0`).
- `--github-run-id`: The GitHub Actions run ID from a successful pre-release build that has the R package attached as an artifact.
- `--cluster-id`: The Databricks cluster ID to configure and test on.
- `--notebooks`: One or more notebook paths on Databricks to execute as tests.

The script SHALL accept the following optional parameters:

- `--databricks-runtime`: The Databricks runtime version to configure (e.g., `17.3.x-scala2.12`). Defaults to a sensible current value.
- `--timeout`: Maximum time in minutes to wait for cluster startup and each notebook run. Defaults to 30.
- `--github-repo`: The GitHub repository to download the R package from. Defaults to `aehrc/pathling`.

#### Scenario: All required parameters provided

- **WHEN** the script is invoked with all required parameters
- **THEN** it SHALL proceed with cluster configuration and testing.

#### Scenario: Missing required parameter

- **WHEN** any required parameter is omitted
- **THEN** the script SHALL exit with a non-zero status and print a usage message.

### Requirement: Script configures cluster libraries

The script SHALL configure the nominated Databricks cluster with:

1. The Maven library `au.csiro.pathling:library-runtime:<maven-version>` with the Maven Central snapshots repository (`https://central.sonatype.com/repository/maven-snapshots/`) as an additional repository.
2. The PyPI package `pathling==<pypi-version>`.

The script SHALL remove any previously configured Pathling libraries before installing the new ones.

#### Scenario: Cluster libraries configured correctly

- **WHEN** the script configures the cluster
- **THEN** the cluster's library list SHALL include the specified Maven and PyPI packages.

### Requirement: Script configures cluster environment

The script SHALL set the cluster's Databricks runtime version to the value of `--databricks-runtime`.

The script SHALL set the environment variable `JNAME=zulu21-ca-amd64` on the cluster to enable Java 21.

#### Scenario: Runtime and Java version configured

- **WHEN** the script configures the cluster
- **THEN** the cluster SHALL use the specified Databricks runtime and have Java 21 enabled via the JNAME environment variable.

### Requirement: Script downloads and uploads R package

The script SHALL use the GitHub CLI to download the `r-package` artifact from the specified GitHub Actions run ID.

The script SHALL extract the R package tarball from the downloaded artifact and upload it to a path on DBFS (e.g., `dbfs:/FileStore/pathling/pathling_<version>.tar.gz`).

#### Scenario: R package uploaded to DBFS

- **WHEN** the script downloads and uploads the R package
- **THEN** the R package file SHALL be available at the expected DBFS path.

#### Scenario: GitHub Actions run not found

- **WHEN** the specified GitHub Actions run ID does not exist or has no R package artifact
- **THEN** the script SHALL exit with a non-zero status and a descriptive error message.

### Requirement: Script starts the cluster

The script SHALL start the nominated cluster if it is not already running.

The script SHALL wait for the cluster to reach the `RUNNING` state, polling at regular intervals up to the configured timeout.

#### Scenario: Cluster starts successfully

- **WHEN** the cluster is started
- **THEN** the script SHALL proceed to notebook execution once the cluster reaches RUNNING state.

#### Scenario: Cluster fails to start within timeout

- **WHEN** the cluster does not reach RUNNING state within the timeout
- **THEN** the script SHALL exit with a non-zero status and an error message indicating the timeout.

### Requirement: Script executes test notebooks

The script SHALL execute each nominated notebook on the configured cluster using the Databricks Jobs API (`runs/submit`).

The script SHALL wait for each notebook run to complete, polling at regular intervals up to the configured timeout.

#### Scenario: All notebooks pass

- **WHEN** all nominated notebooks complete with a `SUCCESS` result state
- **THEN** the script SHALL exit with status 0 and print a summary indicating all notebooks passed.

#### Scenario: A notebook fails

- **WHEN** any nominated notebook completes with a `FAILED` result state
- **THEN** the script SHALL print the error details from the run and exit with a non-zero status.

#### Scenario: A notebook run times out

- **WHEN** a notebook run does not complete within the timeout
- **THEN** the script SHALL cancel the run, print an error message, and exit with a non-zero status.

### Requirement: Script provides clear progress output

The script SHALL print progress messages to stdout at each major step: parameter validation, cluster configuration, R package download/upload, cluster startup, and each notebook execution.

Error messages SHALL be printed to stderr.

#### Scenario: Progress output during normal execution

- **WHEN** the script runs successfully
- **THEN** each major step SHALL produce a progress message indicating what is happening and when it completes.
