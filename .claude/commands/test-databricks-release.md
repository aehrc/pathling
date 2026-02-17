# Test Databricks release

Automatically look up all required inputs and run the Databricks release test
script (`scripts/test_databricks_release.py`).

## Overrides

The user may provide overrides after the command: $ARGUMENTS

Parse any overrides from the arguments. Supported overrides (in any order):

- `--cluster-id <id>` — override the default cluster ID
- `--notebooks <path1> <path2> ...` — override the default notebook paths
- `--databricks-runtime <version>` — override the default runtime version
- `--timeout <minutes>` — override the default timeout
- `--profile <name>` — override the Databricks CLI profile (default: `pathling`)

## Steps

### 1. Get Maven version

Read the `<version>` element from the root `pom.xml` (under `<project>`).
This should be a SNAPSHOT version like `9.4.0-SNAPSHOT`.

### 2. Get current branch

Run `git branch --show-current` to get the current branch name.

### 3. Get PyPI dev version

Derive the base version by stripping `-SNAPSHOT` from the Maven version (e.g.,
`9.4.0`). Then query PyPI for the latest pre-release matching that base:

```bash
curl -s https://pypi.org/pypi/pathling/json | python3 -c "
import sys, json
base = '<base_version>'
releases = json.load(sys.stdin)['releases']
matches = sorted([k for k in releases if k.startswith(base + '.')])
print(matches[-1] if matches else base + '.dev0')
"
```

If no match is found, default to `<base_version>.dev0`.

### 4. Get GitHub Actions pre-release run ID

Find the latest successful "Pre-release" workflow run on the current branch:

```bash
gh run list --workflow "Pre-release" --repo aehrc/pathling \
  --branch <branch> --status success --limit 1 \
  --json databaseId --jq '.[0].databaseId'
```

If no successful run is found, stop and tell the user.

### 5. Confirm inputs with the user

Display all resolved inputs and ask the user to confirm before running:

- Maven version
- PyPI version
- GitHub run ID
- Cluster ID
- Databricks runtime
- Notebook paths
- Timeout

### 6. Run the script

Execute the script with all resolved inputs:

```bash
uv run scripts/test_databricks_release.py \
  --maven-version <maven_version> \
  --pypi-version <pypi_version> \
  --github-run-id <run_id> \
  --cluster-id <cluster_id> \
  --notebooks <notebook1> <notebook2> \
  --databricks-runtime <runtime> \
  --timeout <timeout>
```

Note: the script uses the `databricks` and `gh` CLI tools directly. Ensure the
Databricks CLI profile is configured (default profile: `pathling`). If the user
specified a non-default profile, set the `DATABRICKS_CONFIG_PROFILE` environment
variable when running the script.

## Defaults

- **Cluster ID**: `0415-214755-32l8prbg`
- **Notebooks**:
    - `/Shared/SQL on FHIR/SQL on FHIR - Prostate cancer demo (explained, simplified)`
    - `/Shared/R test`
- **Databricks runtime**: `17.3.x-scala2.13`
- **Timeout**: `30` minutes
- **Databricks CLI profile**: `pathling`

## Prerequisites

- `databricks` CLI installed and authenticated (profile `pathling`)
- `gh` CLI installed and authenticated
- A successful "Pre-release" workflow run on the current branch
- A Python dev release published to PyPI for the current version
