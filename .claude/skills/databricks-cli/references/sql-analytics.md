# SQL and analytics

## warehouses

### Subcommands

| Command                          | Description              | Key flags                                                                                                                                                                                                              |
| -------------------------------- | ------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `create`                         | Create SQL warehouse     | `--name`, `--cluster-size`, `--min-num-clusters`, `--max-num-clusters`, `--auto-stop-mins`, `--enable-photon`, `--enable-serverless-compute`, `--warehouse-type` (PRO, CLASSIC), `--spot-instance-policy`, `--no-wait` |
| `get ID`                         | Get warehouse info       |                                                                                                                                                                                                                        |
| `list`                           | List warehouses          | `--run-as-user-id`                                                                                                                                                                                                     |
| `edit ID`                        | Update config            | Same flags as `create`                                                                                                                                                                                                 |
| `start ID`                       | Activate warehouse       | `--no-wait`, `--timeout`                                                                                                                                                                                               |
| `stop ID`                        | Deactivate warehouse     | `--no-wait`, `--timeout`                                                                                                                                                                                               |
| `delete ID`                      | Remove warehouse         |                                                                                                                                                                                                                        |
| `get-workspace-warehouse-config` | Workspace-level settings |                                                                                                                                                                                                                        |
| `set-workspace-warehouse-config` | Set workspace settings   | `--instance-profile-arn`, `--security-policy` (DATA_ACCESS_CONTROL, NONE, PASSTHROUGH)                                                                                                                                 |

### Examples

```bash
# Create a serverless SQL warehouse
databricks warehouses create --name "Analytics" --cluster-size "Small" \
  --enable-serverless-compute --auto-stop-mins 10

# Create from JSON
databricks warehouses create --json @warehouse-config.json

# Start a warehouse
databricks warehouses start abc123def456

# Stop a warehouse
databricks warehouses stop abc123def456 --no-wait

# List all warehouses
databricks warehouses list -o json

# Edit warehouse config
databricks warehouses edit abc123def456 --max-num-clusters 5

# Get workspace warehouse config
databricks warehouses get-workspace-warehouse-config
```

## queries

| Command     | Description   | Key flags                               |
| ----------- | ------------- | --------------------------------------- |
| `create`    | Create query  | `--json`, `--auto-resolve-display-name` |
| `get ID`    | Get query     |                                         |
| `list`      | List queries  | `--page-size`, `--page-token`           |
| `update ID` | Update query  | `--json`, `--auto-resolve-display-name` |
| `delete ID` | Move to trash |                                         |

### Examples

```bash
# Create a query
databricks queries create --json '{
  "display_name": "Active Users",
  "warehouse_id": "abc123",
  "query_text": "SELECT * FROM main.default.users WHERE active = true"
}'

# List queries
databricks queries list --page-size 20

# Get a query
databricks queries get abc123-query-id

# Delete a query
databricks queries delete abc123-query-id
```

## alerts

`create`, `get`, `list`, `update`, `delete` - Manage SQL alerts that trigger on query conditions.

## dashboards / lakeview

`create`, `get`, `list`, `update`, `trash`, `publish`, `unpublish`, `migrate` - Manage dashboards.

## query-history

`list` - List past query executions with filters.
