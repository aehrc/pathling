# Compute and runtime

## clusters

### Subcommands

| Command                 | Description                                            |
| ----------------------- | ------------------------------------------------------ |
| `create`                | Provision a new cluster                                |
| `start`                 | Launch a terminated cluster                            |
| `edit`                  | Modify cluster config (running or terminated)          |
| `update`                | Partial config update via update mask                  |
| `resize`                | Adjust worker count (running clusters only)            |
| `restart`               | Reboot a cluster                                       |
| `delete`                | Terminate asynchronously                               |
| `permanent-delete`      | Irreversibly remove cluster                            |
| `get`                   | Fetch cluster details                                  |
| `list`                  | Active/pinned clusters + terminated within 30 days     |
| `events`                | Activity logs with pagination                          |
| `pin`                   | Preserve beyond 30-day retention (admin)               |
| `unpin`                 | Remove from permanent retention (admin)                |
| `change-owner`          | Transfer ownership (admin, cluster must be terminated) |
| `spark-versions`        | List compatible Spark versions                         |
| `list-node-types`       | Available node types                                   |
| `list-zones`            | Availability zones                                     |
| `get-permission-levels` | Available permission tiers                             |
| `get-permissions`       | Current access controls                                |
| `set-permissions`       | Replace all permissions                                |
| `update-permissions`    | Modify existing permissions                            |

### Examples

```bash
# Create cluster from JSON config
databricks clusters create --json @cluster-config.json

# Start a terminated cluster, don't wait
databricks clusters start 0123-456789-abcdef --no-wait

# Edit cluster configuration
databricks clusters edit 0123-456789-abcdef --json @updated-config.json

# Resize running cluster to 5 workers
databricks clusters resize 0123-456789-abcdef --json '{"num_workers": 5}'

# List all clusters as JSON
databricks clusters list -o json

# Get cluster details
databricks clusters get 0123-456789-abcdef

# List available Spark versions
databricks clusters spark-versions

# Pin a cluster
databricks clusters pin 0123-456789-abcdef

# Terminate a cluster
databricks clusters delete 0123-456789-abcdef

# Permanently delete
databricks clusters permanent-delete 0123-456789-abcdef
```

## libraries

### Subcommands

| Command                | Description                                                  |
| ---------------------- | ------------------------------------------------------------ |
| `all-cluster-statuses` | Status of all libraries on all clusters                      |
| `cluster-status`       | Library status on a specific cluster                         |
| `install`              | Install libraries (async, use `--json`)                      |
| `uninstall`            | Uninstall libraries (requires cluster restart, use `--json`) |

### Examples

```bash
# Check library status on a cluster
databricks libraries cluster-status 0123-456789-abcdef

# Install a PyPI package
databricks libraries install --json '{
  "cluster_id": "0123-456789-abcdef",
  "libraries": [{"pypi": {"package": "pandas==2.0.0"}}]
}'

# Install a Maven package
databricks libraries install --json '{
  "cluster_id": "0123-456789-abcdef",
  "libraries": [{"maven": {"coordinates": "com.example:lib:1.0.0"}}]
}'

# Uninstall a library
databricks libraries uninstall --json '{
  "cluster_id": "0123-456789-abcdef",
  "libraries": [{"pypi": {"package": "pandas"}}]
}'

# List all library statuses across all clusters
databricks libraries all-cluster-statuses
```
