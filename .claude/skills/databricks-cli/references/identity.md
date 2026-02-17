# Identity and access

## auth

| Command    | Description                           | Key flags                                                                                           |
| ---------- | ------------------------------------- | --------------------------------------------------------------------------------------------------- |
| `login`    | Interactive OAuth login               | `--host`, `--account-id`, `--configure-cluster`, `--configure-serverless`, `--timeout` (default 1h) |
| `describe` | Current auth config details           | `--sensitive` (include tokens in output)                                                            |
| `env`      | Show profile environment settings     | `--host`, `--profile`                                                                               |
| `profiles` | List all config profiles              | `--skip-validate`                                                                                   |
| `token`    | Get cached token (refresh if expired) | `--account-id`, `--timeout`                                                                         |

### Examples

```bash
# Login to a workspace (OAuth U2M)
databricks auth login --host https://my-workspace.cloud.databricks.com

# Login to account console
databricks auth login --host https://accounts.cloud.databricks.com --account-id abc123

# List all profiles
databricks auth profiles

# Show current auth config
databricks auth describe

# Show profile environment variables
databricks auth env -p PROD

# Get current token
databricks auth token -p PROD
```

## current-user

| Command | Description                                   |
| ------- | --------------------------------------------- |
| `me`    | Get authenticated user/service principal info |

```bash
databricks current-user me
```

## users

| Command          | Description    | Key flags                               |
| ---------------- | -------------- | --------------------------------------- |
| `create`         | Create user    | `--json`                                |
| `get USER_ID`    | Get user       |                                         |
| `list`           | List users     | `--filter`, `--sort-by`, `--sort-order` |
| `update USER_ID` | Replace user   | `--json`                                |
| `patch USER_ID`  | Partial update | `--json`                                |
| `delete USER_ID` | Delete user    |                                         |

## groups

| Command           | Description    | Key flags                               |
| ----------------- | -------------- | --------------------------------------- |
| `create`          | Create group   | `--json`                                |
| `get GROUP_ID`    | Get group      |                                         |
| `list`            | List groups    | `--filter`, `--sort-by`, `--sort-order` |
| `update GROUP_ID` | Replace group  | `--json`                                |
| `patch GROUP_ID`  | Partial update | `--json`                                |
| `delete GROUP_ID` | Delete group   |                                         |

## service-principals

| Command        | Description               | Key flags                               |
| -------------- | ------------------------- | --------------------------------------- |
| `create`       | Create service principal  | `--json`                                |
| `get SP_ID`    | Get service principal     |                                         |
| `list`         | List service principals   | `--filter`, `--sort-by`, `--sort-order` |
| `update SP_ID` | Replace service principal | `--json`                                |
| `patch SP_ID`  | Partial update            | `--json`                                |
| `delete SP_ID` | Delete service principal  |                                         |

## permissions

Generic permission management for any resource type.

| Command                                                       | Description                    |
| ------------------------------------------------------------- | ------------------------------ |
| `get REQUEST_OBJECT_TYPE REQUEST_OBJECT_ID`                   | Get permissions                |
| `set REQUEST_OBJECT_TYPE REQUEST_OBJECT_ID`                   | Replace permissions (`--json`) |
| `update REQUEST_OBJECT_TYPE REQUEST_OBJECT_ID`                | Modify permissions (`--json`)  |
| `get-permission-levels REQUEST_OBJECT_TYPE REQUEST_OBJECT_ID` | Available levels               |

Supported object types: `alerts`, `authorization`, `clusters`, `cluster-policies`, `dashboards`, `dbsql-dashboards`, `directories`, `experiments`, `files`, `instance-pools`, `jobs`, `notebooks`, `pipelines`, `queries`, `registered-models`, `repos`, `serving-endpoints`, `warehouses`.

### Examples

```bash
# Get cluster permissions
databricks permissions get clusters 0123-456789-abcdef

# Update job permissions
databricks permissions update jobs 12345 --json '{
  "access_control_list": [
    {"user_name": "user@example.com", "permission_level": "CAN_MANAGE_RUN"}
  ]
}'

# Get available permission levels for a warehouse
databricks permissions get-permission-levels warehouses abc123
```
