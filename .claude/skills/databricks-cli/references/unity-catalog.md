# Unity Catalog

## catalogs

| Command       | Description              | Key flags                                                                                                                                |
| ------------- | ------------------------ | ---------------------------------------------------------------------------------------------------------------------------------------- |
| `create`      | Create catalog           | `--comment`, `--connection-name`, `--provider-name`, `--share-name`, `--storage-root`, `--json`                                          |
| `get NAME`    | Get catalog details      | `--include-browse`                                                                                                                       |
| `list`        | List accessible catalogs | `--include-browse`, `--max-results`, `--page-token`                                                                                      |
| `update NAME` | Update catalog           | `--comment`, `--new-name`, `--owner`, `--isolation-mode` (ISOLATED, OPEN), `--enable-predictive-optimization` (DISABLE, ENABLE, INHERIT) |
| `delete NAME` | Delete catalog           | `--force` (for non-empty catalogs)                                                                                                       |

### Examples

```bash
databricks catalogs create --json '{"name": "analytics", "comment": "Analytics catalog"}'
databricks catalogs list -o json
databricks catalogs get analytics
databricks catalogs update analytics --comment "Updated description"
databricks catalogs delete old-catalog --force
```

## schemas

| Command             | Description   | Key flags                                                           |
| ------------------- | ------------- | ------------------------------------------------------------------- |
| `create`            | Create schema | `--json`, `--catalog-name`, `--name`, `--comment`, `--storage-root` |
| `get FULL_NAME`     | Get schema    |                                                                     |
| `list CATALOG_NAME` | List schemas  | `--max-results`, `--page-token`                                     |
| `update FULL_NAME`  | Update schema | `--comment`, `--new-name`, `--owner`, `--json`                      |
| `delete FULL_NAME`  | Delete schema | `--force`                                                           |

## tables

| Command                  | Description     | Key flags                                                                                                                 |
| ------------------------ | --------------- | ------------------------------------------------------------------------------------------------------------------------- |
| `get FULL_NAME`          | Get table info  | `--include-browse`, `--include-delta-metadata`, `--include-manifest-capabilities`                                         |
| `list CATALOG SCHEMA`    | List tables     | `--include-browse`, `--include-delta-metadata`, `--max-results`, `--omit-columns`, `--omit-properties`, `--omit-username` |
| `list-summaries CATALOG` | Table summaries | `--schema-name-pattern`, `--table-name-pattern`, `--max-results`                                                          |
| `exists FULL_NAME`       | Check existence |                                                                                                                           |
| `delete FULL_NAME`       | Delete table    |                                                                                                                           |

Full name format: `catalog.schema.table`.

### Examples

```bash
databricks tables list main default -o json
databricks tables get main.default.my_table
databricks tables exists main.default.my_table
databricks tables list-summaries main --table-name-pattern "user*"
```

## volumes

| Command                                  | Description     | Key flags                            |
| ---------------------------------------- | --------------- | ------------------------------------ |
| `create CATALOG SCHEMA NAME VOLUME_TYPE` | Create volume   | `--comment`, `--storage-location`    |
| `read NAME`                              | Get volume info | `--include-browse`                   |
| `list CATALOG SCHEMA`                    | List volumes    | `--include-browse`, `--max-results`  |
| `update NAME`                            | Update volume   | `--comment`, `--new-name`, `--owner` |
| `delete NAME`                            | Delete volume   |                                      |

Volume types: `MANAGED`, `EXTERNAL`.

### Examples

```bash
databricks volumes create main default my-volume MANAGED --comment "Managed volume"
databricks volumes create main default ext-volume EXTERNAL --storage-location s3://bucket/path
databricks volumes list main default
databricks volumes read main.default.my-volume
```

## grants

| Command                                  | Description                           | Key flags                      |
| ---------------------------------------- | ------------------------------------- | ------------------------------ |
| `get SECURABLE_TYPE FULL_NAME`           | Get permissions (direct only)         | `--principal`, `--max-results` |
| `get-effective SECURABLE_TYPE FULL_NAME` | Get permissions (including inherited) | `--principal`, `--max-results` |
| `update SECURABLE_TYPE FULL_NAME`        | Modify permissions                    | `--json`                       |

### Examples

```bash
databricks grants get catalog analytics
databricks grants get-effective table main.default.users --principal data-team
databricks grants update table main.default.users --json '{
  "changes": [{"principal": "data-readers", "add": ["SELECT"]}]
}'
```

## Other Unity Catalog groups

### storage-credentials

`create`, `get`, `list`, `update`, `delete`, `validate` - Manage credentials for external storage access.

### connections

`create`, `get`, `list`, `update`, `delete` - Manage external data source connections (e.g., MySQL, PostgreSQL).

### functions

`create`, `get`, `list`, `update`, `delete` - Manage user-defined functions.

### metastores

`create`, `get`, `list`, `update`, `delete`, `assign`, `unassign`, `current` - Manage Unity Catalog metastores.

### registered-models / model-versions

`create`, `get`, `list`, `update`, `delete` - Manage MLflow models in Unity Catalog.

### online-tables

`create`, `get`, `delete` - Manage online tables for low-latency serving.

### quality-monitors

`create`, `get`, `list`, `update`, `delete`, `run-refresh`, `list-refreshes`, `get-refresh`, `cancel-refresh` - Data quality monitoring.
