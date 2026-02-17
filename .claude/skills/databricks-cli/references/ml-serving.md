# ML and serving

## serving-endpoints

### Core management

| Command              | Description                    | Key flags                                                                     |
| -------------------- | ------------------------------ | ----------------------------------------------------------------------------- |
| `create NAME`        | Create endpoint                | `--json`, `--budget-policy-id`, `--route-optimized`, `--no-wait`, `--timeout` |
| `get NAME`           | Get endpoint details           |                                                                               |
| `list`               | List all endpoints             |                                                                               |
| `update-config NAME` | Update served entities/traffic | `--json`, `--no-wait`, `--timeout`                                            |
| `delete NAME`        | Remove endpoint                |                                                                               |
| `patch NAME`         | Add/remove tags                | `--json`                                                                      |

### Querying

| Command      | Description            | Key flags                                                    |
| ------------ | ---------------------- | ------------------------------------------------------------ |
| `query NAME` | Send inference request | `--json`, `--max-tokens`, `--n`, `--stream`, `--temperature` |

### Monitoring

| Command                             | Description                           |
| ----------------------------------- | ------------------------------------- |
| `build-logs NAME SERVED_MODEL_NAME` | Build logs for a served model         |
| `logs NAME SERVED_MODEL_NAME`       | Service logs for a served model       |
| `export-metrics NAME`               | Prometheus/OpenMetrics format metrics |
| `get-open-api NAME`                 | OpenAPI schema                        |

### AI Gateway

| Command               | Description                                     |
| --------------------- | ----------------------------------------------- |
| `put-ai-gateway NAME` | Configure AI Gateway (`--json`)                 |
| `put NAME`            | Update rate limits (deprecated, use AI Gateway) |

### Examples

```bash
# Create a serving endpoint
databricks serving-endpoints create my-endpoint --json @endpoint-config.json

# Query an endpoint
databricks serving-endpoints query my-endpoint --json '{
  "inputs": [{"text": "Hello world"}]
}'

# Query with LLM parameters
databricks serving-endpoints query my-llm-endpoint --json '{
  "messages": [{"role": "user", "content": "Explain Spark"}]
}' --max-tokens 500 --temperature 0.7

# Stream response
databricks serving-endpoints query my-llm-endpoint --json '{
  "messages": [{"role": "user", "content": "Write a poem"}]
}' --stream

# Update endpoint config
databricks serving-endpoints update-config my-endpoint --json @updated-config.json

# Get build logs
databricks serving-endpoints build-logs my-endpoint my-model

# Get service logs
databricks serving-endpoints logs my-endpoint my-model

# Export metrics
databricks serving-endpoints export-metrics my-endpoint

# Delete endpoint
databricks serving-endpoints delete my-endpoint
```

## experiments

`create`, `get`, `get-by-name`, `list`, `update`, `delete`, `restore`, `set-experiment-tag` - MLflow experiment management.

`create-run`, `get-run`, `update-run`, `delete-run`, `delete-runs`, `restore-runs`, `search-runs`, `log-batch`, `log-inputs`, `log-metric`, `log-model`, `log-param`, `set-tag`, `delete-tag` - MLflow run management.

`list-artifacts` - List run artifacts.

## model-registry

`create-model`, `get-model`, `list-models`, `update-model`, `delete-model`, `rename-model` - Model CRUD.

`create-model-version`, `get-model-version`, `list-model-versions`, `update-model-version`, `delete-model-version`, `get-model-version-download-uri` - Version management.

`create-transition-request`, `approve-transition-request`, `reject-transition-request`, `list-transition-requests`, `delete-transition-request`, `transition-stage` - Stage transitions.

`create-comment`, `update-comment`, `delete-comment` - Version comments.

`create-webhook`, `list-webhooks`, `update-webhook`, `delete-webhook`, `test-registry-webhook` - Registry webhooks.

## feature-engineering

`create-feature-table`, `get-feature-table`, `list-feature-tables`, `update-feature-table`, `delete-feature-table` - Feature table management.

## vector-search-endpoints / vector-search-indexes

`create-endpoint`, `get-endpoint`, `list-endpoints`, `delete-endpoint` - Vector search endpoint management.

`create-index`, `get-index`, `list-indexes`, `delete-index`, `sync-index`, `query-index`, `query-next-page`, `scan-index`, `upsert-data-vector-index`, `delete-data-vector-index` - Vector index operations.
