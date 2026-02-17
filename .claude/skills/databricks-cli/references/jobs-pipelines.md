# Jobs and pipelines

## jobs

### Job management

| Command         | Description                                                   |
| --------------- | ------------------------------------------------------------- |
| `create`        | Create a new job (`--json`)                                   |
| `get JOB_ID`    | Get job details                                               |
| `list`          | List jobs (`--name`, `--limit`, `--offset`, `--expand-tasks`) |
| `update JOB_ID` | Add/update/remove specific settings (`--json`)                |
| `reset`         | Overwrite all job settings (`--json`)                         |
| `delete JOB_ID` | Delete a job                                                  |

### Run management

| Command                 | Description                                                                 |
| ----------------------- | --------------------------------------------------------------------------- |
| `run-now JOB_ID`        | Trigger a job run                                                           |
| `submit`                | One-time run (not saved as a job)                                           |
| `cancel-run RUN_ID`     | Cancel a run                                                                |
| `cancel-all-runs`       | Cancel all active runs of a job (`--job-id`)                                |
| `get-run RUN_ID`        | Run metadata (`--include-history`, `--include-resolved-values`)             |
| `get-run-output RUN_ID` | Output and metadata of a single task run                                    |
| `list-runs`             | List runs (`--job-id`, `--active-only`, `--completed-only`, `--run-type`)   |
| `repair-run RUN_ID`     | Re-run failed tasks (`--rerun-all-failed-tasks`, `--rerun-dependent-tasks`) |
| `export-run RUN_ID`     | Export run (`--views-to-export`: ALL, CODE, DASHBOARDS)                     |
| `delete-run RUN_ID`     | Delete a non-active run                                                     |

### Key flags

- `--no-wait` - Return immediately without waiting for completion.
- `--timeout duration` - Max wait time (default `20m0s`).
- `--json JSON` - Inline JSON or `@path` to file.
- `--idempotency-token` - Prevent duplicate runs.
- `--performance-target` - `PERFORMANCE_OPTIMIZED` or `STANDARD`.

### Examples

```bash
# Create a job from JSON file
databricks jobs create --json @job-definition.json

# Run a job and wait for completion
databricks jobs run-now 12345

# Run a job without waiting
databricks jobs run-now 12345 --no-wait

# Submit a one-time run
databricks jobs submit --json @one-time-run.json --no-wait

# List all jobs matching a name
databricks jobs list --name "ETL Pipeline"

# List recent runs for a job
databricks jobs list-runs --job-id 12345 --limit 5

# Get run output
databricks jobs get-run-output 67890

# Cancel a run
databricks jobs cancel-run 67890

# Repair failed tasks in a run
databricks jobs repair-run 67890 --rerun-all-failed-tasks

# Update specific job settings
databricks jobs update 12345 --json '{"new_settings": {"max_retries": 3}}'
```

## pipelines

### Project management (local pipeline projects)

| Command                   | Description                                                                                      |
| ------------------------- | ------------------------------------------------------------------------------------------------ |
| `init`                    | Initialise a new pipeline project                                                                |
| `deploy`                  | Upload project files to workspace (`--auto-approve`, `--fail-on-active-runs`, `--force-lock`)    |
| `destroy`                 | Remove a pipeline project (`--auto-approve`, `--force-lock`)                                     |
| `run [KEY]`               | Run the pipeline (`--full-refresh`, `--full-refresh-all`, `--refresh`, `--restart`, `--no-wait`) |
| `dry-run [KEY]`           | Validate without materialising (`--no-wait`, `--restart`)                                        |
| `stop [KEY\|PIPELINE_ID]` | Stop active run (`--no-wait`, `--timeout`)                                                       |
| `history [KEY]`           | Past executions (`--start-time`, `--end-time`)                                                   |
| `logs [KEY]`              | Event logs (`--level`, `--event-type`, `--update-id`, `-n`)                                      |
| `open [KEY]`              | Open pipeline in browser                                                                         |
| `generate`                | Generate config for existing pipeline (`--existing-pipeline-dir`, `--force`)                     |

### Object management (remote pipeline objects)

| Command                            | Description                                                                                                     |
| ---------------------------------- | --------------------------------------------------------------------------------------------------------------- |
| `create`                           | Create pipeline (`--json`)                                                                                      |
| `get PIPELINE_ID`                  | Get pipeline details                                                                                            |
| `update PIPELINE_ID`               | Update config (many flags: `--name`, `--catalog`, `--schema`, `--continuous`, `--photon`, `--serverless`, etc.) |
| `delete PIPELINE_ID`               | Delete pipeline                                                                                                 |
| `list-pipelines`                   | List all pipelines (`--filter`, `--max-results`)                                                                |
| `start-update PIPELINE_ID`         | Start update (`--cause`, `--full-refresh`, `--validate-only`)                                                   |
| `get-update PIPELINE_ID UPDATE_ID` | Get specific update                                                                                             |
| `list-updates PIPELINE_ID`         | List updates                                                                                                    |
| `list-pipeline-events PIPELINE_ID` | List events (`--filter`, `--max-results`)                                                                       |

### Examples

```bash
# Initialise a new pipeline project
databricks pipelines init

# Deploy pipeline project
databricks pipelines deploy --auto-approve

# Run a pipeline with full refresh
databricks pipelines run my-pipeline --full-refresh-all

# View pipeline logs (errors only, last 20)
databricks pipelines logs my-pipeline --level ERROR -n 20

# List all pipelines
databricks pipelines list-pipelines

# Start a pipeline update via API
databricks pipelines start-update abc123-pipeline-id --json '{"full_refresh": true}'

# Stop a running pipeline
databricks pipelines stop my-pipeline
```
