# Databricks Asset Bundles

Infrastructure-as-code for Databricks. Express jobs, pipelines, and artifacts as code; validate, deploy, and run them programmatically.

## Subcommands

| Command    | Description                    | Key flags                                                                                         |
| ---------- | ------------------------------ | ------------------------------------------------------------------------------------------------- |
| `init`     | Initialise from template       | `--config-file`, `--output-dir`                                                                   |
| `validate` | Verify config syntax           |                                                                                                   |
| `plan`     | Preview deployment actions     |                                                                                                   |
| `deploy`   | Deploy to workspace            | `--auto-approve`, `-c/--cluster-id`, `--fail-on-active-runs`, `--force`, `--force-lock`, `--plan` |
| `run`      | Execute jobs/pipelines/scripts | See run flags below                                                                               |
| `destroy`  | Delete deployed resources      | `--auto-approve`, `--force-lock`                                                                  |
| `open`     | Open resource in browser       |                                                                                                   |
| `schema`   | Output JSON Schema for config  |                                                                                                   |
| `summary`  | Bundle identity and overview   |                                                                                                   |
| `sync`     | One-way sync to workspace      | `--dry-run`, `--full`, `--watch`, `--interval`                                                    |

### deploy flags

- `--auto-approve` - Skip interactive approvals.
- `-c, --cluster-id` - Override deployment cluster.
- `--fail-on-active-runs` - Prevent deployment if runs are active.
- `--force` - Override Git branch validation.
- `--force-lock` - Override stale deployment locks.

### run flags

- `--params key=value` - Job-level parameters.
- `--validate-only` - Validate without executing.
- `--full-refresh-all` - Reset all tables (pipelines).
- `--refresh-all` - Update all tables (pipelines).
- `--no-wait` - Don't wait for completion.
- `--restart` - Restart if already running.
- `--notebook-params`, `--python-params`, `--sql-params`, `--dbt-commands` - Task-type-specific params.
- `-- <command>` - Execute arbitrary scripts (e.g., `-- python3 -c 'print("hi")'`).

## deployment subcommands

| Command              | Description                                           |
| -------------------- | ----------------------------------------------------- |
| `deployment bind`    | Link bundle resources to existing workspace resources |
| `deployment unbind`  | Remove resource bindings                              |
| `deployment migrate` | Transition from Terraform to direct deployment        |

Bindable resources: apps, clusters, dashboards, jobs, model serving endpoints, pipelines, quality monitors, registered models, schemas, volumes.

## generate subcommands

| Command              | Description                               | Key flags                                                    |
| -------------------- | ----------------------------------------- | ------------------------------------------------------------ |
| `generate app`       | Generate app config                       | `--bind`, `-d/--config-dir`, `-f/--force`, `-s/--source-dir` |
| `generate dashboard` | Generate dashboard config                 | Same as above                                                |
| `generate job`       | Generate job config (notebook tasks only) | Same as above                                                |
| `generate pipeline`  | Generate pipeline config                  | Same as above                                                |

## Bundle-specific global flag

`--var` - Set bundle variables (e.g., `--var "env=prod"`).

## Examples

```bash
# Initialise a new bundle project
databricks bundle init

# Validate bundle configuration
databricks bundle validate

# Preview what will be deployed
databricks bundle plan

# Deploy to the dev target
databricks bundle deploy -t dev

# Deploy with auto-approve
databricks bundle deploy -t prod --auto-approve

# Run a job defined in the bundle
databricks bundle run my-job -t dev

# Run with parameters
databricks bundle run my-job --params "date=2024-01-01"

# Run a pipeline with full refresh
databricks bundle run my-pipeline --full-refresh-all

# Execute a script
databricks bundle run -- python3 scripts/process.py

# Sync files (watch mode)
databricks bundle sync --watch -t dev

# Destroy deployed resources
databricks bundle destroy -t dev --auto-approve

# Generate config for existing job
databricks bundle generate job --bind

# Bind bundle resource to existing workspace resource
databricks bundle deployment bind my-job
```
