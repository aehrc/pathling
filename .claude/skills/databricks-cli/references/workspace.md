# Workspace, files, repos, and secrets

## fs (file system)

Operates on Unity Catalog volumes (`dbfs:/Volumes/...`) and DBFS (`dbfs:/...`).

| Command            | Description          | Key flags                       |
| ------------------ | -------------------- | ------------------------------- |
| `cat FILE_PATH`    | Output file contents |                                 |
| `cp SOURCE TARGET` | Copy files/dirs      | `--overwrite`, `-r/--recursive` |
| `ls DIR_PATH`      | List directory       | `--absolute`, `-l/--long`       |
| `mkdir DIR_PATH`   | Create directory     |                                 |
| `rm PATH`          | Remove file/dir      | `-r/--recursive`                |

### Examples

```bash
# Read a file from a volume
databricks fs cat dbfs:/Volumes/main/default/my-volume/data.csv

# Copy local directory to a volume recursively
databricks fs cp ./local-data dbfs:/Volumes/main/default/my-volume/data -r

# Copy with overwrite
databricks fs cp ./updated.csv dbfs:/Volumes/main/default/my-volume/data.csv --overwrite

# List volume contents (long format, absolute paths)
databricks fs ls dbfs:/Volumes/main/default/my-volume -l --absolute

# Create a directory
databricks fs mkdir dbfs:/Volumes/main/default/my-volume/new-dir

# Remove recursively
databricks fs rm dbfs:/Volumes/main/default/my-volume/old-data -r
```

## workspace

| Command                    | Description                       | Key flags                                                                |
| -------------------------- | --------------------------------- | ------------------------------------------------------------------------ |
| `list PATH`                | List directory contents           | `--notebooks-modified-after`                                             |
| `get-status PATH`          | Object/directory status           |                                                                          |
| `import TARGET_PATH`       | Import objects                    | `--file`, `--format`, `--language`, `--overwrite`                        |
| `import-dir SOURCE TARGET` | Recursive import                  | `--overwrite`                                                            |
| `export SOURCE_PATH`       | Export objects                    | `--file`, `--format` (AUTO, DBC, HTML, JUPYTER, RAW, R_MARKDOWN, SOURCE) |
| `export-dir SOURCE TARGET` | Recursive export                  | `--overwrite`                                                            |
| `mkdirs PATH`              | Create directories (with parents) |                                                                          |
| `delete PATH`              | Remove objects/dirs               | `--recursive`                                                            |

### Examples

```bash
# List workspace root
databricks workspace list /

# Export a notebook as source
databricks workspace export /Users/me/notebook --file ./notebook.py --format SOURCE

# Import a notebook
databricks workspace import /Users/me/notebook --file ./notebook.py --language PYTHON --overwrite

# Recursively export a directory
databricks workspace export-dir /Users/me/project ./local-project

# Recursively import
databricks workspace import-dir ./local-project /Users/me/project --overwrite

# Create nested directories
databricks workspace mkdirs /Users/me/project/subdir

# Delete recursively
databricks workspace delete /Users/me/old-project --recursive
```

## repos

| Command                  | Description          | Key flags           |
| ------------------------ | -------------------- | ------------------- |
| `create URL [PROVIDER]`  | Create and link repo | `--path`            |
| `get REPO_ID_OR_PATH`    | Get repo info        |                     |
| `list`                   | List repos           | `--path-prefix`     |
| `update REPO_ID_OR_PATH` | Switch branch/tag    | `--branch`, `--tag` |
| `delete REPO_ID_OR_PATH` | Delete repo          |                     |

Providers: `gitHub`, `bitbucketCloud`, `gitLab`, `azureDevOpsServices`, `gitHubEnterprise`, `bitbucketServer`, `gitLabEnterpriseEdition`, `awsCodeCommit`.

### Examples

```bash
# Clone a GitHub repo
databricks repos create https://github.com/user/repo.git gitHub --path /Repos/me/repo

# Switch to a branch
databricks repos update /Repos/me/repo --branch feature-branch

# Switch to a tag
databricks repos update 12345 --tag v2.0.0

# List repos under a path
databricks repos list --path-prefix /Repos/me

# Delete a repo
databricks repos delete /Repos/me/old-repo
```

## secrets

| Command                              | Description                        | Key flags                                                                         |
| ------------------------------------ | ---------------------------------- | --------------------------------------------------------------------------------- |
| `create-scope SCOPE`                 | Create scope                       | `--initial-manage-principal`, `--scope-backend-type` (DATABRICKS, AZURE_KEYVAULT) |
| `delete-scope SCOPE`                 | Delete scope                       |                                                                                   |
| `list-scopes`                        | List all scopes                    |                                                                                   |
| `put-secret SCOPE KEY`               | Store a secret                     | `--string-value`, `--bytes-value`                                                 |
| `get-secret SCOPE KEY`               | Retrieve secret bytes              |                                                                                   |
| `delete-secret SCOPE KEY`            | Delete a secret                    |                                                                                   |
| `list-secrets SCOPE`                 | List keys in scope (metadata only) |                                                                                   |
| `put-acl SCOPE PRINCIPAL PERMISSION` | Set ACL                            | Permission: `MANAGE`, `WRITE`, `READ`                                             |
| `get-acl SCOPE PRINCIPAL`            | Get ACL details                    |                                                                                   |
| `list-acls SCOPE`                    | List all ACLs for scope            |                                                                                   |
| `delete-acl SCOPE PRINCIPAL`         | Remove ACL                         |                                                                                   |

Constraints: key max 128 chars (alphanumeric, dashes, underscores, periods), value max 128 KB, max 1000 secrets per scope.

### Examples

```bash
# Create a secret scope
databricks secrets create-scope my-scope

# Store a secret
databricks secrets put-secret my-scope db-password --string-value "s3cr3t"

# Store from stdin (multi-line)
echo "multi-line-secret" | databricks secrets put-secret my-scope api-key

# List scopes
databricks secrets list-scopes

# List secrets in a scope
databricks secrets list-secrets my-scope

# Grant read access
databricks secrets put-acl my-scope user@example.com READ

# Delete a secret
databricks secrets delete-secret my-scope old-key

# Delete entire scope
databricks secrets delete-scope my-scope
```
