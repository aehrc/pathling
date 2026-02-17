# Account administration

The `account` command group manages account-level resources across workspaces. Requires account-level authentication.

```bash
databricks auth login --host https://accounts.cloud.databricks.com --account-id <id>
databricks account <subgroup> <command> -p <account-profile>
```

## Subgroups

### Identity and access

| Subgroup               | Commands                                                               |
| ---------------------- | ---------------------------------------------------------------------- |
| `access-control`       | `get-assignable-roles-for-resource`, `get-rule-set`, `update-rule-set` |
| `groups`               | `create`, `delete`, `get`, `list`, `patch`, `update`                   |
| `service-principals`   | `create`, `delete`, `get`, `list`, `patch`, `update`                   |
| `users`                | `create`, `delete`, `get`, `list`, `patch`, `update`                   |
| `workspace-assignment` | `delete`, `get`, `list`, `update`                                      |

### Unity Catalog

| Subgroup                | Commands                                    |
| ----------------------- | ------------------------------------------- |
| `metastore-assignments` | `create`, `delete`, `get`, `list`, `update` |
| `metastores`            | `create`, `delete`, `get`, `list`, `update` |
| `storage-credentials`   | `create`, `delete`, `get`, `list`, `update` |

### Security

| Subgroup               | Commands                                               |
| ---------------------- | ------------------------------------------------------ |
| `ip-access-lists`      | `create`, `delete`, `get`, `list`, `replace`, `update` |
| `network-connectivity` | 8 commands for network configuration                   |
| `settings`             | `csp-enablement-account`, `esm-enablement-account`     |

### Provisioning

| Subgroup          | Commands                |
| ----------------- | ----------------------- |
| `credentials`     | Credential config CRUD  |
| `encryption-keys` | Encryption key CRUD     |
| `networks`        | Network config CRUD     |
| `private-access`  | Private access settings |
| `storage`         | Storage config CRUD     |
| `vpc-endpoints`   | VPC endpoint management |
| `workspaces`      | Workspace CRUD          |

### Billing

| Subgroup           | Commands                   |
| ------------------ | -------------------------- |
| `billable-usage`   | Usage download             |
| `budget-policy`    | Budget policy management   |
| `budgets`          | Budget CRUD                |
| `log-delivery`     | Log delivery configuration |
| `usage-dashboards` | Usage dashboard management |

### OAuth and federation

| Subgroup                              | Commands                      |
| ------------------------------------- | ----------------------------- |
| `custom-app-integration`              | Custom OAuth app registration |
| `federation-policy`                   | Account federation policies   |
| `o-auth-published-apps`               | Published OAuth app listing   |
| `published-app-integration`           | Published app integration     |
| `service-principal-federation-policy` | SP federation policies        |
| `service-principal-secrets`           | SP secret management          |
