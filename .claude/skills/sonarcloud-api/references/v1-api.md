# SonarCloud Web API v1

Base URL: `https://sonarcloud.io/api/`

All endpoints require bearer token authentication. Interactive documentation available at `https://sonarcloud.io/web_api`.

## Table of contents

- [Issues API](#issues-api)
- [Measures API](#measures-api)
- [Components API](#components-api)
- [Quality Gates API](#quality-gates-api)
- [Rules API](#rules-api)
- [Sources API](#sources-api)
- [Project Analyses API](#project-analyses-api)
- [Webhooks API](#webhooks-api)
- [Permissions API](#permissions-api)
- [User Tokens API](#user-tokens-api)

---

## Issues API

### Search issues

```
GET /api/issues/search
```

| Parameter     | Type    | Description                                 |
| ------------- | ------- | ------------------------------------------- |
| componentKeys | string  | Comma-separated project keys                |
| types         | string  | BUG, VULNERABILITY, CODE_SMELL              |
| severities    | string  | INFO, MINOR, MAJOR, CRITICAL, BLOCKER       |
| statuses      | string  | OPEN, CONFIRMED, REOPENED, RESOLVED, CLOSED |
| resolutions   | string  | FALSE-POSITIVE, WONTFIX, FIXED, REMOVED     |
| assigned      | boolean | Filter to assigned issues                   |
| assignees     | string  | Comma-separated assignee logins             |
| createdAfter  | string  | ISO 8601 date                               |
| createdBefore | string  | ISO 8601 date                               |
| p             | integer | Page number                                 |
| ps            | integer | Page size (max 500)                         |
| facets        | string  | Facets to compute (e.g., severities,types)  |

Example:

```bash
curl -X GET "https://sonarcloud.io/api/issues/search?componentKeys=my_project&types=BUG&severities=CRITICAL,BLOCKER&ps=100" \
  -H "Authorization: Bearer YOUR_TOKEN"
```

Response:

```json
{
    "total": 42,
    "p": 1,
    "ps": 100,
    "issues": [
        {
            "key": "issue-uuid",
            "rule": "java:S1068",
            "severity": "MAJOR",
            "component": "my_project:src/Main.java",
            "project": "my_project",
            "line": 42,
            "message": "Remove this unused private field.",
            "type": "CODE_SMELL",
            "status": "OPEN"
        }
    ]
}
```

### Assign issue

```
POST /api/issues/assign
```

| Parameter | Type   | Description                           |
| --------- | ------ | ------------------------------------- |
| issue     | string | Issue key (required)                  |
| assignee  | string | Login of assignee (empty to unassign) |

### Add comment

```
POST /api/issues/add_comment
```

| Parameter | Type   | Description             |
| --------- | ------ | ----------------------- |
| issue     | string | Issue key (required)    |
| text      | string | Comment text (required) |

### Change issue status

```
POST /api/issues/do_transition
```

| Parameter  | Type   | Description                                                 |
| ---------- | ------ | ----------------------------------------------------------- |
| issue      | string | Issue key (required)                                        |
| transition | string | confirm, unconfirm, reopen, resolve, falsepositive, wontfix |

### Bulk change

```
POST /api/issues/bulk_change
```

| Parameter     | Type   | Description                |
| ------------- | ------ | -------------------------- |
| issues        | string | Comma-separated issue keys |
| do_transition | string | Transition to apply        |
| assign        | string | Assignee login             |
| add_tags      | string | Tags to add                |
| remove_tags   | string | Tags to remove             |
| comment       | string | Comment to add             |

---

## Measures API

### Get component measures

```
GET /api/measures/component
```

| Parameter        | Type   | Description                            |
| ---------------- | ------ | -------------------------------------- |
| component        | string | Component key (required)               |
| metricKeys       | string | Comma-separated metric keys (required) |
| additionalFields | string | metrics, periods                       |

Example:

```bash
curl -X GET "https://sonarcloud.io/api/measures/component?component=my_project&metricKeys=ncloc,coverage,bugs,vulnerabilities,code_smells" \
  -H "Authorization: Bearer YOUR_TOKEN"
```

Response:

```json
{
    "component": {
        "key": "my_project",
        "name": "My Project",
        "qualifier": "TRK",
        "measures": [
            { "metric": "ncloc", "value": "15234" },
            { "metric": "coverage", "value": "78.5" },
            { "metric": "bugs", "value": "3" }
        ]
    }
}
```

### Get component tree measures

```
GET /api/measures/component_tree
```

| Parameter  | Type    | Description                            |
| ---------- | ------- | -------------------------------------- |
| component  | string  | Component key (required)               |
| metricKeys | string  | Comma-separated metric keys (required) |
| strategy   | string  | children, leaves, all                  |
| qualifiers | string  | FIL, DIR, UTS, TRK                     |
| s          | string  | Sort field (metric, name, path)        |
| asc        | boolean | Ascending sort                         |
| p          | integer | Page number                            |
| ps         | integer | Page size                              |

### Search measures history

```
GET /api/measures/search_history
```

| Parameter | Type    | Description                            |
| --------- | ------- | -------------------------------------- |
| component | string  | Component key (required)               |
| metrics   | string  | Comma-separated metric keys (required) |
| from      | string  | Start date (ISO 8601)                  |
| to        | string  | End date (ISO 8601)                    |
| p         | integer | Page number                            |
| ps        | integer | Page size                              |

---

## Components API

### Search components

```
GET /api/components/search
```

| Parameter    | Type    | Description        |
| ------------ | ------- | ------------------ |
| qualifiers   | string  | TRK, BRC, FIL, DIR |
| q            | string  | Search query       |
| organization | string  | Organisation key   |
| p            | integer | Page number        |
| ps           | integer | Page size          |

### Show component

```
GET /api/components/show
```

| Parameter | Type   | Description              |
| --------- | ------ | ------------------------ |
| component | string | Component key (required) |

### Get component tree

```
GET /api/components/tree
```

| Parameter  | Type    | Description              |
| ---------- | ------- | ------------------------ |
| component  | string  | Component key (required) |
| qualifiers | string  | FIL, DIR, UTS            |
| q          | string  | Search query             |
| strategy   | string  | children, leaves, all    |
| p          | integer | Page number              |
| ps         | integer | Page size                |

---

## Quality Gates API

### Get project status

```
GET /api/qualitygates/project_status
```

| Parameter   | Type   | Description     |
| ----------- | ------ | --------------- |
| projectKey  | string | Project key     |
| projectId   | string | Project ID      |
| branch      | string | Branch name     |
| pullRequest | string | Pull request ID |

Example:

```bash
curl -X GET "https://sonarcloud.io/api/qualitygates/project_status?projectKey=my_project" \
  -H "Authorization: Bearer YOUR_TOKEN"
```

Response:

```json
{
    "projectStatus": {
        "status": "OK",
        "conditions": [
            {
                "status": "OK",
                "metricKey": "new_coverage",
                "comparator": "LT",
                "errorThreshold": "80",
                "actualValue": "85.3"
            }
        ]
    }
}
```

### List quality gates

```
GET /api/qualitygates/list
```

| Parameter    | Type   | Description      |
| ------------ | ------ | ---------------- |
| organization | string | Organisation key |

### Get gate details

```
GET /api/qualitygates/show
```

| Parameter    | Type   | Description      |
| ------------ | ------ | ---------------- |
| id           | string | Gate ID          |
| name         | string | Gate name        |
| organization | string | Organisation key |

---

## Rules API

### Search rules

```
GET /api/rules/search
```

| Parameter    | Type    | Description                                      |
| ------------ | ------- | ------------------------------------------------ |
| languages    | string  | Comma-separated language keys                    |
| repositories | string  | Comma-separated repository keys                  |
| severities   | string  | INFO, MINOR, MAJOR, CRITICAL, BLOCKER            |
| statuses     | string  | BETA, DEPRECATED, READY, REMOVED                 |
| types        | string  | CODE_SMELL, BUG, VULNERABILITY, SECURITY_HOTSPOT |
| q            | string  | Search query                                     |
| qprofile     | string  | Quality profile key                              |
| activation   | boolean | Filter by activation in profile                  |
| p            | integer | Page number                                      |
| ps           | integer | Page size                                        |
| facets       | string  | Facets to compute                                |

### Show rule

```
GET /api/rules/show
```

| Parameter | Type    | Description         |
| --------- | ------- | ------------------- |
| key       | string  | Rule key (required) |
| actives   | boolean | Include activations |

---

## Sources API

### Show source

```
GET /api/sources/raw
```

| Parameter | Type   | Description         |
| --------- | ------ | ------------------- |
| key       | string | File key (required) |

### Show source with issues

```
GET /api/sources/lines
```

| Parameter | Type    | Description         |
| --------- | ------- | ------------------- |
| key       | string  | File key (required) |
| from      | integer | Start line          |
| to        | integer | End line            |

### Show SCM blame

```
GET /api/sources/scm
```

| Parameter       | Type    | Description                  |
| --------------- | ------- | ---------------------------- |
| key             | string  | File key (required)          |
| from            | integer | Start line                   |
| to              | integer | End line                     |
| commits_by_line | boolean | Include commit info per line |

---

## Project Analyses API

### Search analyses

```
GET /api/project_analyses/search
```

| Parameter | Type    | Description            |
| --------- | ------- | ---------------------- |
| project   | string  | Project key (required) |
| branch    | string  | Branch name            |
| category  | string  | VERSION, OTHER         |
| from      | string  | Start date             |
| to        | string  | End date               |
| p         | integer | Page number            |
| ps        | integer | Page size              |

---

## Webhooks API

### List webhooks

```
GET /api/webhooks/list
```

| Parameter    | Type   | Description      |
| ------------ | ------ | ---------------- |
| organization | string | Organisation key |
| project      | string | Project key      |

### Create webhook

```
POST /api/webhooks/create
```

| Parameter    | Type   | Description             |
| ------------ | ------ | ----------------------- |
| name         | string | Webhook name (required) |
| url          | string | Target URL (required)   |
| organization | string | Organisation key        |
| project      | string | Project key             |
| secret       | string | Secret for signature    |

### Delete webhook

```
POST /api/webhooks/delete
```

| Parameter | Type   | Description            |
| --------- | ------ | ---------------------- |
| webhook   | string | Webhook key (required) |

---

## Permissions API

### Add user permission

```
POST /api/permissions/add_user
```

| Parameter    | Type   | Description               |
| ------------ | ------ | ------------------------- |
| login        | string | User login (required)     |
| permission   | string | Permission key (required) |
| projectKey   | string | Project key               |
| organization | string | Organisation key          |

Permissions: `admin`, `codeviewer`, `issueadmin`, `securityhotspotadmin`, `scan`, `user`

### Remove user permission

```
POST /api/permissions/remove_user
```

Same parameters as add_user.

### Add group permission

```
POST /api/permissions/add_group
```

| Parameter    | Type   | Description               |
| ------------ | ------ | ------------------------- |
| groupName    | string | Group name (required)     |
| permission   | string | Permission key (required) |
| projectKey   | string | Project key               |
| organization | string | Organisation key          |

---

## User Tokens API

### Generate token

```
POST /api/user_tokens/generate
```

| Parameter      | Type   | Description                                               |
| -------------- | ------ | --------------------------------------------------------- |
| name           | string | Token name (required)                                     |
| login          | string | User login (admin only)                                   |
| type           | string | USER_TOKEN, GLOBAL_ANALYSIS_TOKEN, PROJECT_ANALYSIS_TOKEN |
| projectKey     | string | For project tokens                                        |
| expirationDate | string | Expiration date (ISO 8601)                                |

### Revoke token

```
POST /api/user_tokens/revoke
```

| Parameter | Type   | Description             |
| --------- | ------ | ----------------------- |
| name      | string | Token name (required)   |
| login     | string | User login (admin only) |

### Search tokens

```
GET /api/user_tokens/search
```

| Parameter | Type   | Description           |
| --------- | ------ | --------------------- |
| login     | string | User login (required) |
