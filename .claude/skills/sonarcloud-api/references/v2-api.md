# SonarCloud Web API v2

Base URL: `https://api.sonarcloud.io/api/v2/`

All endpoints require bearer token authentication.

## Table of contents

- [Projects API](#projects-api)
- [Quality Gates API](#quality-gates-api)
- [Analysis API](#analysis-api)
- [Organisations API](#organisations-api)
- [Authentication Domain API](#authentication-domain-api)
- [Enterprises, Reports, Portfolios API](#enterprises-reports-portfolios-api)
- [Users and Roles API](#users-and-roles-api)
- [SCA API](#sca-api)
- [AICA API](#aica-api)
- [Audit Logs API](#audit-logs-api)
- [Software Quality Reports API](#software-quality-reports-api)

---

## Projects API

| Method | Endpoint         | Description                           |
| ------ | ---------------- | ------------------------------------- |
| GET    | `/projects`      | Retrieve projects for an organisation |
| GET    | `/projects/{id}` | Get project details by ID             |
| GET    | `/project-tags`  | Search project tags                   |

### GET /projects

Query parameters:

| Parameter    | Type    | Description                 |
| ------------ | ------- | --------------------------- |
| organization | string  | Organisation key (required) |
| q            | string  | Search query                |
| p            | integer | Page number                 |
| ps           | integer | Page size (max 500)         |

Response:

```json
{
    "projects": [
        {
            "key": "my_project",
            "name": "My Project",
            "qualifier": "TRK",
            "visibility": "public",
            "lastAnalysisDate": "2024-01-15T10:30:00+0000"
        }
    ],
    "paging": {
        "pageIndex": 1,
        "pageSize": 100,
        "total": 42
    }
}
```

---

## Quality Gates API

| Method | Endpoint                                   | Description                 |
| ------ | ------------------------------------------ | --------------------------- |
| GET    | `/quality-gates`                           | Search quality gates        |
| POST   | `/quality-gates`                           | Create quality gate         |
| GET    | `/quality-gates/{id}`                      | Get quality gate details    |
| DELETE | `/quality-gates/{id}`                      | Delete quality gate         |
| PATCH  | `/quality-gates/{id}`                      | Update quality gate         |
| GET    | `/project-associations`                    | Search project associations |
| POST   | `/project-associations`                    | Associate project with gate |
| DELETE | `/project-associations/{id}`               | Remove association          |
| GET    | `/conditions`                              | Get gate conditions         |
| POST   | `/conditions`                              | Add condition               |
| DELETE | `/conditions/{id}`                         | Delete condition            |
| PATCH  | `/conditions/{id}`                         | Update condition            |
| GET    | `/quality-gates/defaults/{organizationId}` | Get default gate            |
| PATCH  | `/quality-gates/defaults/{organizationId}` | Set default gate            |

### POST /quality-gates

Request body:

```json
{
    "name": "My Quality Gate",
    "organizationId": "org-uuid"
}
```

### POST /conditions

Request body:

```json
{
    "qualityGateId": "gate-uuid",
    "metric": "new_coverage",
    "op": "LT",
    "error": "80"
}
```

Operators: `LT` (less than), `GT` (greater than)

---

## Analysis API

| Method | Endpoint                      | Description                         |
| ------ | ----------------------------- | ----------------------------------- |
| GET    | `/engine`                     | Get latest scanner engine version   |
| GET    | `/jres`                       | Get JRE version for OS/architecture |
| POST   | `/active-rules`               | Retrieve active rules for analysis  |
| POST   | `/analysis-statuses`          | Get branch/PR analysis status       |
| POST   | `/analyses`                   | Create analysis                     |
| POST   | `/sensor-cache/prepare-read`  | Get presigned cache download URL    |
| POST   | `/sensor-cache/prepare-write` | Get presigned cache upload URL      |

### POST /active-rules

Request body:

```json
{
    "projectKey": "my_project",
    "branchName": "main"
}
```

### POST /analysis-statuses

Request body:

```json
{
    "projectKey": "my_project",
    "branchName": "main"
}
```

---

## Organisations API

| Method | Endpoint                          | Description                |
| ------ | --------------------------------- | -------------------------- |
| GET    | `/organizations`                  | List organisations         |
| GET    | `/organizations/{organizationId}` | Get organisation details   |
| PATCH  | `/organizations/{organizationId}` | Update organisation        |
| GET    | `/organizations/{id}/users`       | Search organisation users  |
| GET    | `/organizations/{id}/groups`      | Search organisation groups |

### GET /organizations

Query parameters:

| Parameter | Type    | Description                               |
| --------- | ------- | ----------------------------------------- |
| member    | boolean | Filter to organisations user is member of |
| p         | integer | Page number                               |
| ps        | integer | Page size                                 |

---

## Authentication Domain API

| Method | Endpoint                  | Description                      |
| ------ | ------------------------- | -------------------------------- |
| GET    | `/token-definitions`      | List organisation-scoped tokens  |
| POST   | `/token-definitions`      | Create organisation-scoped token |
| DELETE | `/token-definitions/{id}` | Delete organisation-scoped token |
| GET    | `/ip-allowlists`          | Retrieve IP allowlists           |
| PATCH  | `/ip-allowlists/{id}`     | Update IP allowlists             |

### POST /token-definitions

Request body:

```json
{
    "name": "CI Token",
    "organizationId": "org-uuid",
    "expirationDate": "2025-12-31"
}
```

---

## Enterprises, Reports, Portfolios API

| Method | Endpoint                               | Description                     |
| ------ | -------------------------------------- | ------------------------------- |
| GET    | `/enterprises`                         | List enterprises                |
| POST   | `/enterprises`                         | Create enterprise               |
| GET    | `/enterprises/{enterpriseId}`          | Get enterprise details          |
| DELETE | `/enterprises/{enterpriseId}`          | Delete enterprise               |
| PATCH  | `/enterprises/{enterpriseId}`          | Update enterprise               |
| GET    | `/enterprise-organizations`            | List organisation associations  |
| POST   | `/enterprise-organizations`            | Add organisation to enterprise  |
| DELETE | `/enterprise-organizations/{id}`       | Remove organisation             |
| GET    | `/portfolio-reports`                   | Download portfolio report (ZIP) |
| GET    | `/project-reports`                     | Download project report (PDF)   |
| GET    | `/project-report-subscriptions`        | Check subscription status       |
| POST   | `/project-report-subscriptions`        | Subscribe to reports            |
| DELETE | `/project-report-subscriptions`        | Unsubscribe from reports        |
| GET    | `/regulatory-reports`                  | Download regulatory report      |
| GET    | `/portfolios`                          | List enterprise portfolios      |
| POST   | `/portfolios`                          | Create portfolio                |
| GET    | `/portfolios/{portfolioId}`            | Get portfolio details           |
| DELETE | `/portfolios/{portfolioId}`            | Delete portfolio                |
| PATCH  | `/portfolios/{portfolioId}`            | Modify portfolio                |
| GET    | `/portfolio-measures`                  | Retrieve portfolio metrics      |
| GET    | `/portfolio-projects`                  | Search portfolio projects       |
| GET    | `/portfolio-computed-project-measures` | Get computed project metrics    |

---

## Users and Roles API

| Method | Endpoint | Description         |
| ------ | -------- | ------------------- |
| GET    | `/users` | Search users        |
| GET    | `/roles` | Get available roles |
| POST   | `/roles` | Add role            |

### GET /users

Query parameters:

| Parameter      | Type    | Description       |
| -------------- | ------- | ----------------- |
| q              | string  | Search query      |
| organizationId | string  | Organisation UUID |
| p              | integer | Page number       |
| ps             | integer | Page size         |

---

## SCA API

Software Composition Analysis for dependency scanning.

| Method | Endpoint            | Description                     |
| ------ | ------------------- | ------------------------------- |
| GET    | `/sca/risk-reports` | Retrieve dependency risk report |
| GET    | `/sca/sbom-reports` | Generate SBOM report            |

---

## AICA API

AI Code Assurance for AI-generated code detection.

| Method | Endpoint              | Description                    |
| ------ | --------------------- | ------------------------------ |
| GET    | `/projects`           | Check AI code detection status |
| PATCH  | `/projects/{id}`      | Update AI code assurance flag  |
| GET    | `/ai-code-assurances` | Get AI assurance status        |

---

## Audit Logs API

| Method | Endpoint    | Description         |
| ------ | ----------- | ------------------- |
| GET    | `/download` | Download audit logs |

Query parameters:

| Parameter      | Type   | Description           |
| -------------- | ------ | --------------------- |
| organizationId | string | Organisation UUID     |
| from           | string | Start date (ISO 8601) |
| to             | string | End date (ISO 8601)   |

---

## Software Quality Reports API

| Method | Endpoint                                | Description                   |
| ------ | --------------------------------------- | ----------------------------- |
| GET    | `/security-reports`                     | Get security report           |
| GET    | `/download-security-reports`            | Download security report      |
| GET    | `/portfolio-security-reports`           | Get portfolio security report |
| GET    | `/portfolio-security-reports-breakdown` | Get report breakdown          |
