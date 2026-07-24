---
sidebar_position: 11
description: The jobs operation lists the asynchronous jobs held in the server's in-memory registry, so a client can monitor and cancel their work without retaining every kick-off response.
---

# Jobs

This operation lists the asynchronous jobs currently held in the server's
in-memory registry, such as those created by
[import](import), [export](export), [SQL on FHIR export](sql-export) and
[ViewDefinition export](view-export). It lets a client enumerate the jobs it
owns, with their current status and progress, without having to retain the
poll URL returned when each job was started.

The operation is only available when asynchronous processing is enabled
(`pathling.async.enabled`); otherwise it is not registered and requests fail as
an unknown operation.

```
GET [FHIR endpoint]/$jobs
```

## Response

The response is a `Parameters` resource with one repeating `job` parameter per
job, ordered newest first. Each `job` parameter has the following parts:

| Part        | Type      | Description                                                                             |
| ----------- | --------- | --------------------------------------------------------------------------------------- |
| `id`        | `string`  | The unique identifier of the job.                                                       |
| `operation` | `code`    | The name of the operation that initiated the job (e.g. `export`).                       |
| `status`    | `code`    | The derived status: `in-progress`, `completed`, `failed` or `cancelled`.                |
| `progress`  | `integer` | The progress percentage (0-100), present only for in-progress jobs with known progress. |
| `startTime` | `instant` | The time at which the job was started.                                                  |
| `url`       | `uri`     | The absolute URL of the job's status-polling and cancellation endpoint.                 |

```json
{
    "resourceType": "Parameters",
    "parameter": [
        {
            "name": "job",
            "part": [
                {
                    "name": "id",
                    "valueString": "7f3a9c1e-2b4d-4b8a-9c0d-1e2f3a4b5c6d"
                },
                { "name": "operation", "valueCode": "export" },
                { "name": "status", "valueCode": "in-progress" },
                { "name": "progress", "valueInteger": 62 },
                {
                    "name": "startTime",
                    "valueInstant": "2026-07-24T00:42:11.000Z"
                },
                {
                    "name": "url",
                    "valueUri": "https://server.example.org/fhir/$job?id=7f3a9c1e-2b4d-4b8a-9c0d-1e2f3a4b5c6d"
                }
            ]
        }
    ]
}
```

The `url` of each job can be used to poll its status (`GET`) or to cancel it
(`DELETE`), exactly as with the `Content-Location` returned when the job was
started.

## Ownership

When [authorisation](../authorization) is enabled, the caller must hold the
`pathling:jobs` authority, and the response contains only the jobs whose owner
matches the caller's token subject. A caller whose token has no subject claim
receives an empty list. When authorisation is disabled, all jobs in the
registry are returned.

## Persistence

The job registry is held in memory and is not persisted, so the list is empty
after a server restart. Cancelled and deleted jobs are removed from the
registry and no longer appear in the list.
