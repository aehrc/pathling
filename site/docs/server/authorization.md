---
sidebar_position: 6
description: Pathling can perform the role of a resource server within the OpenID Connect framework.
---

# Authorization

Pathling can perform the role of a resource server within the
[OpenID Connect framework](https://openid.net/connect/).

When authorization is enabled through configuration, Pathling will refuse any
requests which are not accompanied by a valid
[bearer token](https://tools.ietf.org/html/rfc6750). The following requirements
must be met:

- Token is a [JSON Web Token](https://tools.ietf.org/html/rfc7519)
- Token contains an
  [audience claim](https://tools.ietf.org/html/rfc7519#section-4.1.3) that
  matches the configured value
- Token contains
  an [issuer claim](https://tools.ietf.org/html/rfc7519#section-4.1.1)
  that matches the configured value
- Issuer provides
  an [OpenID Connect Discovery endpoint](https://openid.net/specs/openid-connect-discovery-1_0.html)
  that provides information about how to validate the token, including a link to
  a
  [JSON Web Key Set](https://tools.ietf.org/html/rfc7517) containing the signing
  key. This endpoint needs to be accessible to the Pathling server.

## Authorities

Pathling supports a set of authorities that control access to resources and
operations. Authorities must be provided within the `authorities` claim within
the JWT bearer token provided with each request.

```mermaid
%%{init: {'themeVariables': { 'edgeLabelBackground':'#ffffff'}, 'flowchart': {'nodeSpacing': 30, 'rankSpacing': 40}}}%%
graph TB
    operation["pathling:[operation]"]
    pathling[pathling]

    subgraph Resources[" "]
        direction LR
        read-resource["pathling:read:[resource type]"]
        read[pathling:read]
        write[pathling:write]
        write-resource["pathling:write:[resource type]"]
    end

    pathling -->|includes| operation
    pathling -->|includes| read
    pathling -->|includes| write
    read -->|includes| read-resource
    write -->|includes| write-resource

    style pathling fill:#f9d5e5,stroke:#333
    style operation fill:#d5e5f9,stroke:#333
    style read fill:#f9e5d5,stroke:#333
    style write fill:#f9e5d5,stroke:#333
    style read-resource fill:#e5f9d5,stroke:#333
    style write-resource fill:#e5f9d5,stroke:#333
```

| Authority                        | Description                                                                     |
| -------------------------------- | ------------------------------------------------------------------------------- |
| `pathling`                       | Provides access to all operations and resources, implies all other authorities. |
| `pathling:read`                  | Provides read access to all resource types.                                     |
| `pathling:read:[resource type]`  | Provides read access to only a specified resource type.                         |
| `pathling:write`                 | Provides write access to all resource types.                                    |
| `pathling:write:[resource type]` | Provides write access to only a specified resource type.                        |
| `pathling:import`                | Provides access to the import operation.                                        |
| `pathling:import-pnp`            | Provides access to the ping and pull import operation.                          |
| `pathling:search`                | Provides access to the search operation.                                        |
| `pathling:update`                | Provides access to the update operation.                                        |
| `pathling:batch`                 | Provides access to the batch operation.                                         |
| `pathling:bulk-submit`           | Provides access to the bulk submit operation.                                   |
| `pathling:export`                | Provides access to the export operation.                                        |

In order to enable access to an operation, an operation authority (e.g.
`pathling:search`) must be provided along with a `read` or `write` authority
(e.g. `pathling:read:Patient`).

Where expressions within a request reference multiple different resource types
(e.g. through resource references), authority for read access to all those
resources must be present within the token.

The import and batch operations require `write` authority for all resource types
that are referenced within the request.
