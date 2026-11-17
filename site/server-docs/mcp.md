---
sidebar_position: 5.5
description: Pathling can serve the Model Context Protocol, giving AI assistants such as Claude read-only access to its data.
---

# Model Context Protocol

Pathling can serve the [Model Context Protocol](https://modelcontextprotocol.io)
(MCP) at `/mcp`, which gives AI assistants such as Claude read-only access to
the data held by the server. The endpoint uses the stateless
[Streamable HTTP transport](https://modelcontextprotocol.io/specification/2025-06-18/basic/transports#streamable-http),
so any replica of the server can answer any request.

The endpoint is disabled by default. Enable it by setting
`pathling.mcp.enabled` to `true`. The other settings are described in
[Configuration](configuration#model-context-protocol).

## Tools

| Tool                  | Description                                                                                       | Equivalent operation                |
| --------------------- | ------------------------------------------------------------------------------------------------- | ----------------------------------- |
| `list_resource_types` | Lists the resource types for which the server holds data.                                         | -                                   |
| `read_resource`       | Reads a resource by type and id.                                                                  | [read](operations/crud)             |
| `search`              | Searches one resource type with FHIR search parameters, FHIRPath filters, or both.                | [search](operations/search)         |
| `run_view_definition` | Runs a [ViewDefinition](sql-on-fhir) supplied with the call, and returns its rows.                | [$sql-run](operations/sql-run)      |
| `run_sql_query`       | Runs a SQLQuery Library supplied with the call over ViewDefinitions, and returns its rows.        | [$sql-run](operations/sql-run)      |

Every tool is read-only. A tool is offered only when its equivalent operation
is enabled in [`pathling.operations`](configuration#operations).

The ViewDefinitions that a SQL query depends on can be supplied with the call
in the `views` argument, or stored on the server. The SQL can be given as plain
text in the `sql` argument instead of as Base64 data within the Library.

Results are limited so that they fit within the context of a language model:

- `search` returns 10 resources unless the call asks for more, and never more
  than `pathling.mcp.maxSearchCount` (default 100). The `total` of the returned
  Bundle counts every match.
- `run_view_definition` and `run_sql_query` return 100 rows unless the call
  asks for more, and never more than `pathling.mcp.maxRows` (default 1000). The
  result reports `truncated: true` when there were more rows.

A failure, such as an invalid FHIRPath expression or a missing authority, is
returned as a tool error with a message that the model can act on.

## Connecting a client

To add Pathling to Claude on the web, Claude Desktop or Claude mobile, go to
**Settings > Connectors**, choose **Add custom connector**, and enter the URL
of the MCP endpoint, e.g. `https://pathling.example.com/mcp`. A connector added
on the web or on Desktop is also available in Claude mobile. The connection is
made from Anthropic's servers rather than from the user's device, so the
endpoint must be reachable from the internet over HTTPS. See
[Get started with custom connectors](https://support.claude.com/en/articles/11175166-get-started-with-custom-connectors-using-remote-mcp).

To add it to Claude Code:

```bash
claude mcp add --transport http pathling https://pathling.example.com/mcp
```

## Authorization

When [authorization](authorization) is enabled, the MCP endpoint is an OAuth
2.0 protected resource, as described in the
[MCP authorization specification](https://modelcontextprotocol.io/specification/2025-06-18/basic/authorization).
An MCP client discovers the authorization server and signs the user in without
any configuration beyond the endpoint URL:

1. A request without a valid token is refused with a `401`, and a
   `WWW-Authenticate` header that points at the protected resource metadata,
   e.g.
   `Bearer resource_metadata="https://pathling.example.com/.well-known/oauth-protected-resource/mcp", scope="pathling:mcp"`.
2. The [protected resource metadata](https://www.rfc-editor.org/rfc/rfc9728),
   served without authentication, names the MCP endpoint as the `resource`, the
   MCP issuer as the authorization server, and the scopes configured in
   `pathling.mcp.auth.scopesSupported`. It is served at
   `/.well-known/oauth-protected-resource/mcp` and at
   `/.well-known/oauth-protected-resource`.
3. The client signs the user in at the authorization server with the
   authorization code grant and PKCE, naming the MCP endpoint as the
   [resource indicator](https://www.rfc-editor.org/rfc/rfc8707).

An access token is accepted at the MCP endpoint when:

- it is signed by, and its `iss` claim names, the MCP issuer
  (`pathling.mcp.auth.issuer`, or `pathling.auth.issuer` when that is not set);
  and
- its `aud` claim includes the URL of the MCP endpoint. A trailing slash is
  ignored. A token issued for the FHIR API is therefore refused.

The URL of the MCP endpoint is `pathling.mcp.resourceUrl` if it is set, or
otherwise the URL that the request arrived at, reconstructed from the
`Forwarded` and `X-Forwarded-*` headers when they are present. Set
`pathling.mcp.resourceUrl` when a proxy in front of Pathling does not send
these headers.

Each tool requires the same [authorities](authorization#authorities) as its
equivalent operation:

| Tool                  | Required authorities                                                                     |
| --------------------- | ---------------------------------------------------------------------------------------- |
| `list_resource_types` | None beyond a valid token.                                                               |
| `read_resource`       | `pathling:read-resource`, and `pathling:read:[resource type]`.                           |
| `search`              | `pathling:search`, and `pathling:read:[resource type]`.                                  |
| `run_view_definition` | `pathling:sql-run`, and read on the projected resource type.                             |
| `run_sql_query`       | `pathling:sql-run`, read on every projected resource type, and read on stored artefacts. |

The authorization server must support PKCE with `S256`, and must have a client
registered for each MCP client. Claude can register itself with
[dynamic client registration](https://www.rfc-editor.org/rfc/rfc7591) or a
[client ID metadata document](https://modelcontextprotocol.io/specification/2025-11-25/basic/authorization#client-id-metadata-documents)
where the authorization server supports them. Otherwise, register a client with
the redirect URI `https://claude.ai/api/mcp/auth_callback`, and enter its client
ID and secret in the **Advanced settings** of the custom connector. See
[Authentication for connectors](https://claude.com/docs/connectors/building/authentication)
for Claude's requirements of an authorization server.

### Using Signet

[Signet](https://github.com/aehrc/signet) can issue the tokens for the MCP
endpoint. It needs an OAuth / OpenID Connect endpoint for this, rather than a
SMART on FHIR endpoint: MCP clients name the resource they want a token for
with the `resource` parameter, whereas a SMART endpoint requires the `aud`
parameter. The SMART endpoint that protects the FHIR API can stay as it is.

1. Create an endpoint of kind **OAuth / OpenID Connect**, e.g. with the slug
   `pathling-mcp`, whose audience is the URL of the MCP endpoint, e.g.
   `https://pathling.example.com/mcp`.
2. Publish a policy for it that starts from the **OAuth / OpenID Connect
   baseline**, and adds a grant rule for an MCP scope and a claim rule that
   turns that scope into Pathling authorities:

   ```json
   {
     "scopeGrants": [
       {
         "id": "grant-pathling-mcp",
         "description": "Query the data in Pathling, read-only.",
         "match": "pathling:mcp",
         "allow": true
       }
     ],
     "claimRules": [
       {
         "id": "claim-pathling-mcp-authorities",
         "description": "Pathling's authorities for read-only queries through MCP.",
         "when": { "scope": "pathling:mcp" },
         "emit": {
           "authorities": [
             "pathling:read",
             "pathling:read-resource",
             "pathling:search",
             "pathling:sql-run"
           ]
         }
       }
     ]
   }
   ```

   These rules are added to the ones in the baseline. Narrow `pathling:read` to
   specific resource types, e.g. `pathling:read:Observation`, to limit what can
   be queried.
3. Register a confidential client for Claude with the redirect URI
   `https://claude.ai/api/mcp/auth_callback`, the `authorization_code` and
   `refresh_token` grant types, and the allowed scopes `pathling:mcp` and
   `offline_access`.
4. Configure Pathling with the endpoint as the MCP issuer:

   ```yaml
   pathling:
     auth:
       enabled: true
       issuer: https://signet.example.com/t/acme/e/pathling
       audience: https://pathling.example.com/fhir
     mcp:
       enabled: true
       resourceUrl: https://pathling.example.com/mcp
       auth:
         issuer: https://signet.example.com/t/acme/e/pathling-mcp
         scopesSupported:
           - pathling:mcp
   ```

5. Add the custom connector in Claude, and enter the client ID and secret from
   step 3 in its **Advanced settings**.
