---
sidebar_position: 7
description: The admin UI provides a web interface for managing Pathling Server.
---

# Admin UI

Pathling Server includes an admin UI that provides a web interface for common
server operations.

## Features

The admin UI provides access to:

- **Import** - Load FHIR data from NDJSON files or URLs
- **Export** - Bulk export data at system, patient, or group level
- **Resources** - Browse and search resources using FHIRPath expressions
- **View definitions** - Manage and execute SQL on FHIR view definitions

## Accessing the UI

When running the Pathling Server, the admin UI is available at `/admin/`. For
example, if your server is running at `https://pathling.example.com`, the admin
UI would be at `https://pathling.example.com/admin/`.

## Authentication

When [authorization](./authorization) is enabled (`pathling.auth.enabled:
true`), the admin UI requires authentication via SMART on FHIR. Users must log
in before accessing protected features.

### Client ID configuration

The admin UI needs an OAuth client ID to initiate authentication flows. The
client ID is resolved using the following precedence:

1. `admin_ui_client_id` from the server's SMART configuration
   (`/.well-known/smart-configuration`)
2. `VITE_CLIENT_ID` environment variable (set at build time)
3. Default value: `pathling-admin-ui`

To configure the client ID on the server:

```yaml
pathling:
    adminUi:
        clientId: your-client-id
```

### Token authorities

When authorization is enabled, tokens issued by your authorization server must
include an `authorities` claim containing the appropriate Pathling authorities.
For full admin UI functionality, the token should include:

```json
{
    "authorities": ["pathling"]
}
```

The `pathling` authority grants access to all operations and resources. See
[Authorization](./authorization#authorities) for more granular authority
options.

Your authorization server must be configured to include this claim in issued
tokens. The exact configuration depends on your provider:

- **Auth0**: Use a post-login Action to add custom claims
- **Keycloak**: Configure a client scope with a "User Attribute" or "Hardcoded
  claim" mapper

## Setting up an OAuth client

To use the admin UI with authentication, you need to register an OAuth client
with your authorization server.

### Required settings

| Setting        | Value                                |
| -------------- | ------------------------------------ |
| Client type    | Public (no client secret)            |
| PKCE           | Required (S256)                      |
| Redirect URI   | `https://your-server/admin/callback` |
| Allowed scopes | `openid profile user/*.read`         |

### Auth0

1. Create a new application in the Auth0 dashboard
2. Select "Single Page Application" as the application type
3. Configure the following settings:
    - **Allowed Callback URLs**: `https://your-server/admin/callback`
    - **Allowed Logout URLs**: `https://your-server/admin/`
    - **Allowed Web Origins**: `https://your-server`
4. Copy the Client ID and configure it in Pathling:

```yaml
pathling:
    auth:
        enabled: true
        issuer: https://your-tenant.au.auth0.com/
        audience: https://your-server/fhir
    adminUi:
        clientId: your-auth0-client-id
```

### Keycloak

1. Create a new client in your Keycloak realm
2. Configure the following settings:
    - **Client Protocol**: openid-connect
    - **Access Type**: public
    - **Standard Flow Enabled**: ON
    - **Valid Redirect URIs**: `https://your-server/admin/callback`
    - **Web Origins**: `https://your-server`
3. Copy the Client ID and configure it in Pathling:

```yaml
pathling:
    auth:
        enabled: true
        issuer: https://keycloak.example.com/realms/your-realm
        audience: https://your-server/fhir
    adminUi:
        clientId: your-keycloak-client-id
```

## Standalone deployment

The admin UI can be deployed separately from the Pathling Server, which is
useful for scenarios where you want to:

- Host the UI on a CDN or static file server
- Use a different domain for the UI
- Deploy multiple UI instances pointing to the same server

### Building the UI

To build the UI for standalone deployment:

```bash
cd ui
VITE_FHIR_BASE_URL=https://your-server/fhir VITE_CLIENT_ID=your-client-id bun run build
```

This produces static files in the `dist/` directory.

### Environment variables

| Variable             | Description                     | Default             |
| -------------------- | ------------------------------- | ------------------- |
| `VITE_FHIR_BASE_URL` | The base URL of the FHIR server | `/fhir`             |
| `VITE_CLIENT_ID`     | The OAuth client ID             | `pathling-admin-ui` |

### Docker deployment

Create a Dockerfile for nginx:

```dockerfile
FROM nginx:alpine
COPY dist/ /usr/share/nginx/html/admin/
COPY nginx.conf /etc/nginx/conf.d/default.conf
```

Create an `nginx.conf` for SPA routing:

```nginx
server {
    listen 80;
    root /usr/share/nginx/html;
    index index.html;

    location /admin/ {
        try_files $uri $uri/ /admin/index.html;
    }
}
```

Build and run:

```bash
docker build -t pathling-admin-ui .
docker run -p 8080:80 pathling-admin-ui
```

### Static hosting

The built files can be deployed to any static hosting service:

- **AWS S3** with CloudFront
- **Cloudflare Pages**
- **Netlify**
- **Vercel**

Configure your hosting service to:

1. Serve files from the `dist/` directory
2. Route all requests under `/admin/` to `/admin/index.html` (SPA fallback)
3. Set appropriate CORS headers if hosting on a different domain

### CORS configuration

When deploying the UI on a different domain, configure CORS on the Pathling
Server:

```yaml
pathling:
    cors:
        allowedOriginPatterns:
            - "https://your-ui-domain.com"
```
