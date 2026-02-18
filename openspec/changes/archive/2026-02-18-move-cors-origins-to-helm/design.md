## Context

The FHIRPath Lab API server currently hardcodes three CORS origins in
`DEFAULT_CORS_ORIGINS` within `app.py`. The `CORS_ALLOWED_ORIGINS` environment
variable adds origins on top of these defaults. This means the application
itself decides which origins are allowed, and the Helm chart can only add more.

## Goals / Non-goals

**Goals:**

- Remove all hardcoded CORS origins from the Python application code.
- Make the application read CORS origins solely from `CORS_ALLOWED_ORIGINS`.
- Provide sensible defaults in the Helm chart `values.yaml`.

**Non-goals:**

- Changing the CORS behaviour (e.g. credentials, headers, methods).
- Changing the deployment process or CI workflow.

## Decisions

### CORS origins read from environment variable only

The application will read `CORS_ALLOWED_ORIGINS` as a comma-separated list and
pass it directly to Flask-CORS. If the variable is empty or unset, no origins
are allowed (the application has no opinion about which origins are correct).

**Rationale:** This keeps the application generic. Deployment-specific
configuration belongs in deployment manifests.

### Default origins set in Helm values

The Helm chart `values.yaml` will set `CORS_ALLOWED_ORIGINS` to
`https://fhirpath-lab.azurewebsites.net,http://localhost:3000`. This is
overridable per-environment via values files.

**Rationale:** The Helm chart is the natural place for environment-specific
defaults. Operators can override in their values files.

## Risks / Trade-offs

- **Running without Helm**: If someone runs the Docker container directly
  without setting `CORS_ALLOWED_ORIGINS`, CORS will block all cross-origin
  requests. This is acceptable — operators should configure their environment.
  The README will document this.
