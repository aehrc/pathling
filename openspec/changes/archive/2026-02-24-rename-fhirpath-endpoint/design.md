## Context

The fhirpath-lab-api currently serves its evaluation endpoint at `POST /$fhirpath-r4`. This path sits at the application root and embeds a FHIR version suffix. The goal is to move it to `POST /fhir/$fhirpath` to follow standard FHIR API conventions.

The health check endpoint at `GET /healthcheck` remains unchanged.

## Goals / Non-goals

**Goals:**

- Rename the route from `/$fhirpath-r4` to `/fhir/$fhirpath`.
- Update all tests, documentation, and specs to reflect the new path.

**Non-goals:**

- No changes to request/response format, parameters, or evaluation logic.
- No changes to deployment infrastructure, Docker configuration, or CI/CD.
- No backwards-compatibility shim or redirect from the old path.

## Decisions

**Single route rename, no redirect.** The old path will simply stop working. A redirect or dual-path approach adds complexity for no benefit — the only known consumer (FHIRPath Lab frontend) will be updated in tandem.

**Health check path unchanged.** The `/healthcheck` endpoint does not need to move under `/fhir` as it is not a FHIR operation.

## Risks / Trade-offs

**Breaking change for existing clients** → Acceptable because the only known consumer is the FHIRPath Lab frontend, which will be updated at the same time. If other consumers exist, they will need to update their URLs.
