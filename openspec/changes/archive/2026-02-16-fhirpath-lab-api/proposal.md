## Why

[FHIRPath Lab](https://github.com/brianpos/fhirpath-lab) is a web-based tool
for experimenting with FHIRPath expressions across different engines. It
requires server-side APIs for engines that cannot run in the browser. Pathling
has a mature FHIRPath implementation but no way to expose it to FHIRPath Lab,
because the library API only supports dataset-level evaluation — there is no
public API for evaluating an expression against a single FHIR resource and
returning materialised results.

## What changes

- New Python HTTP server application (`fhirpath-lab-api/`) implementing the
  [FHIRPath Lab server API specification](https://github.com/brianpos/fhirpath-lab/blob/master/server-api.md).
- New library API capability to evaluate FHIRPath expressions against individual
  FHIR resources (not just Spark datasets), returning materialised result values
  with type information.
- `POST /$fhirpath-r4` endpoint accepting and returning FHIR Parameters
  resources, with CORS support for FHIRPath Lab domains.
- Support for context expressions, environment variables, expression parsing
  metadata (AST, return type), and trace output.
- Error handling returning FHIR OperationOutcome resources.
- Helm chart for generic Kubernetes deployment at
  `deployment/fhirpath-lab-api/chart/`.
- GitHub Actions workflow for building and publishing the Docker image.

## Capabilities

### New capabilities

- `fhirpath-evaluation-api`: Library API extension for evaluating FHIRPath
  expressions against individual FHIR resources, returning materialised results
  with type metadata.
- `fhirpath-lab-server`: Python HTTP server implementing the FHIRPath Lab
  server API specification, including the `/$fhirpath-r4` endpoint, CORS,
  and FHIR Parameters request/response handling.
- `fhirpath-lab-deployment`: Helm chart for deploying the FHIRPath Lab API
  server to any Kubernetes cluster, and a CI/CD workflow for building and
  publishing the Docker image.

### Modified capabilities

No existing capabilities are modified.

## Impact

- **New module**: `fhirpath-lab-api/` at repository root, versioned
  independently (like `server` and `ui`).
- **Library API**: New public methods on `PathlingContext` or a new class for
  single-resource FHIRPath evaluation. Changes to `library-api` module.
- **Python library**: New Python bindings wrapping the library API extension
  (changes to `lib/python`).
- **Dependencies**: Python server depends on Pathling Python library, Flask or
  FastAPI, and CORS middleware.
- **Deployment**: New Helm chart under `deployment/fhirpath-lab-api/chart/`
  (generic, not tied to any specific cluster), new GitHub Actions workflow for
  image build and publish.
- **No breaking changes** to existing APIs or modules.
