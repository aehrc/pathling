## 1. Library API — single-resource FHIRPath evaluation

- [x] 1.1 Add Java method to library-api module for evaluating a FHIRPath
      expression against a single FHIR resource JSON string, returning typed result
      values, AST JSON, and inferred return type
- [x] 1.2 Implement context expression support: evaluate the context expression
      first, then evaluate the main expression once per context item
- [x] 1.3 Implement environment variable support: accept named variables and
      make them available via `%variable` syntax during evaluation
- [x] 1.4 Implement trace capture: collect output from `trace()` function calls
      and include in results
- [x] 1.5 Write unit tests for the library API evaluation method covering
      primitive results, complex types, empty results, context expressions,
      variables, and error cases
- [x] 1.6 Rebuild library-runtime to bundle the new library API changes

## 2. Python bindings

- [x] 2.1 Add Python wrapper function in `lib/python` that calls the Java
      evaluation method via Py4J and returns a Python data structure
- [x] 2.2 Write unit tests for the Python bindings

## 3. Python HTTP server

- [x] 3.1 Create `fhirpath-lab-api/` directory with Python project structure
      (pyproject.toml, uv config, Flask application)
- [x] 3.2 Implement input parameter extraction: parse FHIR Parameters request
      body and extract expression, resource, context, and variables
- [x] 3.3 Implement `POST /$fhirpath-r4` endpoint: call the Python evaluation
      function and construct the FHIR Parameters response
- [x] 3.4 Implement output parameter construction: parameters part (evaluator,
      expression, resource, context, variables, parseDebugTree,
      expectedReturnType) and result parts with typed values
- [x] 3.5 Implement complex type serialisation using the json-value extension
      for types that cannot be represented as FHIR Parameters value types
- [x] 3.6 Implement trace output in result parts
- [x] 3.7 Implement context-scoped results: one result part per context item
- [x] 3.8 Implement CORS support with default FHIRPath Lab origins and
      configurable additional origins via environment variable
- [x] 3.9 Implement `GET /healthcheck` endpoint
- [x] 3.10 Implement error handling returning FHIR OperationOutcome resources
      for all error responses (400, 500)
- [x] 3.11 Write unit tests for request parsing, response construction, CORS,
      health check, and error handling
- [x] 3.12 Write integration test exercising the full request/response cycle

## 4. Docker image

- [x] 4.1 Create Dockerfile for the Python server
- [x] 4.2 Verify the image builds and the server starts and responds to
      requests

## 5. Helm chart

- [x] 5.1 Create Helm chart at `deployment/fhirpath-lab-api/chart/` with
      Chart.yaml, values.yaml, values.schema.json, and README.md
- [x] 5.2 Create deployment template with image, resources, probes
      (startup/liveness/readiness against `/healthcheck`), environment variables,
      and scheduling controls
- [x] 5.3 Create service template with configurable type and port
- [x] 5.4 Validate chart with `helm lint` and dry-run install

## 6. CI/CD

- [x] 6.1 Create GitHub Actions workflow for building and publishing the Docker
      image on changes to `fhirpath-lab-api/` or its deployment files
- [x] 6.2 Add manual trigger to the workflow for on-demand image builds

## 7. Documentation

- [x] 7.1 Write README for `fhirpath-lab-api/` covering running locally,
      configuration, and example requests/responses
- [x] 7.2 Write deployment documentation in the Helm chart README
