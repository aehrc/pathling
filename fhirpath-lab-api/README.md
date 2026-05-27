# FHIRPath Lab API

A FHIRPath evaluation server backed by the Pathling FHIRPath engine. This
server implements the API used by
[FHIRPath Lab](https://fhirpath-lab.com) to evaluate FHIRPath expressions
against FHIR resources.

## Prerequisites

- Python 3.9+
- [uv](https://docs.astral.sh/uv/) (Python package manager)
- Java 21 (required by Pathling's Spark runtime)

## Running locally

Install dependencies and start the server:

```bash
uv sync
uv run python -m fhirpath_lab_api
```

The server starts on port 8080 by default. Override with the `PORT` environment
variable:

```bash
PORT=9000 uv run python -m fhirpath_lab_api
```

## Running tests

```bash
uv sync --extra dev
uv run pytest -v
```

## Configuration

The server is configured via environment variables:

| Variable               | Description                                                        | Default |
| ---------------------- | ------------------------------------------------------------------ | ------- |
| `PORT`                 | HTTP port to listen on                                             | `8080`  |
| `CORS_ALLOWED_ORIGINS` | Comma-separated list of allowed origins                            | (none)  |
| `MAX_CONTEXT_ELEMENTS` | Upper bound on the number of context elements in a grouped request | `100`   |

CORS is disabled unless `CORS_ALLOWED_ORIGINS` is set. The Helm chart provides
default origins for production deployments.

Requests whose `context` expression yields more than `MAX_CONTEXT_ELEMENTS`
elements are rejected with HTTP 400 (`OperationOutcome` issue code
`too-costly`). Each element triggers an additional round-trip through the
Pathling engine, so the cap protects against denial-of-service via large
contexts such as `Bundle.entry`.

## API

### `GET /healthcheck`

Returns server health status.

```json
{ "status": "ok" }
```

### `POST /fhir/$fhirpath`

Evaluates a FHIRPath expression against a FHIR resource.

Request body (FHIR Parameters):

```json
{
    "resourceType": "Parameters",
    "parameter": [
        { "name": "expression", "valueString": "name.family" },
        {
            "name": "resource",
            "resource": {
                "resourceType": "Patient",
                "id": "example",
                "name": [{ "family": "Smith", "given": ["John"] }]
            }
        }
    ]
}
```

If the expression is empty or contains only whitespace, the endpoint returns a
successful response with an empty collection (no result parts) without invoking
the FHIRPath engine.

Optional parameters:

- `context` (valueString): A FHIRPath context expression.
- `variables` (parts): Named variables available as `%varName` in the
  expression. Each part has a `name` and `valueString`.

Response (FHIR Parameters):

```json
{
    "resourceType": "Parameters",
    "parameter": [
        {
            "name": "parameters",
            "part": [
                { "name": "evaluator", "valueString": "Pathling 9.3.1 (R4)" },
                { "name": "expression", "valueString": "name.family" },
                { "name": "expectedReturnType", "valueString": "string" }
            ]
        },
        {
            "name": "result",
            "part": [{ "name": "string", "valueString": "Smith" }]
        }
    ]
}
```

#### Trace entries

Each invocation of the FHIRPath `trace()` function inside the expression
contributes an additional part inside the `result` part. The trace part is
named `trace`, its `valueString` is the trace label, and its `part` list
contains the typed values that flowed through the trace. Trace parts appear
after the typed result values within the same `result` part.

For an expression such as `name.given.trace('all-given').first()`:

```json
{
    "name": "result",
    "part": [
        { "name": "string", "valueString": "John" },
        {
            "name": "trace",
            "valueString": "all-given",
            "part": [
                { "name": "string", "valueString": "John" },
                { "name": "string", "valueString": "Jim" }
            ]
        }
    ]
}
```

#### Grouped responses (with `context`)

When a `context` parameter is supplied, the main expression is evaluated once
per element of the context. The response then contains one `result` part per
context element, in order. Each result part carries a `valueString` label of
the form `"<context>[i]"` identifying the element it relates to, and its
`part` list contains the typed values (and any `trace` parts) produced by
that iteration.

For an expression of `given.first()` with a context of `name` against a
patient with two names:

```json
{
    "resourceType": "Parameters",
    "parameter": [
        {
            "name": "parameters",
            "part": [
                { "name": "evaluator", "valueString": "Pathling 9.6.0 (R4)" },
                { "name": "expression", "valueString": "given.first()" },
                { "name": "context", "valueString": "name" },
                { "name": "expectedReturnType", "valueString": "string" }
            ]
        },
        {
            "name": "result",
            "valueString": "name[0]",
            "part": [{ "name": "string", "valueString": "Peter" }]
        },
        {
            "name": "result",
            "valueString": "name[1]",
            "part": [{ "name": "string", "valueString": "Jim" }]
        }
    ]
}
```

A context that yields zero elements produces a successful response with no
`result` parts. A context that yields more than `MAX_CONTEXT_ELEMENTS`
elements is rejected with HTTP 400 - see the configuration section above.

## Docker

Build the image:

```bash
docker build -t fhirpath-lab-api .
```

Run the container:

```bash
docker run -p 8080:8080 fhirpath-lab-api
```

## Deployment

A Helm chart is available at `deployment/fhirpath-lab-api/chart/`. See the
chart [README](../deployment/fhirpath-lab-api/chart/README.md) for deployment
instructions.
