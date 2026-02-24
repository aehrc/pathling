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

| Variable               | Description                             | Default |
| ---------------------- | --------------------------------------- | ------- |
| `PORT`                 | HTTP port to listen on                  | `8080`  |
| `CORS_ALLOWED_ORIGINS` | Comma-separated list of allowed origins | (none)  |

CORS is disabled unless `CORS_ALLOWED_ORIGINS` is set. The Helm chart provides
default origins for production deployments.

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
