## Context

Pathling's FHIRPath engine is built on Apache Spark, translating FHIRPath
expressions into Spark SQL Column expressions for batch evaluation over large
datasets. There is no public API for evaluating an expression against a single
resource and returning materialised values.

FHIRPath Lab expects a simple HTTP API: POST a FHIR Parameters resource
containing an expression and a resource, get back a Parameters resource
containing the results. The server must handle one request at a time with low
latency.

The existing Python library (`lib/python`) wraps the Java library API via Py4J
through PySpark. The `PathlingContext` class exposes dataset-level operations
(encode, decode, query, terminology functions) but nothing for single-resource
evaluation.

## Goals / Non-goals

**Goals:**

- Expose Pathling's FHIRPath engine for single-resource expression evaluation
  through a new library API method.
- Implement a Python HTTP server conforming to the FHIRPath Lab server API
  specification.
- Provide a Helm chart for deploying to any Kubernetes cluster.
- Keep the server application thin — business logic lives in the library API.

**Non-goals:**

- Supporting non-R4 FHIR versions (R4 only for now).
- Implementing the `debug-trace` output (step-by-step execution tracing). This
  requires deep integration with the expression evaluator internals and is
  deferred.
- Implementing `validate` mode (expression validation without execution).
- Supporting `terminologyserver` parameter (Pathling uses its own terminology
  configuration).
- Running Pathling in cluster mode — the server uses Spark local mode only.

## Decisions

### 1. Single-resource evaluation via one-row Spark Dataset

**Decision**: Evaluate FHIRPath against a single resource by encoding it into a
one-row Spark Dataset, applying the existing Column-based evaluator, and
collecting the results.

**Alternatives considered**:

- _Build a non-Spark evaluator_: Would require reimplementing the entire
  expression evaluation logic. High effort, high divergence risk.
- _Use HAPI FHIRPath evaluator_: Would not exercise Pathling's engine (which is
  the whole point).

**Rationale**: Reuses 100% of Pathling's existing FHIRPath implementation.
Performance is acceptable for interactive use — Spark local mode with a single
row is fast enough. The Spark session is created once at startup and reused.

### 2. New `evaluateFhirPath` method on library API

**Decision**: Add a new Java method to the library API module that accepts a
FHIR resource string, a FHIRPath expression, optional context expression, and
optional variables. Returns a structured result containing typed values, the
parsed AST, and the inferred return type.

**Rationale**: Keeps the HTTP server layer thin. The library API method is
reusable beyond the FHIRPath Lab use case. Python wraps it via Py4J.

### 3. Python server using Flask

**Decision**: Use Flask for the HTTP server.

**Alternatives considered**:

- _FastAPI_: More modern, but adds uvicorn/Starlette/Pydantic dependencies.
  Async is unnecessary for this synchronous, single-threaded workload.
- _Plain WSGI_: Too low-level for CORS and JSON handling.

**Rationale**: Flask is lightweight, well-known, and sufficient. The server has
a single endpoint. Flask-CORS handles CORS. The Python library already depends
on PySpark which dwarfs any Flask dependency.

### 4. Module location and versioning

**Decision**: New directory `fhirpath-lab-api/` at the repository root,
versioned independently from both the core libraries and the server.

**Rationale**: Follows the pattern established by `server/` and `ui/`. The
FHIRPath Lab API has different release cadence and deployment concerns from the
core libraries.

### 5. Helm chart at `deployment/fhirpath-lab-api/chart/`

**Decision**: Follow the pattern of existing deployment charts (Pathling server,
cache, SQL on FHIR). Single deployment, single service, configurable probes,
environment-based configuration.

**Rationale**: Consistency with existing charts. The server is a single-pod
stateless application — no persistence, no RBAC, no multi-port services needed.

### 6. Result serialisation

**Decision**: Map Pathling result types to FHIR Parameters parts following the
FHIRPath Lab specification. Primitive types use the appropriate `value[x]`
property. Complex types (e.g., HumanName, Address) that cannot be represented
as FHIR Parameters parts use the
`http://fhir.forms-lab.com/StructureDefinition/json-value` extension with JSON
serialisation.

**Rationale**: Matches the behaviour of existing FHIRPath Lab server
implementations (JS, .NET, Java).

## Risks / Trade-offs

- **Spark overhead for single-resource evaluation**: Even in local mode, Spark
  has non-trivial startup time (~5-10 seconds) and per-query overhead. →
  Mitigation: Initialise Spark session at server startup; accept ~100-500ms
  per-request latency which is acceptable for interactive use.

- **Memory footprint**: Spark local mode consumes significant memory (~1-2 GB).
  → Mitigation: Configure JVM heap appropriately in Helm chart; document
  minimum resource requirements.

- **FHIRPath feature coverage**: Some FHIRPath Lab features (debug trace,
  validate mode) are not initially supported. → Mitigation: Document
  limitations; implement incrementally as the FHIRPath Lab specification allows
  optional features.

- **Python library rebuild requirement**: Changes to the Java library API
  require rebuilding `library-runtime` and the Python library. → Mitigation:
  Document the build dependency chain; CI workflow handles the full build.

## Open questions

- Should the AST output format match the FHIRPath Lab specification exactly, or
  is an approximation acceptable? The Pathling parser produces a different AST
  structure than the specification describes.
- What level of variable support is feasible in the initial implementation?
  Pathling's FHIRPath engine has limited variable support beyond `%resource`
  and `%context`.
