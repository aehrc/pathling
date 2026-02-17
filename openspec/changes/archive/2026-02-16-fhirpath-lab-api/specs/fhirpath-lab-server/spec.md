## ADDED Requirements

### Requirement: POST /$fhirpath-r4 endpoint

The server SHALL expose a `POST /$fhirpath-r4` endpoint that accepts a FHIR
Parameters resource as the request body (Content-Type: `application/json`) and
returns a FHIR Parameters resource as the response.

#### Scenario: Successful expression evaluation

- **WHEN** a POST request is sent to `/$fhirpath-r4` with a valid Parameters
  resource containing an `expression` and a `resource` parameter
- **THEN** the server returns HTTP 200 with a FHIR Parameters resource
  containing the evaluation results

#### Scenario: Missing required expression parameter

- **WHEN** a POST request is sent without an `expression` parameter
- **THEN** the server returns HTTP 400 with a FHIR OperationOutcome describing
  the missing parameter

#### Scenario: Missing required resource parameter

- **WHEN** a POST request is sent without a `resource` parameter
- **THEN** the server returns HTTP 400 with a FHIR OperationOutcome describing
  the missing parameter

#### Scenario: Invalid JSON body

- **WHEN** a POST request is sent with malformed JSON
- **THEN** the server returns HTTP 400 with a FHIR OperationOutcome

### Requirement: Input parameter extraction

The server SHALL extract the following parameters from the input FHIR Parameters
resource:

- `expression` (required, string): The FHIRPath expression to evaluate.
- `resource` (required, resource): The FHIR resource to evaluate against.
- `context` (optional, string): A context expression for iterative evaluation.
- `variables` (optional, multi-part): Named variables for the expression.

#### Scenario: Expression and resource extraction

- **WHEN** a Parameters resource contains `expression` (valueString) and
  `resource` (resource) parameters
- **THEN** the server extracts both values and passes them to the evaluation API

#### Scenario: Context expression extraction

- **WHEN** a Parameters resource includes a `context` parameter with
  valueString `name`
- **THEN** the server evaluates the main expression once per context item

#### Scenario: Variable extraction

- **WHEN** a Parameters resource includes a `variables` parameter with parts
  containing name/value pairs
- **THEN** the server passes those variables to the evaluation API

#### Scenario: Resource via JSON extension

- **WHEN** a Parameters resource includes a `resource` parameter with the
  `http://fhir.forms-lab.com/StructureDefinition/json-value` extension
  containing a JSON string
- **THEN** the server parses the JSON string and uses it as the input resource

### Requirement: Output parameter construction

The server SHALL construct a FHIR Parameters response containing:

- A `parameters` part echoing input metadata and engine information.
- Zero or more `result` parts containing evaluation results.

#### Scenario: Parameters part content

- **WHEN** an expression is evaluated successfully
- **THEN** the response contains a `parameters` part with sub-parts:
  `evaluator` (Pathling version string), `expression`, `resource`, and
  optionally `context` and `variables`

#### Scenario: Parse debug tree output

- **WHEN** an expression is evaluated successfully
- **THEN** the `parameters` part includes a `parseDebugTree` sub-part containing
  a JSON string representation of the expression AST

#### Scenario: Expected return type output

- **WHEN** an expression is evaluated successfully
- **THEN** the `parameters` part includes an `expectedReturnType` sub-part with
  the statically inferred return type

### Requirement: Result formatting

Each evaluation result SHALL be returned as a `result` part in the output
Parameters resource. The part name of each value within a result SHALL be the
FHIR data type name (e.g., `string`, `integer`, `boolean`, `HumanName`).

#### Scenario: Primitive result formatting

- **WHEN** the expression returns primitive values (e.g., strings)
- **THEN** each value is a part with name equal to the type and the appropriate
  `value[x]` property (e.g., `valueString`)

#### Scenario: Complex type result formatting

- **WHEN** the expression returns complex types (e.g., HumanName) that cannot
  be represented as FHIR Parameters value types
- **THEN** each value uses the
  `http://fhir.forms-lab.com/StructureDefinition/json-value` extension with
  the value serialised as JSON

#### Scenario: Empty result

- **WHEN** the expression evaluates to an empty collection
- **THEN** the response contains no `result` parts (only the `parameters` part)

#### Scenario: Context-scoped results

- **WHEN** a context expression is provided that yields multiple items
- **THEN** the response contains one `result` part per context item, each with
  a `valueString` indicating the context path

### Requirement: Trace output support

The server SHALL capture trace output from FHIRPath `trace()` function calls
and include them in the result parts.

#### Scenario: Trace function output

- **WHEN** the expression contains a `trace('label')` call
- **THEN** the corresponding result part includes a `trace` sub-part with the
  trace label and traced values

### Requirement: CORS support

The server SHALL support CORS for requests from FHIRPath Lab domains.

#### Scenario: Preflight CORS request

- **WHEN** an OPTIONS request is sent with `Origin: https://fhirpath-lab.com`
- **THEN** the server responds with appropriate CORS headers allowing the
  request

#### Scenario: Allowed origins

- **WHEN** a request originates from `https://fhirpath-lab.com`,
  `https://dev.fhirpath-lab.com`, or `http://localhost:3000`
- **THEN** the server includes CORS headers permitting the request

#### Scenario: Configurable CORS origins

- **WHEN** additional CORS origins are configured via environment variable
- **THEN** the server allows requests from those origins in addition to the
  defaults

### Requirement: Health check endpoint

The server SHALL expose a `GET /healthcheck` endpoint for liveness and
readiness probes.

#### Scenario: Healthy server

- **WHEN** a GET request is sent to `/healthcheck` and the server is ready
- **THEN** the server returns HTTP 200

### Requirement: Error handling

The server SHALL return FHIR OperationOutcome resources for all error responses.

#### Scenario: Expression evaluation error

- **WHEN** a valid expression fails during evaluation (e.g., type mismatch)
- **THEN** the server returns HTTP 500 with a FHIR OperationOutcome containing
  the error details

#### Scenario: Unexpected server error

- **WHEN** an unexpected exception occurs during request processing
- **THEN** the server returns HTTP 500 with a FHIR OperationOutcome with a
  generic error message

### Requirement: Evaluator identification

The server SHALL identify itself in the response using the evaluator name
"Pathling" and the current library version.

#### Scenario: Evaluator string format

- **WHEN** any successful evaluation is performed
- **THEN** the `evaluator` part in the response parameters contains a string in
  the format `Pathling <version> (R4)` (e.g., `Pathling 8.1.0 (R4)`)
