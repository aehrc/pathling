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

Complex type results SHALL be sanitised before serialisation to remove synthetic
fields that are not part of the FHIR specification. A field is synthetic if its
name starts with `_` or ends with `_scale`, consistent with the
`PruneSyntheticFields.isAnnotation` convention in the encoders module.

Complex type results SHALL also have null-valued fields stripped before
serialisation, so that the JSON representation only contains fields with
non-null values. This stripping SHALL be applied recursively to nested structs.

Both synthetic field removal and null-value stripping SHALL be performed in the
Java `SingleInstanceEvaluator.sanitiseRow()` method, so that the JSON string
produced by `rowToJson()` is already clean before crossing the Java/Python
boundary. The Python response construction layer SHALL treat complex type values
as opaque JSON strings and SHALL NOT perform additional null stripping.

This sanitisation SHALL be applied recursively to nested struct types within
complex type results.

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

#### Scenario: Complex type results exclude synthetic fields

- **WHEN** the expression returns a Quantity value
- **THEN** the JSON representation does not contain `value_scale`,
  `_value_canonicalized`, or `_code_canonicalized` fields

#### Scenario: Complex type results exclude \_fid field

- **WHEN** the expression returns any complex type
- **THEN** the JSON representation does not contain a `_fid` field

#### Scenario: Nested struct sanitisation

- **WHEN** the expression returns a complex type containing nested struct
  fields with synthetic fields
- **THEN** the synthetic fields are stripped from nested structs as well

#### Scenario: Complex type results exclude null-valued fields

- **WHEN** the expression returns a Quantity value with `id`, `comparator`, and
  other fields set to null
- **THEN** the JSON representation omits those null-valued fields entirely

#### Scenario: Nested null stripping

- **WHEN** the expression returns a complex type containing nested structs with
  null-valued fields
- **THEN** the null-valued fields are stripped from nested structs as well

#### Scenario: Empty result

- **WHEN** the expression evaluates to an empty collection
- **THEN** the response contains no `result` parts (only the `parameters` part)

#### Scenario: Context-scoped results

- **WHEN** a context expression is provided that yields multiple items
- **THEN** the response contains one `result` part per context item, each with
  a `valueString` indicating the context path

### Requirement: Variable support

The server SHALL pass custom variables from the input Parameters to the
FHIRPath evaluation engine so that expressions referencing `%varName` resolve
correctly.

#### Scenario: String variable

- **WHEN** a `variables` parameter contains a part with `name: "greeting"` and
  `valueString: "hello"`
- **AND** the expression is `%greeting`
- **THEN** the result contains a single string value `"hello"`

#### Scenario: Integer variable

- **WHEN** a `variables` parameter contains a part with `name: "count"` and
  `valueInteger: 42`
- **AND** the expression is `%count`
- **THEN** the result contains a single integer value `42`

#### Scenario: Boolean variable

- **WHEN** a `variables` parameter contains a part with `name: "flag"` and
  `valueBoolean: true`
- **AND** the expression is `%flag`
- **THEN** the result contains a single boolean value `true`

#### Scenario: Decimal variable

- **WHEN** a `variables` parameter contains a part with `name: "rate"` and
  `valueDecimal: 3.14`
- **AND** the expression is `%rate`
- **THEN** the result contains a single decimal value `3.14`

#### Scenario: No variables provided

- **WHEN** the request contains no `variables` parameter
- **THEN** evaluation proceeds with default environment variables only
  (`%context`, `%resource`, `%rootResource`)

### Requirement: Trace output support

The server SHALL capture trace output from FHIRPath `trace()` function calls
and include them in the result parts.

#### Scenario: Trace function output

- **WHEN** the expression contains a `trace('label')` call
- **THEN** the corresponding result part includes a `trace` sub-part with the
  trace label and traced values

### Requirement: CORS support

The server SHALL support CORS for cross-origin requests. Allowed origins SHALL
be read from the `CORS_ALLOWED_ORIGINS` environment variable as a
comma-separated list. The server SHALL NOT hardcode any default origins.

If `CORS_ALLOWED_ORIGINS` is empty or unset, no cross-origin requests SHALL be
permitted.

#### Scenario: Preflight CORS request

- **WHEN** an OPTIONS request is sent with an `Origin` header matching a value
  in `CORS_ALLOWED_ORIGINS`
- **THEN** the server responds with appropriate CORS headers allowing the
  request

#### Scenario: Allowed origins from environment

- **WHEN** `CORS_ALLOWED_ORIGINS` is set to
  `https://fhirpath-lab.azurewebsites.net,http://localhost:3000`
- **AND** a request originates from `https://fhirpath-lab.azurewebsites.net`
- **THEN** the server includes CORS headers permitting the request

#### Scenario: No origins configured

- **WHEN** `CORS_ALLOWED_ORIGINS` is empty or unset
- **AND** a cross-origin request is received
- **THEN** the server does not include CORS allow headers

#### Scenario: Multiple origins

- **WHEN** `CORS_ALLOWED_ORIGINS` contains multiple comma-separated origins
- **THEN** the server permits requests from any of the listed origins

### Requirement: Health check endpoint

The server SHALL expose a `GET /healthcheck` endpoint for liveness and
readiness probes.

#### Scenario: Healthy server

- **WHEN** a GET request is sent to `/healthcheck` and the server is ready
- **THEN** the server returns HTTP 200

### Requirement: Error handling

The server SHALL return FHIR OperationOutcome resources for all error responses.

For errors originating from the Java evaluation engine (Py4JJavaError), the
server SHALL extract a human-readable message from the Java exception and place
it in the OperationOutcome issue `details.text` field. The full exception string
including the stack trace SHALL be placed in the `diagnostics` field.

The human-readable message SHALL consist of the Java exception short class name
and its message (e.g. "UnsupportedFhirPathFeatureError: Unsupported function:
trace"). If the exception string cannot be parsed, the server SHALL fall back to
using the full exception string as the message.

For non-Py4J exceptions, the server SHALL use the exception message directly as
`details.text` with no `diagnostics` field.

#### Scenario: Py4J evaluation error

- **WHEN** a FHIRPath expression triggers a Java exception via Py4J
  (e.g. `UnsupportedFhirPathFeatureError`)
- **THEN** the server returns HTTP 500 with a FHIR OperationOutcome where
  `issue[0].details.text` contains a concise message such as
  "UnsupportedFhirPathFeatureError: Unsupported function: trace"
- **AND** `issue[0].diagnostics` contains the full exception string including
  the Java stack trace

#### Scenario: Py4J error with unparseable format

- **WHEN** a FHIRPath expression triggers a Java exception via Py4J whose
  string representation does not match the expected format
- **THEN** the server returns HTTP 500 with a FHIR OperationOutcome where
  `issue[0].details.text` contains the full exception string
- **AND** `issue[0].diagnostics` is not present

#### Scenario: Non-Py4J evaluation error

- **WHEN** a FHIRPath expression triggers a Python-level exception that is not
  a Py4JJavaError
- **THEN** the server returns HTTP 500 with a FHIR OperationOutcome where
  `issue[0].details.text` contains the exception message
- **AND** `issue[0].diagnostics` is not present

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

### Requirement: Production WSGI server

The server SHALL use Gunicorn as the production WSGI server when running inside
the Docker container. The container CMD SHALL invoke Gunicorn directly rather
than using Flask's built-in development server.

#### Scenario: Container starts with Gunicorn

- **WHEN** the Docker container starts with default configuration
- **THEN** the process running inside the container is Gunicorn serving the
  Flask application, and the werkzeug development server warning does not appear
  in the logs

#### Scenario: Configurable bind address

- **WHEN** the `PORT` environment variable is set to `9090`
- **THEN** Gunicorn binds to `0.0.0.0:9090`

#### Scenario: Default port

- **WHEN** the `PORT` environment variable is not set
- **THEN** Gunicorn binds to `0.0.0.0:8080`

#### Scenario: Configurable worker count

- **WHEN** the `WEB_WORKERS` environment variable is set to `2`
- **THEN** Gunicorn starts with 2 worker processes

#### Scenario: Default worker count

- **WHEN** the `WEB_WORKERS` environment variable is not set
- **THEN** Gunicorn starts with 1 worker process

### Requirement: Local development entry point

The `python -m fhirpath_lab_api` entry point SHALL remain functional for local
development, using Flask's built-in server.

#### Scenario: Local development server

- **WHEN** the application is started via `python -m fhirpath_lab_api`
- **THEN** the Flask development server starts on the configured port
