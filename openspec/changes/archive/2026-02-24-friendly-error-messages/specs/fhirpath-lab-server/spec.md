## MODIFIED Requirements

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
