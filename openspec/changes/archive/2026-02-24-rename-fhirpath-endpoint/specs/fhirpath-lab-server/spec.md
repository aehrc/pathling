## MODIFIED Requirements

### Requirement: POST /$fhirpath-r4 endpoint

The server SHALL expose a `POST /fhir/$fhirpath` endpoint that accepts a FHIR
Parameters resource as the request body (Content-Type: `application/json`) and
returns a FHIR Parameters resource as the response.

#### Scenario: Successful expression evaluation

- **WHEN** a POST request is sent to `/fhir/$fhirpath` with a valid Parameters
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

## RENAMED Requirements

### Requirement: POST /$fhirpath-r4 endpoint

- **FROM:** POST /$fhirpath-r4 endpoint
- **TO:** POST /fhir/$fhirpath endpoint
