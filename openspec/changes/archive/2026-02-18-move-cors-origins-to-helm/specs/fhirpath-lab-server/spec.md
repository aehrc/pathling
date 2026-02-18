## MODIFIED Requirements

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
