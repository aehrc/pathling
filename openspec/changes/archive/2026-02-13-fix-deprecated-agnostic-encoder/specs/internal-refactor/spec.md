## ADDED Requirements

### Requirement: Encoder does not use deprecated Spark APIs

The FHIR encoder SHALL NOT depend on `AgnosticExpressionPathEncoder` or any
other deprecated Spark API.

#### Scenario: Encoder compiles without deprecation warnings

- **WHEN** the encoders module is compiled
- **THEN** no deprecation warnings related to `AgnosticExpressionPathEncoder`
  are emitted

#### Scenario: Encoding produces identical results

- **WHEN** a FHIR resource is encoded using the new encoder implementation
- **THEN** the resulting Spark DataFrame schema and data are identical to the
  previous implementation
