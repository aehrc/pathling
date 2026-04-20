## MODIFIED Requirements

### Requirement: Trace entries carry FHIR type metadata

Each trace entry SHALL include the FHIR type code of the expression being
logged. When no projection is provided, this is the FHIR type of the input
collection. When a projection is provided, this is the FHIR type of the
projected result (e.g., `"string"` for a `family` projection on a `HumanName`
input).

#### Scenario: Trace entry for a complex type

- **WHEN** `Patient.name.trace('names')` is evaluated with a collector
- **THEN** each trace entry SHALL have FHIR type `"HumanName"`

#### Scenario: Trace entry for a primitive type

- **WHEN** `Patient.active.trace('flag')` is evaluated with a collector
- **THEN** each trace entry SHALL have FHIR type `"boolean"`

#### Scenario: Trace entry type reflects projection

- **WHEN** `Patient.name.trace('fam', family)` is evaluated with a collector
- **THEN** each trace entry SHALL have FHIR type `"string"` (the projected
  type), not `"HumanName"` (the input type)
