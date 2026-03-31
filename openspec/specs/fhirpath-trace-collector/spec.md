### Requirement: TraceCollector interface for programmatic trace capture

The system SHALL provide a `TraceCollector` interface that accepts trace data
during FHIRPath expression evaluation. The interface SHALL accept a label
(String), a FHIR type (String), and a value (Object) for each traced element.
How the implementation stores, groups, or retrieves these entries is not
prescribed by the interface.

#### Scenario: Collector is called during trace evaluation

- **WHEN** an expression containing `trace('myLabel')` is evaluated with a
  `TraceCollector` attached to the evaluation context and the result is
  materialised
- **THEN** the collector's add method SHALL be called with the label `myLabel`,
  the FHIR type of the traced collection, and the traced value

### Requirement: List-backed collector implementation

The system SHALL provide a `ListTraceCollector` implementation of
`TraceCollector` backed by a plain list. This implementation is not serializable.
A `TraceCollectorProxy` SHALL wrap the list collector for use in Spark
expressions, delegating via a static registry keyed by UUID. The proxy SHALL be
serializable and SHALL implement `AutoCloseable` for registry cleanup.

#### Scenario: ListTraceCollector captures entries

- **WHEN** a `ListTraceCollector` is used during evaluation (via proxy)
- **THEN** all trace entries are captured and retrievable after materialisation

#### Scenario: Proxy survives Spark plan serialization

- **WHEN** a `TraceCollectorProxy` wrapping a `ListTraceCollector` is serialized
  during Spark closure cleaning
- **THEN** the proxy SHALL deserialize successfully and continue delegating to
  the original collector via the registry

### Requirement: Trace entries carry FHIR type metadata

Each trace entry SHALL include the FHIR type code of the traced collection
(e.g., `"HumanName"`, `"string"`, `"boolean"`), derived from the input
collection's type information at the point where `trace()` is invoked.

#### Scenario: Trace entry for a complex type

- **WHEN** `Patient.name.trace('names')` is evaluated with a collector
- **THEN** each trace entry SHALL have FHIR type `"HumanName"`

#### Scenario: Trace entry for a primitive type

- **WHEN** `Patient.active.trace('flag')` is evaluated with a collector
- **THEN** each trace entry SHALL have FHIR type `"boolean"`

### Requirement: Trace collector is optional on EvaluationContext

The `EvaluationContext` SHALL provide an optional `TraceCollector`. When no
collector is present, `trace()` SHALL still log via SLF4J but SHALL NOT attempt
to collect entries.

#### Scenario: No collector attached

- **WHEN** an expression containing `trace()` is evaluated without a collector
  on the context
- **THEN** the expression SHALL evaluate successfully with SLF4J logging only

#### Scenario: Collector attached

- **WHEN** an expression containing `trace()` is evaluated with a collector on
  the context
- **THEN** both SLF4J logging and collector capture SHALL occur

### Requirement: Trace values are sanitized

Trace values that are complex types (Spark `Row` objects) SHALL be sanitized
before being stored in the collector. Sanitization SHALL strip synthetic fields
and null-valued fields, consistent with the existing
`SingleInstanceEvaluator.sanitiseRow()` logic.

#### Scenario: Synthetic fields stripped from trace values

- **WHEN** tracing a complex type that contains synthetic fields (e.g., `_fid`)
- **THEN** the collected trace value SHALL NOT contain synthetic fields
