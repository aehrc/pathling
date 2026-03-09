## MODIFIED Requirements

### Requirement: FHIRPath evaluation configuration object

The FHIRPath engine SHALL support a `FhirpathConfiguration` object that holds
configurable parameters for FHIRPath expression evaluation. The configuration
SHALL be accessible to FHIRPath functions through the `EvaluationContext`.

The configuration SHALL include `maxUnboundTraversalDepth` (integer), which
controls the maximum depth for same-type recursive traversal in `repeat()` and
`repeatAll()`. Cross-type traversals do not consume depth budget. The default
value SHALL be 10. The minimum valid value SHALL be 1.

When no explicit configuration is provided, the evaluation context SHALL use a
default configuration with `maxUnboundTraversalDepth` set to 10.

#### Scenario: Default configuration

- **WHEN** no `FhirpathConfiguration` is explicitly provided to the evaluation
  context
- **THEN** the evaluation context SHALL use a default configuration with
  `maxUnboundTraversalDepth` equal to 10

#### Scenario: Custom configuration

- **WHEN** a `FhirpathConfiguration` with `maxUnboundTraversalDepth` set to 5 is
  provided to the evaluation context
- **THEN** the evaluation context SHALL use that configuration and same-type
  recursive traversal in `repeat()` and `repeatAll()` SHALL be limited to
  depth 5

## ADDED Requirements

### Requirement: QueryConfiguration maxUnboundTraversalDepth minimum is 1

The `QueryConfiguration.maxUnboundTraversalDepth` SHALL have a minimum valid
value of 1, matching the constraint on `FhirpathConfiguration`. A value of 0
SHALL be rejected by validation.

#### Scenario: Zero value rejected

- **WHEN** `QueryConfiguration` is built with `maxUnboundTraversalDepth` set
  to 0
- **THEN** a validation error SHALL be raised

#### Scenario: Value of 1 accepted

- **WHEN** `QueryConfiguration` is built with `maxUnboundTraversalDepth` set
  to 1
- **THEN** validation SHALL pass and the configuration SHALL be accepted

### Requirement: QueryConfiguration maxUnboundTraversalDepth flows to FHIRPath evaluation

When FHIRPath expressions are evaluated through the library API (via
`SingleResourceEvaluator`), the `maxUnboundTraversalDepth` from
`QueryConfiguration` SHALL be used to configure the `FhirpathConfiguration`
provided to the `EvaluationContext`. This ensures that `repeat()` and
`repeatAll()` functions respect the user-configured depth.

#### Scenario: Custom depth respected by repeat function

- **WHEN** `QueryConfiguration` is built with `maxUnboundTraversalDepth` set
  to 20
- **AND** a FHIRPath expression using `repeat()` or `repeatAll()` is evaluated
  through the library API
- **THEN** the recursive traversal depth limit SHALL be 20

#### Scenario: Default depth used when no QueryConfiguration specified

- **WHEN** no `QueryConfiguration` is explicitly provided
- **AND** a FHIRPath expression using `repeat()` or `repeatAll()` is evaluated
  through the library API
- **THEN** the recursive traversal depth limit SHALL be the default value of 10

### Requirement: SingleResourceEvaluator accepts FhirpathConfiguration

The `SingleResourceEvaluator` SHALL accept a `FhirpathConfiguration` and pass
it to the `FhirEvaluationContext` during expression evaluation. The
`SingleResourceEvaluatorBuilder` SHALL provide a `withConfiguration()` method
for setting the configuration.

When no configuration is explicitly set, `FhirpathConfiguration.DEFAULT` SHALL
be used.

#### Scenario: Evaluator with custom configuration

- **WHEN** a `SingleResourceEvaluator` is built with a `FhirpathConfiguration`
  having `maxUnboundTraversalDepth` set to 5
- **AND** a FHIRPath expression using `repeat()` is evaluated
- **THEN** the evaluation context SHALL have `maxUnboundTraversalDepth` equal
  to 5

#### Scenario: Evaluator with default configuration

- **WHEN** a `SingleResourceEvaluator` is built without specifying a
  `FhirpathConfiguration`
- **THEN** the evaluation context SHALL use `FhirpathConfiguration.DEFAULT`

### Requirement: ViewDefinition repeat clause errors on non-Extension depth exhaustion

The ViewDefinition `repeat` clause SHALL error at runtime when same-type
recursive traversal exhausts the configured `maxUnboundTraversalDepth` for
non-Extension FHIR types. For Extension types, depth exhaustion SHALL silently
stop and return results collected up to that point.

#### Scenario: Non-Extension depth exhaustion raises error

- **WHEN** a ViewDefinition `repeat` clause traverses a non-Extension type
  (e.g., `item` on Questionnaire) and the recursion reaches the configured
  `maxUnboundTraversalDepth`
- **THEN** a runtime error SHALL be raised indicating that the recursive
  traversal exceeded the maximum depth

#### Scenario: Extension depth exhaustion silently stops

- **WHEN** a ViewDefinition `repeat` clause traverses an Extension type and
  the recursion reaches the configured `maxUnboundTraversalDepth`
- **THEN** the clause SHALL silently stop and return all results collected up
  to that depth without raising an error

#### Scenario: Depth not exhausted produces normal results

- **WHEN** a ViewDefinition `repeat` clause traverses a type that terminates
  naturally before reaching `maxUnboundTraversalDepth`
- **THEN** all results from all nesting levels SHALL be returned normally
