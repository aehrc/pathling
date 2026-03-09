## Requirements

### Requirement: FHIRPath evaluation configuration object

The FHIRPath engine SHALL support a `FhirpathConfiguration` object that holds
configurable parameters for FHIRPath expression evaluation. The configuration
SHALL be accessible to FHIRPath functions through the `EvaluationContext`.

The configuration SHALL include `maxUnboundTraversalDepth` (integer), which controls
the maximum depth for same-type recursive traversal in `repeat()` and
`repeatAll()`. Cross-type traversals do not consume depth budget. The default
value SHALL be 10.

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

### Requirement: EvaluationContext provides configuration access

The `EvaluationContext` interface SHALL provide a method to retrieve the
`FhirpathConfiguration`. Implementations that do not explicitly set a
configuration SHALL return the default configuration.

#### Scenario: Configuration accessible from evaluation context

- **WHEN** a FHIRPath function accesses the evaluation context during evaluation
- **THEN** the `FhirpathConfiguration` SHALL be available with the configured
  `maxUnboundTraversalDepth` value

### Requirement: FHIRPath functions can receive EvaluationContext

The `FunctionParameterResolver` SHALL support injecting the `EvaluationContext`
into `@FhirPathFunction`-annotated static methods. When a method parameter is
typed as `EvaluationContext`, the resolver SHALL inject the current evaluation
context without consuming an argument from the FHIRPath argument list.

#### Scenario: EvaluationContext injection

- **WHEN** a `@FhirPathFunction` method declares an `EvaluationContext`
  parameter
- **THEN** the resolver SHALL inject the current evaluation context and the
  function SHALL receive it alongside the normal input and arguments
