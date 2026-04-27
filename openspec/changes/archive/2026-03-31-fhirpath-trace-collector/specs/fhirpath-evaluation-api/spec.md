## MODIFIED Requirements

### Requirement: Evaluate FHIRPath expression against a single resource

The library API SHALL provide a method that accepts a FHIR resource (as a JSON
string), a FHIRPath expression string, an optional context expression string,
and optional variables, and returns a list of typed result values.

The method SHALL use the existing Pathling FHIRPath engine (Spark-based) to
evaluate the expression, encoding the resource into a one-row Dataset
internally.

The method SHALL support all FHIRPath functions implemented in the engine,
including scoped functions that accept projection expressions (e.g., `select()`,
`repeatAll()`).

When the expression is empty or contains only whitespace, the fhirpath-lab-api
SHALL return a successful response with an empty collection (zero result parts)
without invoking the FHIRPath engine.

The method SHALL create a `TraceCollector` before evaluation and attach it to
the evaluation context. After materialisation, collected trace entries SHALL be
included in the result alongside the typed values.

#### Scenario: Simple expression evaluation

- **WHEN** the method is called with a Patient JSON resource and the expression
  `name.family`
- **THEN** the method returns a list containing a single string result with the
  patient's family name

#### Scenario: Expression returning multiple values

- **WHEN** the method is called with a Patient JSON resource containing two
  names and the expression `name.given`
- **THEN** the method returns a list containing all given name strings

#### Scenario: Expression returning empty result

- **WHEN** the method is called with a Patient JSON resource and an expression
  that matches no elements (e.g., `deceased`)
- **THEN** the method returns an empty list

#### Scenario: Empty expression

- **WHEN** the fhirpath-lab-api receives a request with an expression that is an
  empty string or contains only whitespace
- **THEN** it returns a successful Parameters response with zero result parts and
  no type metadata, without invoking the FHIRPath engine

#### Scenario: Invalid expression

- **WHEN** the method is called with a syntactically invalid FHIRPath expression
- **THEN** the method throws an exception with a descriptive error message

#### Scenario: Context expression evaluation

- **WHEN** the method is called with a Patient JSON resource, context expression
  `name`, and expression `given.first()`
- **THEN** the method evaluates the main expression once for each result of the
  context expression and returns results grouped by context item

#### Scenario: Recursive function evaluation

- **WHEN** the method is called with a Questionnaire JSON resource and the
  expression `repeatAll(item).linkId`
- **THEN** the method returns a list containing `linkId` values from all
  recursively nested items

#### Scenario: Trace data included in evaluation result

- **WHEN** the method is called with an expression containing `trace('label')`
- **THEN** the result SHALL include a list of trace entries, each with the label,
  FHIR type, and typed values

#### Scenario: No trace calls produces empty trace list

- **WHEN** the method is called with an expression that does not use `trace()`
- **THEN** the result SHALL include an empty trace list

### Requirement: Python bindings for evaluation method

The Pathling Python library SHALL expose the single-resource FHIRPath evaluation
method through a Python function that wraps the Java library API via Py4J.

The Python result dict SHALL include a `traces` key containing a list of trace
entries, each with `label`, `type`, and `values` keys.

#### Scenario: Python evaluation call

- **WHEN** the Python function is called with a Patient JSON string and a
  FHIRPath expression
- **THEN** it returns a Python data structure containing the typed results, AST,
  and inferred return type

#### Scenario: Python trace data access

- **WHEN** the Python function is called with an expression containing
  `trace('myLabel')`
- **THEN** the returned dict includes a `traces` list with an entry for
  `myLabel` containing FHIR-typed values
