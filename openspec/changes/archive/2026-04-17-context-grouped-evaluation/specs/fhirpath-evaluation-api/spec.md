## MODIFIED Requirements

### Requirement: Evaluate FHIRPath expression against a single resource

The library API SHALL provide a method that accepts a FHIR resource (as a JSON
string), a FHIRPath expression string, an optional context expression string,
and optional variables, and returns a `SingleInstanceEvaluationResult` containing
a list of `ResultGroup` objects and the expected return type.

The method SHALL use the existing Pathling FHIRPath engine (Spark-based) to
evaluate the expression, encoding the resource into a one-row Dataset
internally.

The method SHALL support all FHIRPath functions implemented in the engine,
including scoped functions that accept projection expressions (e.g., `select()`,
`repeatAll()`).

When the expression is empty or contains only whitespace, the fhirpath-lab-api
SHALL return a successful response with an empty collection (zero result groups)
without invoking the FHIRPath engine.

When no context expression is provided, the result SHALL contain a single
`ResultGroup` with `contextKey` set to null, containing all results and traces.

When a context expression is provided, the result SHALL contain one
`ResultGroup` per context element, each with a context key, scoped results, and
scoped traces.

The method SHALL create a `TraceCollector` before evaluation and attach it to
the evaluation context. After materialisation, collected trace entries SHALL be
included in the appropriate `ResultGroup` alongside the typed values.

#### Scenario: Simple expression evaluation

- **WHEN** the method is called with a Patient JSON resource and the expression
  `name.family`
- **THEN** the method returns a single `ResultGroup` (contextKey null) containing
  a string result with the patient's family name

#### Scenario: Expression returning multiple values

- **WHEN** the method is called with a Patient JSON resource containing two
  names and the expression `name.given`
- **THEN** the method returns a single `ResultGroup` (contextKey null) containing
  all given name strings

#### Scenario: Expression returning empty result

- **WHEN** the method is called with a Patient JSON resource and an expression
  that matches no elements (e.g., `multipleBirthBoolean`)
- **THEN** the method returns a single `ResultGroup` (contextKey null) with an
  empty results list

#### Scenario: Empty expression

- **WHEN** the fhirpath-lab-api receives a request with an expression that is an
  empty string or contains only whitespace
- **THEN** it returns a successful Parameters response with zero result groups
  and no type metadata, without invoking the FHIRPath engine

#### Scenario: Invalid expression

- **WHEN** the method is called with a syntactically invalid FHIRPath expression
- **THEN** the method throws an exception with a descriptive error message

#### Scenario: Context expression evaluation

- **WHEN** the method is called with a Patient JSON resource, context expression
  `name`, and expression `given.first()`
- **THEN** the method returns one `ResultGroup` per name entry, each with a
  context key (e.g., `name[0]`), containing the first given name for that entry

#### Scenario: Recursive function evaluation

- **WHEN** the method is called with a Questionnaire JSON resource and the
  expression `repeatAll(item).linkId`
- **THEN** the method returns a single `ResultGroup` (contextKey null) containing
  `linkId` values from all recursively nested items

#### Scenario: Trace data included in evaluation result

- **WHEN** the method is called with an expression containing `trace('label')`
- **THEN** the `ResultGroup` SHALL include a traces list with entries for the
  label, containing FHIR type and typed values

#### Scenario: No trace calls produces empty trace list

- **WHEN** the method is called with an expression that does not use `trace()`
- **THEN** each `ResultGroup` SHALL include an empty traces list

### Requirement: Python bindings for evaluation method

The Pathling Python library SHALL expose the single-resource FHIRPath evaluation
method through a Python function that wraps the Java library API via Py4J.

The Python result dict SHALL include a `resultGroups` key containing a list of
group dicts, each with `contextKey`, `results`, and `traces` keys.

#### Scenario: Python evaluation call

- **WHEN** the Python function is called with a Patient JSON string and a
  FHIRPath expression
- **THEN** it returns a Python dict with `expectedReturnType` and
  `resultGroups` containing a single group with contextKey None

#### Scenario: Python trace data access

- **WHEN** the Python function is called with an expression containing
  `trace('myLabel')`
- **THEN** the group dicts within `resultGroups` include a `traces` list with
  an entry for `myLabel` containing FHIR-typed values

#### Scenario: Python context evaluation

- **WHEN** the Python function is called with a context expression
- **THEN** the `resultGroups` list contains one entry per context element, each
  with a string `contextKey`
