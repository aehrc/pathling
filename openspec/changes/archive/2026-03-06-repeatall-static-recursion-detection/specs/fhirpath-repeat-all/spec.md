## MODIFIED Requirements

### Requirement: repeatAll function terminates on empty iteration

The function SHALL terminate when an iteration of the projection expression
across all current items produces an empty collection (no new results).

When the traversal expression applied to its own result (level_1) produces a
statically empty collection, the function SHALL return the level_0 result
directly for both primitive and complex types. This is equivalent to `select()`
behavior.

#### Scenario: Termination at leaf nodes

- **WHEN** `repeatAll(item)` is evaluated on a Questionnaire where the deepest
  items have no child items
- **THEN** the function SHALL terminate and return all collected items

#### Scenario: Non-recursive collection projection

- **WHEN** `repeatAll` is called with a projection that does not navigate into a
  recursive structure and produces a collection (e.g.,
  `Patient.repeatAll(name)`)
- **THEN** the function SHALL return the level_0 result directly, equivalent to
  `select()`

#### Scenario: Non-recursive singular projection

- **WHEN** `repeatAll` is called with a projection that produces a singular
  value (e.g., `Patient.repeatAll(gender)`)
- **THEN** the function SHALL return the level_0 result directly, equivalent to
  `select()`

#### Scenario: Non-recursive singular complex projection

- **WHEN** `repeatAll` is called with a projection that produces a singular
  complex type (e.g., `Patient.repeatAll(maritalStatus)`)
- **THEN** the function SHALL return the level_0 result directly with all
  sub-elements intact, equivalent to `select()`

### Requirement: repeatAll function bounds same-type recursion depth

The function SHALL perform a static type analysis before entering tree
traversal. It SHALL apply the traversal expression to the input to produce
level_0, then apply the traversal expression to level_0 to produce level_1.
The function SHALL classify behavior based on the FHIR type comparison between
level_0 and level_1:

1. **level_1 is empty**: Return level_0 directly (equivalent to `select()`).
   This applies to both primitive and complex types.
2. **level_1 is same FHIR type as level_0, primitive**: Raise an immediate
   error indicating self-referential primitive traversal.
3. **level_1 is same FHIR type as level_0, complex, Extension**: Proceed to
   recursive tree traversal with silent stop on depth exhaustion.
4. **level_1 is same FHIR type as level_0, complex, non-Extension**: Proceed
   to recursive tree traversal with error on depth exhaustion.
5. **level_1 is a different non-empty FHIR type from level_0**: Raise an
   immediate error indicating inconsistent traversal types.

For case 3 and 4, the function delegates to `variantTransformTree` with
`UnresolvedTransformTree` providing analysis-time same-SQL-type depth limiting.
The depth limit for Extension traversal is controlled by `maxExtensionDepth` in
`FhirpathConfiguration`.

#### Scenario: Self-referential primitive identity traversal detected

- **WHEN** `gender.repeatAll($this)` is evaluated
- **THEN** the function SHALL raise an error indicating self-referential
  primitive traversal

#### Scenario: Self-referential primitive constant traversal detected

- **WHEN** `gender.repeatAll('someValue')` is evaluated
- **THEN** the function SHALL raise an error indicating self-referential
  primitive traversal

#### Scenario: Self-referential primitive function traversal detected

- **WHEN** `gender.repeatAll(length())` is evaluated
- **THEN** the function SHALL raise an error indicating self-referential
  primitive traversal

#### Scenario: Infinite recursion detected for identity traversal on complex type

- **WHEN** `repeatAll($this)` is evaluated on any complex-typed collection
- **THEN** the function SHALL raise an evaluation error indicating infinite
  recursive traversal was detected (via analysis-time detection)

#### Scenario: Infinite recursion detected for first() traversal

- **WHEN** `name.repeatAll(first())` is evaluated on a resource with name
  elements
- **THEN** the function SHALL raise an evaluation error indicating infinite
  recursive traversal was detected (via analysis-time detection)

#### Scenario: Extension traversal bounded by configured depth

- **WHEN** `repeatAll(extension)` is evaluated on a resource whose elements
  carry nested extensions and `maxExtensionDepth` is set to 5
- **THEN** the function SHALL traverse extensions up to depth 5, returning all
  collected extensions, and SHALL NOT raise an error

#### Scenario: Extension traversal with default depth

- **WHEN** `repeatAll(extension)` is evaluated with default configuration
- **THEN** the function SHALL traverse extensions up to depth 10

#### Scenario: Schema-truncated traversal terminates naturally

- **WHEN** `repeatAll(item)` is evaluated on a Questionnaire with nested items
- **THEN** traversal SHALL terminate naturally when the schema no longer
  contains the `item` field (due to `maxNestingLevel` truncation during the
  encoding), without consuming same-type depth budget and without raising an
  error

#### Scenario: Unknown FHIR type defaults to error mode

- **WHEN** `repeatAll` is evaluated with a projection whose FHIR type is
  unknown and same-type depth is exhausted
- **THEN** the function SHALL raise an evaluation error, as unknown types that
  hit the depth limit are more likely bugs than intentional recursion

#### Scenario: Inconsistent traversal types detected

- **WHEN** `repeatAll` is evaluated with a traversal expression that produces a
  different non-empty FHIR type when applied to its own result
- **THEN** the function SHALL raise an error indicating inconsistent traversal
  types

### Requirement: repeatAll function uses distinct error messages

The function SHALL use distinct error messages to differentiate between
detection contexts:

1. **Static primitive self-reference**: A message indicating that the traversal
   expression produces a self-referential primitive type that cannot terminate.
2. **Static inconsistent types**: A message indicating that the traversal
   expression does not produce a consistent type across recursive applications.
3. **Analysis-time complex depth exhaustion**: The existing message "Infinite
   recursive traversal detected." from `UnresolvedTransformTree`.

#### Scenario: Primitive self-reference error message

- **WHEN** a self-referential primitive traversal is detected statically
- **THEN** the error message SHALL be distinct from the analysis-time
  "Infinite recursive traversal detected." message

#### Scenario: Inconsistent type error message

- **WHEN** an inconsistent traversal type is detected statically
- **THEN** the error message SHALL indicate that the traversal expression
  produces inconsistent types

#### Scenario: Analysis-time error message preserved

- **WHEN** a complex non-Extension same-SQL-type depth exhaustion occurs during
  Catalyst analysis
- **THEN** the error message SHALL remain "Infinite recursive traversal
  detected."
