## ADDED Requirements

### Requirement: repeatAll function recursively traverses and collects elements

The FHIRPath engine SHALL support the
`repeatAll(projection: ($this) => collection) : collection` function. The
function SHALL evaluate the projection expression for each item in the input
collection, add all results to the output collection, then re-evaluate the
projection on the new results. This process SHALL repeat until an iteration
produces no new results.

Unlike `repeat()`, the function SHALL NOT check for duplicate items — all
results SHALL be added to the output collection regardless of whether they
already exist.

#### Scenario: Single-level traversal

- **WHEN** `repeatAll` is called on a Questionnaire resource with a single level
  of items and the projection `item`
- **THEN** the result SHALL contain all items from that single level

#### Scenario: Multi-level recursive traversal

- **WHEN** `repeatAll` is called on a Questionnaire resource with nested items
  (items containing sub-items) and the projection `item`
- **THEN** the result SHALL contain items from all nesting levels

#### Scenario: Duplicate items are preserved

- **WHEN** `repeatAll` is called and the same item would be reached through
  multiple traversal paths
- **THEN** all occurrences SHALL be included in the output collection (no
  deduplication)

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

### Requirement: repeatAll function handles empty input

If the input collection is empty, the result SHALL be empty. This SHALL hold
both when the input is a typed collection that evaluates to empty at runtime
(e.g., a resource where the traversed field is absent) and when the input is
the FHIRPath empty literal `{}`.

#### Scenario: Empty collection from absent field

- **WHEN** `repeatAll` is called on a resource where the traversed field is
  absent (e.g., a Questionnaire with no items)
- **THEN** the result SHALL be an empty collection

#### Scenario: Empty collection literal

- **WHEN** `repeatAll` is called on the FHIRPath empty literal `{}` (e.g.,
  `{}.repeatAll(item)`)
- **THEN** the result SHALL be an empty collection

### Requirement: repeatAll function supports chained expressions

The result of `repeatAll` SHALL be a standard collection that supports
subsequent FHIRPath operations such as path navigation, `where()`, `count()`,
`select()`, and other functions.

#### Scenario: Path navigation after repeatAll

- **WHEN** a FHIRPath expression chains a path after `repeatAll` (e.g.,
  `Questionnaire.repeatAll(item).linkId`)
- **THEN** the result SHALL contain the `linkId` values from all recursively
  collected items

#### Scenario: Filtering after repeatAll

- **WHEN** a FHIRPath expression applies `where()` after `repeatAll` (e.g.,
  `Questionnaire.repeatAll(item).where(type = 'group')`)
- **THEN** the result SHALL contain only items matching the filter from all
  nesting levels

#### Scenario: Counting after repeatAll

- **WHEN** a FHIRPath expression applies `count()` after `repeatAll` (e.g.,
  `Questionnaire.repeatAll(item).count()`)
- **THEN** the result SHALL be the total number of items across all nesting
  levels, including duplicates

### Requirement: repeatAll function bounds same-type recursion depth

The function SHALL perform a static type analysis before entering tree
traversal. It SHALL apply the traversal expression to the input to produce
level_0, then apply the traversal expression to level_0 to produce level_1.
The function SHALL classify behavior based on the FHIR type comparison between
level_0 and level_1:

1. **level_1 is empty**: Return level_0 directly (equivalent to `select()`).
   This applies to both primitive and complex types.
2. **level_0 or level_1 has indeterminate FHIR type**: Raise an immediate
   error indicating that the recursive traversal expression produces a result
   with indeterminate FHIR type. This occurs when `getFhirType()` returns
   empty on either side (e.g., choice type elements that have not been resolved
   via `ofType()`).
3. **level_1 is same FHIR type as level_0, primitive or resource,
   self-reference not allowed**: Raise an immediate error indicating
   self-referential traversal. Resource types are treated like primitives
   because they are represented with a boolean existence column (not a
   struct) and cannot be recursed into via `variantTransformTree`.
4. **level_1 is same FHIR type as level_0, primitive or resource,
   self-reference allowed**: Return level_0 directly. The caller (i.e.,
   `repeat()`) is responsible for deduplication, which guarantees
   termination.
5. **level_1 is same FHIR type as level_0, complex, Extension**: Proceed to
   recursive tree traversal with silent stop on depth exhaustion.
6. **level_1 is same FHIR type as level_0, complex, non-Extension,
   self-reference not allowed**: Proceed to recursive tree traversal with
   error on depth exhaustion.
7. **level_1 is same FHIR type as level_0, complex, non-Extension,
   self-reference allowed**: Proceed to recursive tree traversal with silent
   stop on depth exhaustion. The caller (i.e., `repeat()`) is responsible for
   deduplication, which guarantees termination.
8. **level_1 is a different non-empty FHIR type from level_0**: Raise an
   immediate error indicating inconsistent traversal types.

The `allowSelfReference` flag controls both primitive (case 3 vs 4) and complex
(case 6 vs 7) behavior. When set, depth exhaustion does not raise an error,
and the caller is expected to deduplicate the results.

For cases 5, 6, and 7, the function delegates to `variantTransformTree` with
`UnresolvedTransformTree` providing analysis-time same-SQL-type depth limiting.
The depth limit for Extension traversal is controlled by `maxUnboundTraversalDepth` in
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

#### Scenario: Indeterminate type on choice element with identity traversal

- **WHEN** `value.repeatAll($this)` is evaluated on an Observation resource
  where `value` is a choice type (value[x])
- **THEN** the function SHALL raise an error indicating indeterminate FHIR type

#### Scenario: Choice element with ofType fails during type probing

- **WHEN** `value.repeatAll(ofType(Quantity))` is evaluated on an Observation
  resource where `value` is a choice type
- **THEN** the function SHALL raise an error during type probing because the
  `ofType()` transform applied to the MixedCollection attempts to reconstruct
  a collection without a concrete FHIR type or definition

#### Scenario: Choice element traversal fails on polymorphic navigation

- **WHEN** `repeatAll(value)` is evaluated on an Observation resource where
  `value` is a choice type
- **THEN** the function SHALL raise an error indicating that direct traversal
  of polymorphic collections is not supported

#### Scenario: Choice element with first() fails during type probing

- **WHEN** `value.repeatAll(first())` is evaluated on an Observation resource
  where `value` is a choice type
- **THEN** the function SHALL raise an error during type probing because
  `first()` on a MixedCollection attempts to reconstruct a collection without
  a concrete FHIR type or definition

#### Scenario: Resource-level identity traversal raises self-referential error

- **WHEN** `repeatAll($this)` is evaluated on a resource (e.g., Patient)
- **THEN** the function SHALL raise an error indicating self-referential
  traversal that cannot terminate, because resource types are treated like
  primitives in the type analysis gate

#### Scenario: Resource-level %resource projection raises self-referential error

- **WHEN** `name.repeatAll(%resource).gender` is evaluated on a Patient
  resource
- **THEN** the function SHALL raise an error indicating self-referential
  traversal that cannot terminate

#### Scenario: Self-reference flag suppresses depth exhaustion error

- **WHEN** `repeatAll` is invoked internally with self-reference allowed (as
  done by `repeat()`) and the traversal expression produces same-type complex
  results that exhaust the depth limit
- **THEN** the function SHALL NOT raise an error, instead returning all
  collected results up to the depth limit for the caller to deduplicate

#### Scenario: Extension traversal bounded by configured depth

- **WHEN** `repeatAll(extension)` is evaluated on a resource whose elements
  carry nested extensions and `maxUnboundTraversalDepth` is set to 5
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

1. **Static self-reference**: A message indicating that the traversal
   expression produces a self-referential type that cannot terminate. This
   covers both primitive types and resource types (which are treated like
   primitives).
2. **Static indeterminate type**: A message indicating that the recursive
   traversal expression produces a result with indeterminate FHIR type.
3. **Static inconsistent types**: A message indicating that the traversal
   expression does not produce a consistent type across recursive applications.
4. **Analysis-time complex depth exhaustion**: The existing message "Infinite
   recursive traversal detected." from `UnresolvedTransformTree`.

#### Scenario: Self-reference error message

- **WHEN** a self-referential traversal is detected statically (primitive or
  resource type)
- **THEN** the error message SHALL be distinct from the analysis-time
  "Infinite recursive traversal detected." message

#### Scenario: Indeterminate type error message

- **WHEN** an indeterminate FHIR type is detected statically
- **THEN** the error message SHALL indicate that the recursive traversal
  expression produces a result with indeterminate FHIR type, distinct from
  both the inconsistent type and primitive self-reference messages

#### Scenario: Inconsistent type error message

- **WHEN** an inconsistent traversal type is detected statically
- **THEN** the error message SHALL indicate that the traversal expression
  produces inconsistent types

#### Scenario: Analysis-time error message preserved

- **WHEN** a complex non-Extension same-SQL-type depth exhaustion occurs during
  Catalyst analysis
- **THEN** the error message SHALL remain "Infinite recursive traversal
  detected."

### Requirement: repeatAll function does not define $index

The `$index` variable SHALL NOT be set during evaluation of the projection
expression, consistent with the FHIRPath specification.

#### Scenario: $index is not available

- **WHEN** the projection expression references `$index` within `repeatAll`
- **THEN** `$index` SHALL NOT resolve to a value

### Requirement: repeatAll function preserves extensions and sub-elements

The result collection SHALL preserve all fields of the propagated elements,
including FHIR extensions and other sub-elements. This is a general invariant
for all element-propagating FHIRPath functions (e.g., `first()`, `where()`), but
SHALL be verified for `repeatAll()` given the schema alignment involved.

#### Scenario: Extensions preserved in results

- **WHEN** `repeatAll(item)` is evaluated on a Questionnaire where items contain
  extension elements
- **THEN** the result items SHALL retain their extension elements intact

### Requirement: repeatAll function is available as a FHIRPath function

The `repeatAll` function SHALL be registered and invocable using standard
FHIRPath function call syntax (e.g., `collection.repeatAll(expression)`).

#### Scenario: Function invocation syntax

- **WHEN** a FHIRPath expression uses `repeatAll` with dot notation (e.g.,
  `Questionnaire.repeatAll(item)`)
- **THEN** the function SHALL be resolved and executed correctly
