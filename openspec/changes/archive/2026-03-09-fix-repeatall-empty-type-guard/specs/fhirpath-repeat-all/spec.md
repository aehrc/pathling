## MODIFIED Requirements

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
