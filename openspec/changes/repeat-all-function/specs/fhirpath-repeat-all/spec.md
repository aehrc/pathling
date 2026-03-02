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

#### Scenario: Termination at leaf nodes

- **WHEN** `repeatAll(item)` is evaluated on a Questionnaire where the deepest
  items have no child items
- **THEN** the function SHALL terminate and return all collected items

#### Scenario: Non-recursive collection projection

- **WHEN** `repeatAll` is called with a projection that does not navigate into a
  recursive structure and produces a collection (e.g.,
  `Patient.repeatAll(name)`)
- **THEN** the function SHALL silently return the same result as `select()` —
  one iteration producing results, followed by termination when the next
  iteration yields an empty collection

#### Scenario: Non-recursive singular projection

- **WHEN** `repeatAll` is called with a projection that produces a singular
  value (e.g., `Patient.repeatAll(gender)`)
- **THEN** the function SHALL return the same result as `select()` — the
  singular value wrapped as a single-element collection

#### Scenario: Non-recursive singular complex projection

- **WHEN** `repeatAll` is called with a projection that produces a singular
  complex type (e.g., `Patient.repeatAll(maritalStatus)`)
- **THEN** the function SHALL return the complex value with all its sub-elements
  intact

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

The function delegates recursive traversal to `UnresolvedTransformTree`, which
limits recursion depth based on Spark SQL `DataType` equality. The depth counter
only decrements when the traversal expression produces a node whose resolved SQL
`DataType` is structurally identical to its parent's. When the counter reaches
zero, traversal stops and returns results collected up to that point. The
hardcoded same-type depth limit is 10, matching the default
`maxUnboundTraversalDepth` used by the SQL on FHIR `repeat` clause.

For traversals through schema-truncated structures (e.g.,
`Questionnaire.repeatAll(item)`), each nesting level has a progressively smaller
`StructType` due to the encoding's `maxNestingLevel` truncation. These are NOT
same-type traversals — the depth counter is never decremented, and traversal
terminates naturally when a field is no longer present in the truncated schema
(`FIELD_NOT_FOUND`).

Same-type recursion occurs when the traversal produces an element with an
identical SQL `DataType` at every level. Known cases include:

- **Extension traversal**: Extension structs have a fixed flat schema at all
  nesting levels (extensions use a global `_extension` map rather than recursive
  struct fields), so `repeatAll(extension)` produces same-type nodes.
- **Identity-like traversals**: Functions such as `first()` that return an
  element of the same type as the input, causing the SQL `DataType` to remain
  unchanged across levels.

#### Scenario: Extension traversal bounded by actual depth or limit

- **WHEN** `repeatAll(extension)` is evaluated on a resource whose elements
  carry nested extensions
- **THEN** the function SHALL traverse extensions up to the actual nesting depth
  or the same-type depth limit (10), whichever is reached first, and return all
  collected extensions

#### Scenario: Identity-like traversal bounded by depth limit

- **WHEN** `repeatAll` is evaluated with a traversal that always produces the
  same SQL `DataType` (e.g., `repeatAll(first())` on a complex-typed
  collection)
- **THEN** the function SHALL stop after the same-type depth limit and return
  results collected up to that point

#### Scenario: Schema-truncated traversal terminates naturally

- **WHEN** `repeatAll(item)` is evaluated on a Questionnaire with nested items
- **THEN** traversal SHALL terminate naturally when the schema no longer
  contains the `item` field (due to `maxNestingLevel` truncation during the
  encoding), without consuming same-type depth budget

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
