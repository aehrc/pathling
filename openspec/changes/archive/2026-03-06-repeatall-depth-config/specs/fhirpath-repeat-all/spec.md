## MODIFIED Requirements

### Requirement: repeatAll function bounds same-type recursion depth

The function delegates recursive traversal to `UnresolvedTransformTree`, which
limits recursion depth based on Spark SQL `DataType` equality. The depth counter
only decrements when the traversal expression produces a node whose resolved SQL
`DataType` is structurally identical to its parent's.

For traversals through schema-truncated structures (e.g.,
`Questionnaire.repeatAll(item)`), each nesting level has a progressively smaller
`StructType` due to the encoding's `maxNestingLevel` truncation. These are NOT
same-type traversals — the depth counter is never decremented, and traversal
terminates naturally when a field is no longer present in the truncated schema
(`FIELD_NOT_FOUND`).

Same-type recursion occurs when the traversal produces an element with an
identical SQL `DataType` at every level. The function SHALL distinguish two
cases:

- **Extension traversal**: When the projection produces a result with FHIR type
  `Extension`, the function SHALL silently stop traversal when the configured
  `maxExtensionDepth` is reached, returning all results collected up to that
  point. This supports legitimate recursive navigation of Extension hierarchies.
- **Non-Extension same-type traversal**: When the projection produces a
  non-Extension type and same-type depth is exhausted, the function SHALL raise
  an evaluation error with a message indicating that infinite recursive
  traversal was detected. This covers identity-like traversals such as
  `repeatAll($this)`, `repeatAll(first())`, and `repeatAll('const')` that would
  otherwise loop indefinitely.

The same-type recursion depth is controlled by the `maxExtensionDepth` setting
in `FhirpathConfiguration`, with a default value of 10.

#### Scenario: Extension traversal bounded by configured depth

- **WHEN** `repeatAll(extension)` is evaluated on a resource whose elements
  carry nested extensions and `maxExtensionDepth` is set to 5
- **THEN** the function SHALL traverse extensions up to depth 5, returning all
  collected extensions, and SHALL NOT raise an error

#### Scenario: Extension traversal with default depth

- **WHEN** `repeatAll(extension)` is evaluated with default configuration
- **THEN** the function SHALL traverse extensions up to depth 10

#### Scenario: Infinite recursion detected for identity traversal

- **WHEN** `repeatAll($this)` is evaluated on any collection
- **THEN** the function SHALL raise an evaluation error indicating infinite
  recursive traversal was detected

#### Scenario: Infinite recursion detected for first() traversal

- **WHEN** `name.repeatAll(first())` is evaluated on a resource with name
  elements
- **THEN** the function SHALL raise an evaluation error indicating infinite
  recursive traversal was detected

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
