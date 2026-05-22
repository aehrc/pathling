## ADDED Requirements

### Requirement: `repeat` produces typed empty result when traversal exits encoded schema

When a `repeat` directive's recursive traversal references a struct field
that does not exist in the encoded schema at the current nesting depth
(for example, a self-referencing `Item.item` traversal that walks past the
encoder's `maxNestingLevel`), the view runner SHALL produce an empty
result for that branch whose Spark element type matches the projection
clause's declared output schema. The result MUST be a `Cast(empty,
ArrayType(StructType(<declared fields>)))`, never an untyped
`Array<NullType>`.

This guarantees that sibling column combination through `StructProduct`
sees correctly-typed array element types and does not crash with
`ClassCastException` on `StructType` coercion.

#### Scenario: nested repeat at the encoder's nesting cap

- **GIVEN** a `Questionnaire` with a deeply nested `item` chain that
  reaches the encoder's `maxNestingLevel`.
- **WHEN** a ViewDefinition uses an outer `repeat: ["item"]` whose
  projection clause contains an inner `repeat: ["item"]` plus a sibling
  column at the inner select.
- **THEN** the view executes without throwing, and the result contains
  the rows produced by the resolvable traversals; the branches whose
  recursive traversal references a field absent from the encoded struct
  produce zero rows, not a crash.

#### Scenario: `repeat` with sibling column at deep nesting

- **GIVEN** a `Questionnaire.item.item.item.item` traversal that reaches
  the deepest encoded depth.
- **WHEN** the inner select combines a `repeat: ["item"]` with a sibling
  column whose path is a primitive field on the lambda variable.
- **THEN** the result schema contains both the sibling column and the
  repeat's projected columns; for resources where the recursive traversal
  exits the encoded schema, zero rows are emitted but the column shape
  is preserved.

#### Scenario: triple-nested `repeat / forEach / repeat`

- **GIVEN** a `Questionnaire` whose `item` elements contain `answer`
  arrays which in turn contain further `item` arrays.
- **WHEN** a ViewDefinition uses `repeat: ["item"]` with an inner select
  containing `forEach: "answer"` whose nested select contains another
  `repeat: ["item"]`, each declaring its own primitive column outputs.
- **THEN** the view executes without throwing for resources that
  partially populate the chain; branches whose recursive traversal walks
  past the encoder's nesting cap produce no rows but do not propagate
  untyped empty arrays into surrounding `StructProduct` operations.

#### Scenario: three-deep nested repeats

- **GIVEN** a `Questionnaire` with `item` nesting up to three levels.
- **WHEN** a ViewDefinition uses `repeat: ["item"]` whose inner select
  contains a `repeat: ["item"]` whose inner select contains a further
  `repeat: ["item"]`, with each level declaring a primitive column on
  `linkId`.
- **THEN** the view executes without throwing; the resolvable depths
  contribute rows; depths past the encoder's cap contribute no rows
  without raising `ClassCastException`.

---

### Requirement: `repeat` expected element type derives from declared projection metadata

The Spark `StructType` used for the typed empty fallback SHALL be derived
from the projection clause's declared output columns, in the following
precedence order per column:

1. The column's `sqlType` annotation if present.
2. The column's `type` (FHIR primitive) annotation mapped to a Spark
   primitive type if present.
3. The Spark element type of the column's resolved
   `Materializable.getExternalValue(...)` representation as a final
   fallback.

For columns declared with `collection: true`, the resulting field type
MUST be wrapped in `ArrayType`. The derivation MUST NOT depend on
Catalyst resolution of the column's value column when explicit `sqlType`
or `type` is declared.

#### Scenario: column with explicit `type: string`

- **GIVEN** a `repeat` projection clause declaring a column with
  `type: "string"`.
- **WHEN** the typed empty fallback's `StructType` is derived.
- **THEN** the field for that column is `StructField(<name>, StringType,
  nullable=true)`.

#### Scenario: column with `collection: true` and explicit primitive type

- **GIVEN** a `repeat` projection clause declaring a column with
  `type: "integer"` and `collection: true`.
- **WHEN** the typed empty fallback's `StructType` is derived.
- **THEN** the field for that column is `StructField(<name>,
  ArrayType(IntegerType), nullable=true)`.

#### Scenario: nested clause shapes contribute fields flat

- **GIVEN** a `repeat` whose projection clause is a grouping of a
  column and a nested `forEach` projecting more columns.
- **WHEN** the typed empty fallback's `StructType` is derived.
- **THEN** the resulting `StructType` contains one `StructField` per
  declared column across all leaf projections, in declaration order,
  matching the flat `List<ProjectedColumn>` produced by
  `component.evaluate(...)`.

---

### Requirement: Non-`repeat` callers of `transformTree` remain unaffected

The encoder-level utility `ValueFunctions.transformTree` SHALL keep its
existing public overloads unchanged. The typed empty fallback SHALL apply
only when an `expectedElementType` is supplied through the new overload.
When no expected element type is supplied, the FIELD_NOT_FOUND fallback
MUST continue to emit `CreateArray(Seq.empty)` as today.

#### Scenario: existing callers compile unchanged

- **GIVEN** existing callers of `ValueFunctions.transformTree` that pass
  the historical parameter set.
- **WHEN** the change is applied.
- **THEN** those callers continue to compile and exhibit identical
  runtime behaviour.

#### Scenario: opt-in typed fallback

- **WHEN** a caller invokes the new `transformTree` overload supplying an
  explicit `StructType expectedElementType`.
- **THEN** the FIELD_NOT_FOUND fallback at the root node returns
  `Cast(CreateArray(Seq.empty), ArrayType(expectedElementType))`;
  fallbacks at inner nodes continue to return `CreateArray(Seq.empty)`.
