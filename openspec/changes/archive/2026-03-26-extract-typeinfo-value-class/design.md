## Context

The `type()` reflection function was implemented in commit c4811f67ff. The
implementation works correctly but the type reflection knowledge is spread
across three locations:

- `TypeFunctions` — type resolution logic, struct construction, namespace
  string literals.
- `Collection` — `TYPE_INFO_DEFINITION` constant and `buildWithDefinition()`
  wiring.
- `TypeSpecifier` — namespace constants (`SYSTEM_NAMESPACE`, `FHIR_NAMESPACE`)
  that `TypeFunctions` duplicates as raw strings.

## Goals / Non-Goals

**Goals:**

- Consolidate TypeInfo data and logic into a single `TypeInfo` value class.
- Eliminate raw namespace string literals in favour of `TypeSpecifier` constants.
- Keep `TypeFunctions.type()` as a thin FHIRPath function that delegates to
  `TypeInfo`.
- Zero behavioural change — all existing tests pass without modification.

**Non-Goals:**

- Implementing `ClassInfo`, `ListTypeInfo`, or `TupleTypeInfo` from the full
  spec. That is future work.
- Changing `TypeSpecifier` itself.
- Adding new tests (this is a pure refactoring).

## Decisions

### New `TypeInfo` value class alongside `TypeSpecifier`

Create `TypeInfo.java` in `au.csiro.pathling.fhirpath` as a `@Value` class with
three fields: `namespace`, `name`, `baseType` (all `String`).

**Rationale:** `TypeSpecifier` is the _input_ abstraction (parsing `is`/`as`
expressions). `TypeInfo` is the _output_ abstraction (reflection results). They
are siblings conceptually, so they sit next to each other in the same package.

**Alternatives considered:** Making `TypeInfo` a nested record inside
`TypeFunctions` — rejected because the project avoids inner classes and this
data structure will likely be extended for `ClassInfo` in the future.

### `TypeInfo` owns factory methods for type resolution

Move the logic currently in `TypeFunctions.resolveTypeInfoStruct()` into static
factory methods on `TypeInfo`:

- `fromCollection(Collection)` → `Optional<TypeInfo>` — the main dispatch.
- `forFhirType(FHIRDefinedType, boolean isResource)` → `TypeInfo`.
- `forSystemType(FhirPathType)` → `TypeInfo`.
- `forTypeInfo()` → `TypeInfo` (the `System.Object` case).

**Rationale:** The knowledge of how to map a collection to its type metadata
belongs with the type metadata class, not with the FHIRPath function provider.

### `TypeInfo` owns `DEFINITION` and `toStructColumn()`

Move `TYPE_INFO_DEFINITION` from `Collection` to `TypeInfo.DEFINITION`. Add an
instance method `toStructColumn()` that builds the Spark struct from the three
fields.

**Rationale:** The struct schema and its construction are intrinsic to what a
TypeInfo _is_. `Collection` should not need to know about TypeInfo internals.

### `TypeFunctions.type()` becomes a thin delegate

After extraction, `type()` only does: check for empty → call
`TypeInfo.fromCollection()` → map to column → wrap in collection. No type
resolution logic remains in `TypeFunctions`.

## Risks / Trade-offs

- **Risk:** `Collection` currently uses `TYPE_INFO_DEFINITION` for identity
  comparison in `resolveTypeInfoStruct`. After moving the constant, the
  reference changes to `TypeInfo.DEFINITION`, introducing a dependency from
  `TypeInfo.fromCollection()` back to `TypeInfo.DEFINITION`.
  → **Mitigation:** This is a self-referential check within `TypeInfo` itself,
  which is cleaner than the current cross-class reference.

- **Risk:** Adding a new file for a small class.
  → **Mitigation:** The class will grow when `ClassInfo`/`ListTypeInfo` are
  implemented. The extraction pays for itself even now in clarity.
