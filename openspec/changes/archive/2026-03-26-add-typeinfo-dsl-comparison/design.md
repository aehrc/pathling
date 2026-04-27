## Context

The DSL test harness (`FhirPathTestBuilder` + `DefaultYamlTestExecutor`) supports direct equality comparison for primitive values, `FhirTypedLiteral` (Quantity, Coding), and lists. TypeInfo results from `type()` are struct Rows with three string fields (namespace, name, baseType), but there is no typed expectation wrapper for them. Tests currently assert each field individually:

```java
.testEquals("System", "1.type().namespace", "...")
.testEquals("Integer", "1.type().name", "...")
.testEquals("System.Any", "1.type().baseType", "...")
```

## Goals / Non-Goals

**Goals:**

- Enable single-assertion TypeInfo comparison in DSL tests using a compact string format.
- Follow existing patterns (Quantity/Coding comparison) for consistency.

**Non-Goals:**

- Adding TypeInfo comparison support for YAML reference tests.
- Changing the TypeInfo Spark struct representation.
- Adding a `TypeInfoCollection` subclass to production code.

## Decisions

### 1. Explicit marker class (`TypeInfoExpectation`) over convention-based detection

**Decision**: Create a `TypeInfoExpectation` class that the executor can `instanceof` match against, rather than detecting TypeInfo results from the collection's definition metadata.

**Rationale**: Explicit markers are unambiguous and consistent with how `FhirTypedLiteral` works for Quantity/Coding. Detection-based approaches (checking `collection.getDefinition() == TypeInfo.DEFINITION`) couple the test expectation format to production code internals.

**Alternatives considered**:

- **B3 (detect from collection definition)**: No marker class needed, plain strings as expectations. Rejected because it's implicit — the executor silently reinterprets string expectations based on collection type, which could cause confusing failures if the collection type changes.
- **FhirTypedLiteral extension**: Adding a TypeInfo variant to `FhirTypedLiteral`. Rejected because `FhirTypedLiteral` is keyed on `FHIRDefinedType` and TypeInfo has no `FHIRDefinedType` value.

### 2. String format: `Namespace.Name(BaseType)`

**Decision**: Use `"System.Integer(System.Any)"` format — namespace dot name, with baseType in parentheses.

**Rationale**: Reads naturally (the parenthesized baseType suggests "derives from"), is unambiguous since namespace and name are single tokens, and is compact enough for inline test use.

### 3. Package placement: `test.dsl`

**Decision**: Place `TypeInfoExpectation` in `au.csiro.pathling.test.dsl`, not alongside `FhirTypedLiteral` in `test.yaml`.

**Rationale**: This is a DSL testing convenience. `FhirTypedLiteral` serves the YAML test infrastructure; `TypeInfoExpectation` is specific to DSL test assertions.

### 4. Executor handles both singular and array comparison

**Decision**: The executor branch handles both single `TypeInfoExpectation` and `List<TypeInfoExpectation>` for plural TypeInfo results.

**Rationale**: `type()` can return multiple TypeInfo structs (e.g., `('a' | 'b').type()` returns two). Array comparison follows the same normalisation pattern as Quantity arrays.

## Risks / Trade-offs

- **Parsing fragility**: The regex `^(\w+)\.(\w+)\((.+)\)$` assumes namespace and name contain only word characters. This holds for all current TypeInfo values (System, FHIR namespaces; simple type names). → If exotic names appear in future, the parser can be relaxed.
- **Test refactoring scope**: Rewriting existing `TypeFunctionsDslTest` assertions is optional but recommended for consistency. → Can be done incrementally.
