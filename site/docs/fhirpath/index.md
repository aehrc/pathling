---
sidebar_position: 1
description: Pathling implements FHIRPath functionality to aid in querying and constructing views over FHIR data.
---

# FHIRPath

Pathling leverages the [FHIRPath](https://hl7.org/fhirpath/)
language in order to abstract away some of the complexity of navigating and
interacting with FHIR data structures.

Pathling implements the FHIRPath subset within
the [Sharable View Definition](https://sql-on-fhir.org/ig/latest/StructureDefinition-ShareableViewDefinition.html#required-fhirpath-expressionsfunctions)
profile of
the [SQL on FHIR view definition](https://sql-on-fhir.org/ig/latest/StructureDefinition-ViewDefinition.html),
plus additional terminology and utility functions.

## Supported language features

### Path navigation

Pathling supports standard
FHIRPath [path navigation](https://hl7.org/fhirpath/#path-selection) using dot
notation:

```
Patient.name.given
Observation.code.coding.system
```

Array indexing is supported using bracket notation:

```
Patient.name[0].given
```

The `$this` special variable is supported for referring to the current context
within expressions.

### Literals

Pathling supports the
following [literal types](https://hl7.org/fhirpath/#literals):

| Type     | Syntax                     | Examples               |
| -------- | -------------------------- | ---------------------- |
| Boolean  | `true`, `false`            | `active = true`        |
| String   | Single quotes with escapes | `'hello'`, `'it\'s'`   |
| Integer  | Whole numbers              | `123`, `-45`           |
| Decimal  | Numbers with decimal point | `3.14`, `-0.5`         |
| Date     | `@` prefix, ISO 8601       | `@2023-01-15`          |
| DateTime | `@` prefix, ISO 8601       | `@2023-01-15T14:30:00` |
| Time     | `@T` prefix                | `@T14:30:00`           |
| Quantity | Number with unit           | `10 'mg'`, `4 days`    |

### Operators

See [Operators](https://hl7.org/fhirpath/#operators) in the FHIRPath
specification for detailed semantics.

#### Comparison operators

| Operator | Description              |
| -------- | ------------------------ |
| `=`      | Equality                 |
| `!=`     | Inequality               |
| `<`      | Less than                |
| `<=`     | Less than or equal to    |
| `>`      | Greater than             |
| `>=`     | Greater than or equal to |

#### Boolean operators

| Operator  | Description         |
| --------- | ------------------- |
| `and`     | Logical AND         |
| `or`      | Logical OR          |
| `xor`     | Exclusive OR        |
| `implies` | Logical implication |

#### Arithmetic operators

| Operator | Description                     |
| -------- | ------------------------------- |
| `+`      | Addition / string concatenation |
| `-`      | Subtraction                     |
| `*`      | Multiplication                  |
| `/`      | Division                        |
| `mod`    | Modulus                         |

Unary `+` and `-` are also supported for numeric values.

#### Collection operators

| Operator   | Description                                                     |
| ---------- | --------------------------------------------------------------- |
| `\|`       | Union of two collections (equivalent to the `union()` function) |
| `in`       | Test if element is in collection                                |
| `contains` | Test if collection contains element                             |

#### Type operators

| Operator | Description   |
| -------- | ------------- |
| `is`     | Type checking |
| `as`     | Type casting  |

### Standard functions

The following standard FHIRPath functions are implemented. See
[Functions](https://hl7.org/fhirpath/#functions) in the FHIRPath specification
for detailed semantics.

#### Existence functions

| Function            | Description                                                                                       |
| ------------------- | ------------------------------------------------------------------------------------------------- |
| `exists(criteria?)` | Returns `true` if the collection has any elements, optionally filtered by criteria                |
| `empty()`           | Returns `true` if the collection is empty                                                         |
| `count()`           | Returns the integer count of items in the collection (0 if empty)                                 |
| `all(criteria)`     | Returns `true` if criteria evaluates to `true` for every element (`true` for an empty collection) |
| `allTrue()`         | Returns `true` if all Boolean items are `true` (`true` for an empty collection)                   |
| `anyTrue()`         | Returns `true` if any Boolean item is `true` (`false` for an empty collection)                    |
| `allFalse()`        | Returns `true` if all Boolean items are `false` (`true` for an empty collection)                  |
| `anyFalse()`        | Returns `true` if any Boolean item is `false` (`false` for an empty collection)                   |
| `isDistinct()`      | Returns `true` if all items in the collection are distinct, using FHIRPath equality (`=`)         |

#### Filtering and projection functions

| Function                | Description                                                                                  |
| ----------------------- | -------------------------------------------------------------------------------------------- |
| `where(criteria)`       | Filter collection by criteria expression                                                     |
| `select(projection)`    | Evaluate projection for each element, flattening results                                     |
| `ofType(type)`          | Filter collection by type                                                                    |
| `repeat(projection)`    | Recursively evaluate projection, deduplicating results                                       |
| `repeatAll(projection)` | Recursively evaluate projection without deduplication (see below)                            |
| `distinct()`            | Returns only the unique items, using FHIRPath equality (`=`); element order is not preserved |

#### Subsetting functions

| Function  | Description                                 |
| --------- | ------------------------------------------- |
| `first()` | Returns the first element of the collection |

#### Combining functions

| Function         | Description                                                                              |
| ---------------- | ---------------------------------------------------------------------------------------- |
| `union(other)`   | Merge two collections, eliminating duplicates via FHIRPath equality (equivalent to `\|`) |
| `combine(other)` | Merge two collections without eliminating duplicates                                     |

Both functions follow the type-reconciliation rules used by the `|` operator
(for example, Integer is promoted to Decimal when merging with a Decimal
collection), and neither introduces a new iteration context — arguments are
evaluated against the same focus that applies to the function's input, so
expressions like `name.select(use.union(given))` resolve `given` against the
current `name` element, matching `name.select(use | given)`.

#### Boolean functions

| Function | Description      |
| -------- | ---------------- |
| `not()`  | Boolean negation |

#### String functions

| Function           | Description                          |
| ------------------ | ------------------------------------ |
| `join(separator?)` | Join strings with optional separator |

#### Type functions

| Function   | Description                                 |
| ---------- | ------------------------------------------- |
| `is(type)` | Type checking (equivalent to `is` operator) |
| `as(type)` | Type casting (equivalent to `as` operator)  |

#### Conversion functions

| Function            | Description                                                  |
| ------------------- | ------------------------------------------------------------ |
| `toBoolean()`       | Convert to Boolean                                           |
| `toInteger()`       | Convert to Integer                                           |
| `toDecimal()`       | Convert to Decimal                                           |
| `toString()`        | Convert to String                                            |
| `toDate()`          | Convert to Date                                              |
| `toDateTime()`      | Convert to DateTime                                          |
| `toTime()`          | Convert to Time                                              |
| `toQuantity(unit?)` | Convert to Quantity with optional unit conversion using UCUM |

| Function                    | Description                      |
| --------------------------- | -------------------------------- |
| `convertsToBoolean()`       | Check if convertible to Boolean  |
| `convertsToInteger()`       | Check if convertible to Integer  |
| `convertsToDecimal()`       | Check if convertible to Decimal  |
| `convertsToString()`        | Check if convertible to String   |
| `convertsToDate()`          | Check if convertible to Date     |
| `convertsToDateTime()`      | Check if convertible to DateTime |
| `convertsToTime()`          | Check if convertible to Time     |
| `convertsToQuantity(unit?)` | Check if convertible to Quantity |

### Limitations

The following FHIRPath features are **not currently supported**:

- **Equivalence operators**: `~` and `!~`
- **Aggregate functions**: `sum()`, `avg()`, `min()`, `max()`
- **Special variables**: `$index`, `$total`
- **Subset functions**: `subsetOf()` and `supersetOf()`
- **Quantity arithmetic**: Math operations on Quantity types
- **DateTime arithmetic**: DateTime math operations
- **Full resource resolution**: The `resolve()` function extracts type
  information only and does not support field traversal

#### Recursive traversal depth

Both `repeat` and `repeatAll` stop when a level of the traversal produces no
new items. The maximum depth guards against recursion that would never end on
its own: a projection that yields a value of the same SQL type as the level it
was applied to, such as `name.repeatAll(first())`. Each such same-type level
counts towards a configurable maximum depth, which defaults to 10. When the
maximum is reached, `repeatAll` and `repeat` raise an error, except when
traversing extensions, where they stop and return the results collected up to
that point.

Nested structures in the data, such as the `item` elements of a Questionnaire
or extensions nested within extensions, are stored with a different type at
each level, because each level contains the type of the level beneath it. Their
traversal therefore does not count towards the maximum depth, and stops where
the data does: every level the data carries is returned.

The maximum depth can be configured:

- In the **Pathling libraries**, via the `maxUnboundTraversalDepth` parameter in
  the query configuration.
- In the **Pathling server**, via the `pathling.query.maxUnboundTraversalDepth`
  configuration property.

## Additional functions

Pathling also supports additional functions beyond the standard FHIRPath
specification:

- [FHIR-specific functions](fhir-functions.md) - Functions defined in the FHIR
  specification for use with FHIR data, including `extension`, `resolve`,
  `memberOf`, `subsumes`, and `subsumedBy`.
- [Extension functions](extension-functions.md) - Functions unique to Pathling,
  including terminology functions like `designation`, `display`, `property`, and
  `translate`, plus the Coding literal data type.
