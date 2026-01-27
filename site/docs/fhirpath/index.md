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

| Operator | Description    |
| -------- | -------------- |
| `+`      | Addition       |
| `-`      | Subtraction    |
| `*`      | Multiplication |
| `/`      | Division       |
| `mod`    | Modulus        |

Unary `+` and `-` are also supported for numeric values.

#### String operators

| Operator | Description          |
| -------- | -------------------- |
| `&`      | String concatenation |

#### Collection operators

| Operator   | Description                         |
| ---------- | ----------------------------------- |
| `\|`       | Union of two collections            |
| `in`       | Test if element is in collection    |
| `contains` | Test if collection contains element |

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

| Function            | Description                                                                        |
| ------------------- | ---------------------------------------------------------------------------------- |
| `exists(criteria?)` | Returns `true` if the collection has any elements, optionally filtered by criteria |
| `empty()`           | Returns `true` if the collection is empty                                          |

#### Filtering and projection functions

| Function          | Description                              |
| ----------------- | ---------------------------------------- |
| `where(criteria)` | Filter collection by criteria expression |
| `ofType(type)`    | Filter collection by type                |

#### Subsetting functions

| Function  | Description                                 |
| --------- | ------------------------------------------- |
| `first()` | Returns the first element of the collection |

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
- **Lambda expressions**
- **Aggregate functions**: `count()`, `sum()`, `avg()`, `min()`, `max()`
- **Special variables**: `$index`, `$total`
- **Quantity arithmetic**: Math operations on Quantity types
- **DateTime arithmetic**: DateTime math operations
- **Full `resolve()`**: Traversal of resolved references

## Additional functions

Pathling also supports additional functions beyond the standard FHIRPath specification:

- [FHIR-specific functions](fhir-functions.md) - Functions defined in the FHIR
  specification for use with FHIR data, including `extension`, `resolve`,
  `memberOf`, `subsumes`, and `subsumedBy`.
- [Extension functions](extension-functions.md) - Functions unique to Pathling,
  including terminology functions like `designation`, `display`, `property`, and
  `translate`, plus the Coding literal data type.
