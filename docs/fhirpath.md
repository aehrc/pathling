# FHIRPath

Pathling leverages the [FHIRPath](https://hl7.org/fhirpath/) language in order to abstract away some of the complexity of navigating and interacting with FHIR data structures.

Pathling implements the FHIRPath subset within the [Sharable View Definition](https://sql-on-fhir.org/ig/latest/StructureDefinition-ShareableViewDefinition.html#required-fhirpath-expressionsfunctions) profile of the [SQL on FHIR view definition](https://sql-on-fhir.org/ig/latest/StructureDefinition-ViewDefinition.html), plus additional terminology and utility functions.

## Supported language features[​](#supported-language-features "Direct link to Supported language features")

### Path navigation[​](#path-navigation "Direct link to Path navigation")

Pathling supports standard FHIRPath [path navigation](https://hl7.org/fhirpath/#path-selection) using dot notation:

```
Patient.name.given
Observation.code.coding.system
```

Array indexing is supported using bracket notation:

```
Patient.name[0].given
```

The `$this` special variable is supported for referring to the current context within expressions.

### Literals[​](#literals "Direct link to Literals")

Pathling supports the following [literal types](https://hl7.org/fhirpath/#literals):

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

### Operators[​](#operators "Direct link to Operators")

See [Operators](https://hl7.org/fhirpath/#operators) in the FHIRPath specification for detailed semantics.

#### Comparison operators[​](#comparison-operators "Direct link to Comparison operators")

| Operator | Description              |
| -------- | ------------------------ |
| `=`      | Equality                 |
| `!=`     | Inequality               |
| `<`      | Less than                |
| `<=`     | Less than or equal to    |
| `>`      | Greater than             |
| `>=`     | Greater than or equal to |

#### Boolean operators[​](#boolean-operators "Direct link to Boolean operators")

| Operator  | Description         |
| --------- | ------------------- |
| `and`     | Logical AND         |
| `or`      | Logical OR          |
| `xor`     | Exclusive OR        |
| `implies` | Logical implication |

#### Arithmetic operators[​](#arithmetic-operators "Direct link to Arithmetic operators")

| Operator | Description                     |
| -------- | ------------------------------- |
| `+`      | Addition / string concatenation |
| `-`      | Subtraction                     |
| `*`      | Multiplication                  |
| `/`      | Division                        |
| `mod`    | Modulus                         |

Unary `+` and `-` are also supported for numeric values.

#### Collection operators[​](#collection-operators "Direct link to Collection operators")

| Operator   | Description                         |
| ---------- | ----------------------------------- |
| `\|`       | Union of two collections            |
| `in`       | Test if element is in collection    |
| `contains` | Test if collection contains element |

#### Type operators[​](#type-operators "Direct link to Type operators")

| Operator | Description   |
| -------- | ------------- |
| `is`     | Type checking |
| `as`     | Type casting  |

### Standard functions[​](#standard-functions "Direct link to Standard functions")

The following standard FHIRPath functions are implemented. See [Functions](https://hl7.org/fhirpath/#functions) in the FHIRPath specification for detailed semantics.

#### Existence functions[​](#existence-functions "Direct link to Existence functions")

| Function            | Description                                                                        |
| ------------------- | ---------------------------------------------------------------------------------- |
| `exists(criteria?)` | Returns `true` if the collection has any elements, optionally filtered by criteria |
| `empty()`           | Returns `true` if the collection is empty                                          |
| `count()`           | Returns the integer count of items in the collection (0 if empty)                  |

#### Filtering and projection functions[​](#filtering-and-projection-functions "Direct link to Filtering and projection functions")

| Function          | Description                              |
| ----------------- | ---------------------------------------- |
| `where(criteria)` | Filter collection by criteria expression |
| `ofType(type)`    | Filter collection by type                |

#### Subsetting functions[​](#subsetting-functions "Direct link to Subsetting functions")

| Function  | Description                                 |
| --------- | ------------------------------------------- |
| `first()` | Returns the first element of the collection |

#### Boolean functions[​](#boolean-functions "Direct link to Boolean functions")

| Function | Description      |
| -------- | ---------------- |
| `not()`  | Boolean negation |

#### String functions[​](#string-functions "Direct link to String functions")

| Function           | Description                          |
| ------------------ | ------------------------------------ |
| `join(separator?)` | Join strings with optional separator |

#### Type functions[​](#type-functions "Direct link to Type functions")

| Function   | Description                                 |
| ---------- | ------------------------------------------- |
| `is(type)` | Type checking (equivalent to `is` operator) |
| `as(type)` | Type casting (equivalent to `as` operator)  |

#### Conversion functions[​](#conversion-functions "Direct link to Conversion functions")

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

### Limitations[​](#limitations "Direct link to Limitations")

The following FHIRPath features are **not currently supported**:

* **Equivalence operators**: `~` and `!~`
* **Lambda expressions**
* **Aggregate functions**: `sum()`, `avg()`, `min()`, `max()`
* **Special variables**: `$index`, `$total`
* **Quantity arithmetic**: Math operations on Quantity types
* **DateTime arithmetic**: DateTime math operations
* **Full resource resolution**: The `resolve()` function extracts type information only and does not support field traversal

## Additional functions[​](#additional-functions "Direct link to Additional functions")

Pathling also supports additional functions beyond the standard FHIRPath specification:

* [FHIR-specific functions](/docs/fhirpath/fhir-functions.md) - Functions defined in the FHIR specification for use with FHIR data, including `extension`, `resolve`, `memberOf`, `subsumes`, and `subsumedBy`.
* [Extension functions](/docs/fhirpath/extension-functions.md) - Functions unique to Pathling, including terminology functions like `designation`, `display`, `property`, and `translate`, plus the Coding literal data type.
