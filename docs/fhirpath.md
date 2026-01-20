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

| Operator | Description    |
| -------- | -------------- |
| `+`      | Addition       |
| `-`      | Subtraction    |
| `*`      | Multiplication |
| `/`      | Division       |
| `mod`    | Modulus        |

Unary `+` and `-` are also supported for numeric values.

#### String operators[​](#string-operators "Direct link to String operators")

| Operator | Description          |
| -------- | -------------------- |
| `&`      | String concatenation |

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
* **Aggregate functions**: `count()`, `sum()`, `avg()`, `min()`, `max()`
* **Special variables**: `$index`, `$total`
* **Quantity arithmetic**: Math operations on Quantity types
* **DateTime arithmetic**: DateTime math operations
* **Full `resolve()`**: Traversal of resolved references

## FHIR-specific functions[​](#fhir-specific-functions "Direct link to FHIR-specific functions")

The following functions are defined in the [FHIR specification](https://hl7.org/fhir/R4/fhirpath.html#functions) as additional FHIRPath functions for use with FHIR data.

The notation used to describe the type signature of each function is as follows:

```
[input type] -> [function name]([argument name]: [argument type], ...): [return type]
```

### extension[​](#extension "Direct link to extension")

```
collection<Element> -> extension(url: String) : collection<Extension>
```

Filters the input collection to only those elements that have an extension with the specified URL. This is a shortcut for `.extension.where(url = string)`.

Example:

```
Patient.extension('http://hl7.org/fhir/StructureDefinition/patient-birthPlace')
```

### resolve[​](#resolve "Direct link to resolve")

```
collection<Reference> -> resolve() : collection<Resource>
```

For each Reference in the input collection, returns the resource that the reference points to.

note

Pathling has a limited implementation of `resolve()`. It supports type checking with the `is` operator but does not perform actual resource resolution or allow traversal of resolved references.

### memberOf[​](#memberof "Direct link to memberOf")

```
collection<Coding|CodeableConcept> -> memberOf(valueSetUrl: String) : collection<Boolean>
```

The `memberOf` function can be invoked on a collection of [Coding](#coding) or [CodeableConcept](https://hl7.org/fhir/R4/datatypes.html#CodeableConcept) values, returning a collection of Boolean values based on whether each concept is a member of the [ValueSet](https://hl7.org/fhir/R4/valueset.html) with the specified [url](https://hl7.org/fhir/R4/valueset-definitions.html#ValueSet.url).

For a `CodeableConcept`, the function will return `true` if any of the codings are members of the value set.

note

The `memberOf` function is a terminology function, which means that it requires a configured [terminology service](https://hl7.org/fhir/R4/terminology-service.html). See [Terminology functions](/docs/libraries/terminology.md) for details.

### subsumes[​](#subsumes "Direct link to subsumes")

```
collection<Coding|CodeableConcept> -> subsumes(code: Coding|CodeableConcept) : collection<Boolean>
```

This function takes a collection of [Coding](#coding) or [CodeableConcept](https://hl7.org/fhir/R4/datatypes.html#CodeableConcept) elements as input, and another collection as the argument. The result is a collection with a Boolean value for each source concept, each value being true if the concept subsumes any of the concepts within the argument collection, and false otherwise.

Example:

```
Patient.reverseResolve(Condition.subject).code.subsumes(http://snomed.info/sct|770581008)
```

note

The `subsumes` function is a terminology function, which means that it requires a configured [terminology service](https://hl7.org/fhir/R4/terminology-service.html). See [Terminology functions](/docs/libraries/terminology.md) for details.

### subsumedBy[​](#subsumedby "Direct link to subsumedBy")

```
collection<Coding|CodeableConcept> -> subsumedBy(code: Coding|CodeableConcept) : collection<Boolean>
```

The `subsumedBy` function is the inverse of the [subsumes](#subsumes) function, examining whether each input concept is *subsumed by* any of the argument concepts.

Example:

```
Patient.reverseResolve(Condition.subject).code.subsumedBy(http://snomed.info/sct|73211009)
```

note

The `subsumedBy` function is a terminology function, which means that it requires a configured [terminology service](https://hl7.org/fhir/R4/terminology-service.html). See [Terminology functions](/docs/libraries/terminology.md) for details.

## Extension functions[​](#extension-functions "Direct link to Extension functions")

The following functions have been implemented in Pathling in addition to the standard set of functions within the FHIRPath and FHIR specifications.

The notation used to describe the type signature of each function is as follows:

```
[input type] -> [function name]([argument name]: [argument type], ...): [return type]
```

### designation[​](#designation "Direct link to designation")

```
collection<Coding -> designation(use: Coding, language: String) : collection<String>
```

When invoked on a collection of [Coding](#coding) elements, returns a collection of designation values from the [lookup](https://www.hl7.org/fhir/codesystem-operation-lookup.html) operation. This can be used to retrieve synonyms, language translations and more from the underlying terminology.

If the `use` parameter is specified, designation values are filtered to only those with a matching use. If the `language` parameter is specified, designation values are filtered to only those with a matching language. If both are specified, designation values must match both the specified use and language.

See [Display, Definition and Designations](https://www.hl7.org/fhir/codesystem.html#designations) in the FHIR specification for more information.

Example:

```
// Retrieve SNOMED CT synonyms.
Condition.code.coding.designation(http://snomed.info/sct|900000000000013009)
```

note

The `designation` function is a terminology function, which means that it requires a configured [terminology service](https://hl7.org/fhir/R4/terminology-service.html). See [Terminology functions](/docs/libraries/terminology.md) for details.

note

The `designation` function is not within the FHIRPath specification, and is currently unique to the Pathling implementation.

### display[​](#display "Direct link to display")

```
collection<Coding> -> display(language?: String) : collection<String>
```

When invoked on a [Coding](#coding), returns the preferred display term, according to the terminology server.

The optional `language` parameter can be used to specify the preferred language for the display name. It overrides the default value set in the configuration. See [Multi-language support](/docs/libraries/terminology.md#multi-language-support) for details.

Example:

```
// With no argument
Condition.code.display()

// Prefer German language.
Condition.code.display("de")
```

note

The `display` function is a terminology function, which means that it requires a configured [terminology service](https://hl7.org/fhir/R4/terminology-service.html). See [Terminology functions](/docs/libraries/terminology.md) for details.

note

The `display` function is not within the FHIRPath specification, and is currently unique to the Pathling implementation.

### property[​](#property "Direct link to property")

```
collection<Coding> -> property(code: String, type: String = 'string', language?: String) : collection<String|Integer|DateTime|Decimal|Coding>
```

When invoked on a [Coding](#coding), returns any matching property values, using the specified `name` and `type` parameters.

The `type` parameter has these possible values:

* `string` (default)
* `code`
* `Coding`
* `integer`
* `boolean`
* `DateTime`

Both the `code` and the `type` of the property must be present within a [lookup](https://www.hl7.org/fhir/codesystem-operation-lookup.html) response in order for it to be returned by this function. If there are no matches, the function will return an empty collection.

The optional `language` parameter can be used to specify the preferred language for the returned property values. It overrides the default value set in the configuration. See [Multi-language support](/docs/libraries/terminology.md#multi-language-support) for details.

See [Properties](https://www.hl7.org/fhir/codesystem.html#properties) in the FHIR specification for more information.

Example:

```
// Select the code-typed property "parent".
Condition.code.coding.property('parent', 'code')

// Select the "parent" property, preferring the German language.
Condition.code.coding.property('parent', 'code', 'de')
```

note

The `property` function is a terminology function, which means that it requires a configured [terminology service](https://hl7.org/fhir/R4/terminology-service.html). See [Terminology functions](/docs/libraries/terminology.md) for details.

note

The `property` function is not within the FHIRPath specification, and is currently unique to the Pathling implementation.

### translate[​](#translate "Direct link to translate")

```
collection<Coding|CodeableConcept> -> translate(conceptMapUrl: String, reverse: Boolean = false, equivalence: String = 'equivalent', target?: String) : collection<Coding>
```

When invoked on a [Coding](#coding), returns any matching concepts using the ConceptMap specified using `conceptMapUrl`.

The `reverse` parameter controls the direction to traverse the map - `false` results in "source to target" mappings, while `true` results in "target to source".

The `equivalence` parameter is a comma-delimited set of values from the [ConceptMapEquivalence](https://www.hl7.org/fhir/R4/valueset-concept-map-equivalence.html) ValueSet, and is used to filter the mappings returned to only those that have an equivalence value in this list.

The `target` parameter identifies the value set in which a translation is sought — a scope for the translation.

Example:

```
Condition.code.coding.translate('https://csiro.au/fhir/ConceptMap/some-map', true, 'equivalent,wider').display
```

note

The `translate` function is a terminology function, which means that it requires a configured [terminology service](https://hl7.org/fhir/R4/terminology-service.html). See [Terminology functions](/docs/libraries/terminology.md) for details.

note

The `translate` function is not within the FHIRPath specification, and is currently unique to the Pathling implementation.

## Extension data types[​](#extension-data-types "Direct link to Extension data types")

Pathling implements the following additions in the area of data types.

### Coding[​](#coding "Direct link to Coding")

A [Coding](https://hl7.org/fhir/R4/datatypes.html#Coding) is a representation of a defined concept using a symbol from a defined [code system](https://hl7.org/fhir/R4/codesystem.html) - see [Using Codes in resources](https://hl7.org/fhir/R4/terminologies.html) for more details.

The Coding literal comprises a minimum of `system` and `code`, as well as optional `version`, `display`, `userSelected` components:

```
<system>|<code>[|<version>][|<display>[|<userSelected>]]]
```

Not all code systems require the use of a version to unambiguously specify a code - see [Versioning Code Systems](https://hl7.org/fhir/R4/codesystem.html#versioning).

You can also optionally single-quote each of the components within the Coding literal, in cases where certain characters might otherwise confuse the parser.

Examples:

```
http://snomed.info/sct|52101004
http://snomed.info/sct|52101004||Present
http://terminology.hl7.org/CodeSystem/condition-category|problem-list-item|4.0.1|'Problem List Item'
http://snomed.info/sct|'397956004 |Prosthetic arthroplasty of the hip|: 363704007 |Procedure site| = ( 24136001 |Hip joint structure|: 272741003 |Laterality| =  7771000 |Left| )'
```

note

The Coding literal is not within the FHIRPath specification, and is currently unique to the Pathling implementation.
