# Extension functions

The following functions have been implemented in Pathling in addition to the standard set of functions within the FHIRPath and FHIR specifications.

The notation used to describe the type signature of each function is as follows:

```
[input type] -> [function name]([argument name]: [argument type], ...): [return type]
```

## designation[​](#designation "Direct link to designation")

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

## display[​](#display "Direct link to display")

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

## property[​](#property "Direct link to property")

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

## translate[​](#translate "Direct link to translate")

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
