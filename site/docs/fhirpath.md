---
description: Pathling implements FHIRPath functionality to aid in querying and constructing views over FHIR data.
---

# FHIRPath

Pathling leverages the [FHIRPath](https://hl7.org/fhirpath/)
language in order to abstract away some of the complexity of navigating and
interacting with FHIR data structures.

Pathling implements the minimal FHIRPath subset within the [Sharable View Definition](https://sql-on-fhir.org/ig/latest/StructureDefinition-ShareableViewDefinition.html#required-fhirpath-expressionsfunctions) profile of the [SQL on FHIR view definition](https://sql-on-fhir.org/ig/latest/StructureDefinition-ViewDefinition.html).

## Functions

The following functions have been implemented in Pathling in addition to the
standard set of functions within the specification.

The notation used to describe the type signature of each function is as follows:

```
[input type] -> [function name]([argument name]: [argument type], ...): [return type]
```


### designation

```
collection<Coding -> designation(use: Coding, language: String) : collection<String>
```

When invoked on a collection of [Coding](./data-types#coding) elements, returns
a collection of designation values from
the [lookup](https://www.hl7.org/fhir/codesystem-operation-lookup.html)
operation. This can be used to retrieve synonyms, language translations and more
from the underlying terminology.

If the `use` parameter is specified, designation values are filtered to only
those with a matching use. If the `language` parameter is specified, designation
values are filtered to only those with a matching language. If both are
specified, designation values must match both the specified use and language.

See [Display, Definition and Designations](https://www.hl7.org/fhir/codesystem.html#designations)
in the FHIR specification for more information.

Example:

```
// Retrieve SNOMED CT synonyms.
Condition.code.coding.designation(http://snomed.info/sct|900000000000013009)
```

:::note
The `designation` function is a terminology function, which means that it
requires a
configured [terminology service](https://hl7.org/fhir/R4/terminology-service.html).
See [Configuration](/docs/server/configuration#terminology-service) for
details.
:::

:::note
The `designation` function is not within the FHIRPath specification, and is
currently unique to the Pathling implementation.
:::

### display

```
collection<Coding> -> display(language?: String) : collection<String>
```

When invoked on a [Coding](./data-types#coding), returns the preferred display
term, according to the terminology server.

The optional `language` parameter can be used to specify the preferred language
for the display name. It overrides the default value set in the configuration.
See `pathling.terminology.acceptLanguage` in
[Terminology Configuration](/docs/server/configuration#terminology-service)
for details.

Example:

```
// With no argument
Condition.code.display()

// Prefer German language.
Condition.code.display("de")
```
:::note
The `display` function is a terminology function, which means that it requires
a configured
[terminology service](https://hl7.org/fhir/R4/terminology-service.html). See
[Configuration](/docs/server/configuration#terminology-service) for details.
:::

:::note
The `display` function is not within the FHIRPath specification, and is
currently unique to the Pathling implementation.
:::

### memberOf

```
collection<Coding|CodeableConcept> -> memberOf() : collection<Boolean>
```

The `memberOf` function can be invoked on a collection of
[Coding](./data-types#coding) or
[CodeableConcept](https://hl7.org/fhir/R4/datatypes.html#CodeableConcept)
values, returning a collection of [Boolean](./data-types#boolean) values
based on whether each concept is a member of the
[ValueSet](https://hl7.org/fhir/R4/valueset.html) with the specified
[url](https://hl7.org/fhir/R4/valueset-definitions.html#ValueSet.url).

For a `CodeableConcept`, the function will return `true` if any of
the codings are members of the value set.

:::note
The `memberOf` function is a terminology function, which means that it requires
a configured
[terminology service](https://hl7.org/fhir/R4/terminology-service.html). See
[Configuration](/docs/server/configuration#terminology-service) for details.
:::

See also:
[Additional functions](https://hl7.org/fhir/R4/fhirpath.html#functions)

### property

```
collection<Coding> -> property(code: String, type: String = 'string', language?: String) : collection<String|Integer|DateTime|Decimal|Coding>
```

When invoked on a [Coding](./data-types#coding), returns any matching property
values, using the specified `name` and `type` parameters.

The `type` parameter has these possible values:

- `string` (default)
- `code`
- `Coding`
- `integer`
- `boolean`
- `DateTime`

Both the `code` and the `type` of the property must be present within a
[lookup](https://www.hl7.org/fhir/codesystem-operation-lookup.html) response in
order for it to be returned by this function. If there are no matches, the
function will return an empty collection.

The optional `language` parameter can be used to specify the preferred language
for the returned property values. It overrides the default value set in the
configuration. See `pathling.terminology.acceptLanguage` in
[Terminology Configuration](/docs/server/configuration#terminology-service)
for details.

See [Properties](https://www.hl7.org/fhir/codesystem.html#properties)
in the FHIR specification for more information.

Example:

```
// Select the code-typed property "parent".
Condition.code.coding.property('parent', 'code')

// Select the "parent" property, preferring the German language.
Condition.code.coding.property('parent', 'code', 'de')
```

:::note
The `property` function is a terminology function, which means that it requires
a configured
[terminology service](https://hl7.org/fhir/R4/terminology-service.html). See
[Configuration](/docs/server/configuration#terminology-service) for details.
:::

:::note
The `property` function is not within the FHIRPath specification, and is
currently unique to the Pathling implementation.
:::

### subsumes

```
collection<Coding|CodeableConcept> -> subsumes(code: Coding|CodeableConcept) : collection<Boolean>
```

This function takes a collection of [Coding](./data-types#coding) or
[CodeableConcept](https://hl7.org/fhir/R4/datatypes.html#CodeableConcept)
elements as input, and another collection as the argument. The result is a
collection with a Boolean value for each source concept, each value being true
if the concept subsumes any of the concepts within the argument collection, and
false otherwise.

Example:

```
Patient.reverseResolve(Condition.subject).code.subsumes(http://snomed.info/sct|770581008)
```

:::note
The `subsumes` function is a terminology function, which means that it requires
a configured
[terminology service](https://hl7.org/fhir/R4/terminology-service.html). See
[Configuration](/docs/server/configuration#terminology-service) for details.
:::

See also:
[Additional functions](https://hl7.org/fhir/R4/fhirpath.html#functions)

### subsumedBy

```
collection<Coding|CodeableConcept> -> subsumedBy(code: Coding|CodeableConcept) : collection<Boolean>
```

The `subsumedBy` function is the inverse of the [subsumes](#subsumes) function,
examining whether each input concept is _subsumed by_ any of the argument
concepts.

Example:

```
Patient.reverseResolve(Condition.subject).code.subsumedBy(http://snomed.info/sct|73211009)
```

:::note
The `subsumedBy` function is a terminology function, which means that it
requires a configured
[terminology service](https://hl7.org/fhir/R4/terminology-service.html). See
[Configuration](/docs/server/configuration#terminology-service) for details.
:::

See also:
[Additional functions](https://hl7.org/fhir/R4/fhirpath.html#functions)

### translate

```
collection<Coding|CodeableConcept> -> translate(conceptMapUrl: String, reverse: Boolean = false, equivalence: String = 'equivalent', target?: String) : collection<Coding>
```

When invoked on a [Coding](./data-types#coding), returns any
matching concepts using the ConceptMap specified using `conceptMapUrl`.

The `reverse` parameter controls the direction to traverse the map - `false`
results in "source to target" mappings, while `true` results in "target to
source".

The `equivalence` parameter is a comma-delimited set of values from
the [ConceptMapEquivalence](https://www.hl7.org/fhir/R4/valueset-concept-map-equivalence.html)
ValueSet, and is used to filter the mappings returned to only those that have an
equivalence value in this list.

The `target` parameter identifies the value set in which a translation is
sought &mdash; a scope for the translation.

Example:

```
Condition.code.coding.translate('https://csiro.au/fhir/ConceptMap/some-map', true, 'equivalent,wider').display
```

:::note
The `translate` function is a terminology function, which means that it requires
a configured
[terminology service](https://hl7.org/fhir/R4/terminology-service.html). See
[Configuration](/docs/server/configuration#terminology-service) for details.
:::

:::note
The `translate` function is not within the FHIRPath specification, and is
currently unique to the Pathling implementation.
:::

## Data types

Pathling implements the following additions in the area of data types.

### Coding

A [Coding](https://hl7.org/fhir/R4/datatypes.html#Coding) is a representation of
a defined concept using a symbol from a defined
[code system](https://hl7.org/fhir/R4/codesystem.html) - see
[Using Codes in resources](https://hl7.org/fhir/R4/terminologies.html) for more
details.

The Coding literal comprises a minimum of `system` and `code`, as well as
optional `version`, `display`, `userSelected` components:

```
<system>|<code>[|<version>][|<display>[|<userSelected>]]]
```

Not all code systems require the use of a version to unambiguously specify a
code - see
[Versioning Code Systems](https://hl7.org/fhir/R4/codesystem.html#versioning).

You can also optionally single-quote each of the components within the Coding
literal, in cases where certain characters might otherwise confuse the parser.

Examples:

```
http://snomed.info/sct|52101004
http://snomed.info/sct|52101004||Present
http://terminology.hl7.org/CodeSystem/condition-category|problem-list-item|4.0.1|'Problem List Item'
http://snomed.info/sct|'397956004 |Prosthetic arthroplasty of the hip|: 363704007 |Procedure site| = ( 24136001 |Hip joint structure|: 272741003 |Laterality| =  7771000 |Left| )'
```

:::note
The Coding literal is not within the FHIRPath specification, and is currently
unique to the Pathling implementation.
:::
