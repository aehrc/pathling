---
sidebar_position: 4
---

# Functions

FHIRPath supports the notion of functions, which all take a collection of values
as input and produce another collection as output and may take parameters.

The notation used to describe the type signature of each function is as follows:

```
[input type] -> [function name]([argument name]: [argument type], ...): [return type]
```

## allFalse

```
collection -> allFalse() : Boolean
```

Takes a collection of Boolean values and returns `true` if all the items are
`false`. If any items are `true`, the result is `false`. If the input is empty (
`{ }`), the result is `true`.

Example:

```
Condition.code.memberOf('http://snomed.info/sct?fhir_vs=refset/32570581000036105').allFalse()
```

See also: [allFalse](https://hl7.org/fhirpath/#allfalse-boolean)

## allTrue

```
collection -> allTrue() : Boolean
```

Takes a collection of Boolean values and returns `true` if all the items are
`true`. If any items are `false`, the result is `false`. If the input is empty (
`{ }`), the result is `true`.

Example:

```
Condition.code.memberOf('http://snomed.info/sct?fhir_vs=refset/32570581000036105').allTrue()
```

See also: [allTrue](https://hl7.org/fhirpath/#alltrue-boolean)

## anyFalse

```
collection -> anyFalse() : Boolean
```

Takes a collection of Boolean values and returns `true` if any of the items are
`false`. If all the items are `true`, or if the input is empty (`{ }`), the
result is `false`.

Example:

```
Condition.code.memberOf('http://snomed.info/sct?fhir_vs=refset/32570581000036105').anyFalse()
```

See also: [anyFalse](https://hl7.org/fhirpath/#anyfalse-boolean)

## anyTrue

```
collection -> anyTrue() : Boolean
```

Takes a collection of Boolean values and returns `true` if any of the items are
`true`. If all the items are `false`, or if the input is empty (`{ }`), the
result is `false`.

Example:

```
Condition.code.memberOf('http://snomed.info/sct?fhir_vs=refset/32570581000036105').anyTrue()
```

See also: [anyTrue](https://hl7.org/fhirpath/#anytrue-boolean)

## count

```
collection -> count() : Integer
```

Returns the [Integer](./data-types#integer) count of the number of items in
the input collection.

Example:

```
Patient.name.given.count()
```

See also: [count](https://hl7.org/fhirpath/#count-integer)

## designation

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
See [Configuration](/docs/7.2.0/server/configuration#terminology-service) for
details.
:::

:::note
The `designation` function is not within the FHIRPath specification, and is
currently unique to the Pathling implementation.
:::

## display

```
collection<Coding> -> display(language?: String) : collection<String>
```

When invoked on a [Coding](./data-types#coding), returns the preferred display
term, according to the terminology server.

The optional `language` parameter can be used to specify the preferred language
for the display name. It overrides the default value set in the configuration.
See `pathling.terminology.acceptLanguage` in
[Terminology Configuration](/docs/7.2.0/server/configuration#terminology-service)
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
[Configuration](/docs/7.2.0/server/configuration#terminology-service) for
details.
:::

:::note
The `display` function is not within the FHIRPath specification, and is
currently unique to the Pathling implementation.
:::

## empty

```
collection -> empty() : Boolean
```

Returns `true` if the input collection is empty, and `false` otherwise.

Example:

```
Patient.reverseResolve(Condition.subject).empty()
```

See also: [empty](https://hl7.org/fhirpath/#empty-boolean)

## exists

```
collection -> exists() : Boolean
collection -> exists(criteria: [any]) : Boolean
```

Tests whether the input collection is empty. When invoked without arguments,
`exists()` is equivalent to `empty().not()`.

This function can also optionally accept an argument which can filter the input
collection prior to applying the test. When invoked with an
argument, `exists([criteria])` is equivalent to `where([criteria]).exists()`.

See also: [exists](https://hl7.org/fhirpath/#existscriteria-expression-boolean)

## extension

```
[any] -> extension(url: String) : collection
```

Will filter the input collection for items named `extension` with the given url.
This is a syntactical shortcut for `.extension.where(url = [String])`, but is
simpler to write. Will return an empty collection if the input collection is
empty or the url is empty.

:::tip
Your extension content will only be encoded upon import if your encoding
configuration has specified that it should be. Data types and maximum depth of
encoding are both configurable.
See [Configuration](/docs/7.2.0/server/configuration#encoding) for more
information.
:::

See
also: [Additional functions](https://hl7.org/fhir/R4/fhirpath.html#functions)

## first

```
collection -> first() : collection
```

Returns a collection containing only the first item in the input collection.
This function will return an empty collection if the input collection has no
items.

Example:

```
Patient.name.given.first()
```

See also: [first](https://hl7.org/fhirpath/#first-collection)

## iif

```
[any] -> iif(condition: Boolean, ifTrue: [any], otherwise: [any]) : [any]
```

Takes three arguments, the first of which is a Boolean expression. Returns the
second argument if the first argument evaluates to `true`, or the third argument
otherwise.

The `ifTrue` and `otherwise` arguments must be of the same type.

Example:

```
Patient.name.family.iif(empty(), 'Doe', $this)
```

See also:
[iif](http://hl7.org/fhirpath/#iifcriterion-expression-true-result-collection-otherwise-result-collection-collection)

## memberOf

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
[Configuration](/docs/7.2.0/server/configuration#terminology-service) for
details.
:::

See also:
[Additional functions](https://hl7.org/fhir/R4/fhirpath.html#functions)

## not

```
Boolean -> not() : Boolean
```

Returns `true` if the input collection evaluates to `false`, and `false` if it
evaluates to `true`. Otherwise, the result is empty (`{ }`).

Example:

```
(Patient.name.given contains 'Frank').not()
```

See also: [not](http://hl7.org/fhirpath/#not-boolean)

## ofType

```
collection -> ofType(type: Resource): collection
```

Returns a collection that contains all items in the input collection that are of
the given type. It is often necessary to use the `ofType` function in
conjunction with the `resolve` function, to resolve references that are
polymorphic.

Example:

```
Condition.subject.resolve().ofType(Patient).gender
```

:::caution
This function is currently only supported for use with the [resolve](#resolve)
function, for the purpose of disambiguating polymorphic resource references.
:::

See also: [ofType](https://hl7.org/fhirpath/#oftypetype-identifier-collection)

## property

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
[Terminology Configuration](/docs/7.2.0/server/configuration#terminology-service)
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
[Configuration](/docs/7.2.0/server/configuration#terminology-service) for
details.
:::

:::note
The `property` function is not within the FHIRPath specification, and is
currently unique to the Pathling implementation.
:::

## resolve

```
Reference -> resolve(): collection<Resource>
```

The `resolve` function is used to traverse references between FHIR resources.
Given a collection of
[References](https://hl7.org/fhir/R4/references.html#Reference), this function
will return a collection of the resources to which they refer.

Example:

```
AllergyIntolerance.patient.resolve().gender
```

:::caution
The following types of references are not currently supported:

- References to individual technical versions of a resource
- Logical references (via <code>identifier</code>)
- References to contained resources
- Absolute literal references
  :::

See also:
[Additional functions](https://hl7.org/fhir/R4/fhirpath.html#functions)

## reverseResolve

```
collection<Resource> -> reverseResolve(sourceReference: Reference): collection<Resource>
```

In FHIR, resource references are unidirectional, and often the source of the
reference will be a resource type which is not the subject of the current path.

The `reverseResolve` function takes a collection of Resources as input, and a
[Reference](https://hl7.org/fhir/R4/references.html#Reference) as the argument.
It returns a collection of all the parent resources of the source References
that resolve to the input resource.

Example:

```
Patient.reverseResolve(Encounter.subject).reasonCode
```

:::note
The `reverseResolve` function is not within the FHIRPath specification, and is
currently unique to the Pathling implementation.
:::

:::caution
In regard to types of references supported, the same caveats apply as those
described in the [resolve](#resolve) function.
:::

## subsumes

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
[Configuration](/docs/7.2.0/server/configuration#terminology-service) for
details.
:::

See also:
[Additional functions](https://hl7.org/fhir/R4/fhirpath.html#functions)

## subsumedBy

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
[Configuration](/docs/7.2.0/server/configuration#terminology-service) for
details.
:::

See also:
[Additional functions](https://hl7.org/fhir/R4/fhirpath.html#functions)

## sum

```
collection -> sum() : Integer|Decimal
```

Returns the sum of the numeric values ([Integer](./data-types#integer) or
[Decimal](./data-types#decimal)) in the input collection.

Example:

```
Observation.valueDecimal.sum()
```

:::note
The `sum` function is not within the FHIRPath specification, and is currently
unique to the Pathling implementation.
:::

## translate

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
[Configuration](/docs/7.2.0/server/configuration#terminology-service) for
details.
:::

:::note
The `translate` function is not within the FHIRPath specification, and is
currently unique to the Pathling implementation.
:::

## until

```
DateTime|Date -> until(end: DateTime|Date, unit: String) : Integer
```

This function is used to calculate the difference between two `Date`
or `DateTime` values. The result is returned as an integer, with the unit of
time specified by the `unit` argument.

Example:

```
period.start.until(%resource.period.end, 'minutes')
```

:::note
The `until` function is not within the FHIRPath specification, and is
currently unique to the Pathling implementation.
:::

## where

```
collection -> where(criteria: [any]) : collection
```

Returns a collection containing only those elements in the input collection for
which the `criteria` expression evaluates to `true`. Elements for which the
expression evaluates to `false` or an empty collection will return an empty
collection.

The `$this` keyword can be used within the criteria expression to refer to the
item from the input collection currently under evaluation. The context inside
the arguments is also set to the current item, so paths from the root are
assumed to be path traversals from the current element.

Example:

```
Patient.reverseResolve(Condition.subject).where(recordedDate > @1960).severity
```

See also:
[where](https://hl7.org/fhirpath/#wherecriteria-expression-collection)
