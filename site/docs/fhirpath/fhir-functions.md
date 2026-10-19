---
sidebar_position: 2
description: FHIR-specific functions defined in the FHIR specification as additional FHIRPath functions for use with FHIR data.
---

# FHIR-specific functions

The following functions are defined in
the [FHIR specification](https://hl7.org/fhir/R4/fhirpath.html#functions) as
additional FHIRPath functions for use with FHIR data.

The notation used to describe the type signature of each function is as follows:

```
[input type] -> [function name]([argument name]: [argument type], ...): [return type]
```

## extension

```
collection<Element> -> extension(url: String) : collection<Extension>
```

Filters the input collection to only those elements that have an extension with
the specified URL. This is a shortcut for `.extension.where(url = string)`.

Example:

```
Patient.extension('http://hl7.org/fhir/StructureDefinition/patient-birthPlace')
```

## resolve

```
collection<Reference> -> resolve() : collection<Resource>
```

For each Reference in the input collection, returns type information about the
resource that the reference points to.

### How it works

The `resolve()` function extracts resource type information from Reference
elements using the following priority:

1. Uses the `Reference.type` field if present (highest priority)
2. Parses the type from the `Reference.reference` string (e.g., `Patient/123` →
   Patient type)
3. Returns an empty collection when the type cannot be determined

This supports relative references (`Patient/123`), absolute references
(`http://example.org/fhir/Patient/123`), and canonical URLs.

### Usage with type operators

The primary use case for `resolve()` is type checking and filtering with the
`is` and `ofType()` operators:

```
Encounter.subject.resolve() is Patient
Patient.generalPractitioner.resolve().ofType(Practitioner)
Patient.generalPractitioner.where(resolve() is Organization)
```

### Limitations

The `resolve()` function in Pathling extracts type information only and does
**not** support field traversal. Attempting to access fields on a resolved
reference (e.g., `resolve().name`) will result in an error.

For accessing data from referenced resources, use the `reverseResolve()`
function instead. See [Extension functions](extension-functions.md) for details.

## memberOf

```
Coding|CodeableConcept|code|string|Quantity -> memberOf(valueSetUrl: String) : Boolean
```

The `memberOf` function is implemented as
[defined by FHIR](https://hl7.org/fhir/fhirpath.html#functions). It returns
whether a single concept is a member of the
[ValueSet](https://hl7.org/fhir/R4/valueset.html) with the specified
[url](https://hl7.org/fhir/R4/valueset-definitions.html#ValueSet.url).

The input may be a [Coding](/docs/fhirpath/extension-functions#coding), a
[CodeableConcept](https://hl7.org/fhir/R4/datatypes.html#CodeableConcept), a
`code` or a `string`:

- A `CodeableConcept` is a member if any of its codings is a member.
- A `code` or `string` is a code without a system. The system is inferred from
  the value set, using the `inferSystem` parameter of
  [$validate-code](https://build.fhir.org/valueset-operation-validate-code.html).
  A terminology server that does not support this parameter cannot answer the
  question, and the result is empty. Where the value set contains more than one
  code system, FHIR defines the result as empty, but the terminology server
  decides: Ontoserver, for example, returns `true` for a code found in exactly
  one of the code systems and `false` otherwise. In
  [local mode](/docs/libraries/terminology/local), a value set is evaluated over
  a single code system, and the code takes that system.
- As an extension to the FHIR definition, a `Quantity` is also accepted, in which
  case the concept is the coded unit. See
  [Quantity as a concept](#quantity-as-a-concept).

The result is empty where:

- the input is empty or has more than one item,
- the value set cannot be resolved, or
- the terminology server rejects the request.

A Coding without a `system` or `code` is not a member of any value set, so the
result for it is `false`.

Because the result is empty for more than one item,
`code.coding.memberOf(...)` is empty for any resource whose `code` carries more
than one coding. To ask whether any coding is a member, pass the
`CodeableConcept` itself, or filter the codings:

```
Observation.code.memberOf('http://hl7.org/fhir/ValueSet/observation-vitalsignresult')
Observation.code.coding.where(memberOf('http://hl7.org/fhir/ValueSet/observation-vitalsignresult')).exists()
```

:::note
The `memberOf` function is a terminology function, which means that it requires
a configured
[terminology service](https://hl7.org/fhir/R4/terminology-service.html). See
[Terminology functions](/docs/libraries/terminology) for details.
:::

## subsumes

```
collection<Coding|CodeableConcept|Quantity> -> subsumes(code: Coding|CodeableConcept|Quantity) : collection<Boolean>
```

This function takes a collection of [Coding](/docs/fhirpath/extension-functions#coding),
[CodeableConcept](https://hl7.org/fhir/R4/datatypes.html#CodeableConcept) or
[Quantity](https://hl7.org/fhir/R4/datatypes.html#Quantity)
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
[Terminology functions](/docs/libraries/terminology) for details.
:::

## subsumedBy

```
collection<Coding|CodeableConcept|Quantity> -> subsumedBy(code: Coding|CodeableConcept|Quantity) : collection<Boolean>
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
[Terminology functions](/docs/libraries/terminology) for details.
:::

## Quantity as a concept

The terminology functions that operate upon concepts &mdash; `memberOf`,
`subsumes`, `subsumedBy` and
[translate](extension-functions.md#translate) &mdash; can also be invoked on a
[Quantity](https://hl7.org/fhir/R4/datatypes.html#Quantity). The concept is the
coded unit of the quantity, taken from its `system` and `code`, with the `unit`
as the display. The value of the quantity plays no part.

This allows units of measure to be checked against a value set, or mapped to
another set of unit codes:

```
Observation.value.ofType(Quantity).translate('https://csiro.au/fhir/ConceptMap/units')
```

The original unit code is used, rather than the canonical form that Pathling
derives for the purposes of comparison, as a ConceptMap or ValueSet is authored
against the codes that appear within the data.

A quantity that carries only a free-text `unit`, without a `system` and `code`,
does not identify a concept. Such a quantity yields `false` from `memberOf`,
`subsumes` and `subsumedBy`, and an empty result from `translate` &mdash; the
same behaviour as a Coding without a system and code.
