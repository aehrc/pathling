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
collection<Coding|CodeableConcept> -> memberOf(valueSetUrl: String) : collection<Boolean>
```

The `memberOf` function can be invoked on a collection of
[Coding](/docs/fhirpath/extension-functions#coding) or
[CodeableConcept](https://hl7.org/fhir/R4/datatypes.html#CodeableConcept)
values, returning a collection of Boolean values
based on whether each concept is a member of the
[ValueSet](https://hl7.org/fhir/R4/valueset.html) with the specified
[url](https://hl7.org/fhir/R4/valueset-definitions.html#ValueSet.url).

For a `CodeableConcept`, the function will return `true` if any of
the codings are members of the value set.

:::note
The `memberOf` function is a terminology function, which means that it requires
a configured
[terminology service](https://hl7.org/fhir/R4/terminology-service.html). See
[Terminology functions](/docs/libraries/terminology) for details.
:::

## subsumes

```
collection<Coding|CodeableConcept> -> subsumes(code: Coding|CodeableConcept) : collection<Boolean>
```

This function takes a collection of [Coding](/docs/fhirpath/extension-functions#coding) or
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
[Terminology functions](/docs/libraries/terminology) for details.
:::

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
[Terminology functions](/docs/libraries/terminology) for details.
:::
