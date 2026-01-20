# FHIR-specific functions

The following functions are defined in the [FHIR specification](https://hl7.org/fhir/R4/fhirpath.html#functions) as additional FHIRPath functions for use with FHIR data.

The notation used to describe the type signature of each function is as follows:

```
[input type] -> [function name]([argument name]: [argument type], ...): [return type]
```

## extension[​](#extension "Direct link to extension")

```
collection<Element> -> extension(url: String) : collection<Extension>
```

Filters the input collection to only those elements that have an extension with the specified URL. This is a shortcut for `.extension.where(url = string)`.

Example:

```
Patient.extension('http://hl7.org/fhir/StructureDefinition/patient-birthPlace')
```

## resolve[​](#resolve "Direct link to resolve")

```
collection<Reference> -> resolve() : collection<Resource>
```

For each Reference in the input collection, returns the resource that the reference points to.

note

Pathling has a limited implementation of `resolve()`. It supports type checking with the `is` operator but does not perform actual resource resolution or allow traversal of resolved references.

## memberOf[​](#memberof "Direct link to memberOf")

```
collection<Coding|CodeableConcept> -> memberOf(valueSetUrl: String) : collection<Boolean>
```

The `memberOf` function can be invoked on a collection of [Coding](/docs/fhirpath/extension-functions.md#coding) or [CodeableConcept](https://hl7.org/fhir/R4/datatypes.html#CodeableConcept) values, returning a collection of Boolean values based on whether each concept is a member of the [ValueSet](https://hl7.org/fhir/R4/valueset.html) with the specified [url](https://hl7.org/fhir/R4/valueset-definitions.html#ValueSet.url).

For a `CodeableConcept`, the function will return `true` if any of the codings are members of the value set.

note

The `memberOf` function is a terminology function, which means that it requires a configured [terminology service](https://hl7.org/fhir/R4/terminology-service.html). See [Terminology functions](/docs/libraries/terminology.md) for details.

## subsumes[​](#subsumes "Direct link to subsumes")

```
collection<Coding|CodeableConcept> -> subsumes(code: Coding|CodeableConcept) : collection<Boolean>
```

This function takes a collection of [Coding](/docs/fhirpath/extension-functions.md#coding) or [CodeableConcept](https://hl7.org/fhir/R4/datatypes.html#CodeableConcept) elements as input, and another collection as the argument. The result is a collection with a Boolean value for each source concept, each value being true if the concept subsumes any of the concepts within the argument collection, and false otherwise.

Example:

```
Patient.reverseResolve(Condition.subject).code.subsumes(http://snomed.info/sct|770581008)
```

note

The `subsumes` function is a terminology function, which means that it requires a configured [terminology service](https://hl7.org/fhir/R4/terminology-service.html). See [Terminology functions](/docs/libraries/terminology.md) for details.

## subsumedBy[​](#subsumedby "Direct link to subsumedBy")

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
