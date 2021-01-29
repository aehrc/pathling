---
layout: page
title: Functions
nav_order: 2
parent: FHIRPath
grand_parent: Documentation
---

# Functions

FHIRPath supports the notion of functions, which all take a collection of values
as input and produce another collection as output and may take parameters.

The following functions are currently supported:

- [count](#count)
- [first](#first)
- [empty](#empty)
- [not](#not)
- [where](#where)
- [memberOf](#memberof)
- [subsumes](#subsumes)
- [subsumedBy](#subsumedby)
- [resolve](#resolve)
- [reverseResolve](#reverseresolve)
- [ofType](#oftype)

The notation used to describe the type signature of each function is as follows:

```
[input type] -> [function name]([argument name]: [argument type], ...): [return type]
```

See also: [Functions](https://hl7.org/fhirpath/#functions-2)

## count

```
collection -> count() : Integer
```

Returns the [Integer](./data-types.html#integer) count of the number of items in
the input collection.

Example:

```
Patient.name.given.count()
```

See also: [count](https://hl7.org/fhirpath/#count-integer)

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

## where

```
collection -> where(criteria: expression) : collection
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

## memberOf

```
collection<Coding|CodeableConcept> -> memberOf() : collection<Boolean>
```

The `memberOf` function can be invoked on a collection of
[Coding](./data-types.html#coding) or
[CodeableConcept](https://hl7.org/fhir/R4/datatypes.html#CodeableConcept)
values, returning a collection of [Boolean](./data-types.html#boolean) values
based on whether each concept is a member of the
[ValueSet](https://hl7.org/fhir/R4/valueset.html) with the specified
[url](https://hl7.org/fhir/R4/valueset-definitions.html#ValueSet.url).

<div class="callout info">
    The <code>memberOf</code> function is a <em>terminology function</em>, which means that it requires a configured
    <a href="https://hl7.org/fhir/R4/terminology-service.html">terminology service</a>. See 
    <a href="../configuration.html#terminology-service">Configuration and deployment</a> for details.
</div>

See also:
[Additional functions](https://hl7.org/fhir/R4/fhirpath.html#functions)

## subsumes

```
collection<Coding|CodeableConcept> -> subsumes(code: Coding|CodeableConcept) : collection<Boolean>
```

This function takes a collection of [Coding](./data-types.html#coding) or 
[CodeableConcept](https://hl7.org/fhir/R4/datatypes.html#CodeableConcept) 
elements as input, and another collection as the argument. The result is a 
collection with a Boolean value for each source concept, each value being true 
if the concept subsumes any of the concepts within the argument collection, and 
false otherwise.

Example:

```
Patient.reverseResolve(Condition.subject).code.subsumes(http://snomed.info/sct|770581008)
```

<div class="callout info">
    The <code>subsumes</code> function is a <em>terminology function</em>, which means that it requires a configured
    <a href="https://hl7.org/fhir/R4/terminology-service.html">terminology service</a>. See 
    <a href="../configuration.html#terminology-service">Configuration and deployment</a> for details.
</div>

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

<div class="callout info">
    The <code>subsumedBy</code> function is a <em>terminology function</em>, which means that it requires a configured
    <a href="https://hl7.org/fhir/R4/terminology-service.html">terminology service</a>. See 
    <a href="../configuration.html#terminology-service">Configuration and deployment</a> for details.
</div>

See also:
[Additional functions](https://hl7.org/fhir/R4/fhirpath.html#functions)

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

<div class="callout warning">
    The following types of references are not currently supported:
    <ol>
      <li>References to individual technical versions of a resource</li>
      <li>Logical references (via <code>identifier</code>)</li>
      <li>References to contained resources</li>
      <li>Absolute literal references</li>
    </ol>
</div>

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

<div class="callout warning">
    The <code>reverseResolve</code> function is not within the FHIRPath 
    specification, and is currently unique to the Pathling implementation.
</div>

<div class="callout warning">
    The same caveats apply with regards to types of references supported as 
    described in the <a href="#resolve">resolve</a> function.
</div>

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

<div class="callout warning">
    This function is currently only supported for use with the 
    <a href="#resolve">resolve</a> function for the purpose of disambiguating 
    polymorphic resource references.
</div>

See also:
[ofType](https://hl7.org/fhirpath/#oftypetype-identifier-collection)

Next: [Configuration](../configuration.html)
