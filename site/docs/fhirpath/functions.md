---
layout: page
title: Functions
nav_order: 3
parent: FHIRPath
---

# Functions

Source:
[https://hl7.org/fhirpath/2018Sep/index.html#functions-2](https://hl7.org/fhirpath/2018Sep/index.html#functions-2)

FHIRPath supports the notion of functions, which all take a collection of values
as input and produce another collection as output and may take parameters.

The following functions are currently supported by Pathling:

- [resolve](#resolve)
- [reverseResolve](#reverseresolve)
- [ofType](#oftype)
- [count](#count)
- [first](#first)
- [memberOf](#memberof)

The notation used to describe the type signature of each function is as follows:

```
[input type] -> [function name]([argument name]: [argument type], ...): [return type]
```

<div class="callout warning">
    Functions described within the FHIRPath specification that are not covered on this page are not currently supported 
    within Pathling.
</div>

## resolve

Source:
[https://hl7.org/fhir/fhirpath.html#functions](https://hl7.org/fhir/fhirpath.html#functions)

```
Reference -> resolve(): collection<Resource>
```

The `resolve` function is used to traverse references between FHIR resources.
Given a collection of References, this function will return a collection of the
resources to which they refer.

Example:

```
AllergyIntolerance.patient.resolve().gender
```

<div class="callout warning">
    Resolution of <code>uri</code> and <code>canonical</code> types (outside of <code>Reference.reference</code> is not 
    currently supported.
</div>

<div class="callout warning">
    Resolution of resources by identifier is not currently supported.
</div>

## reverseResolve

```
Resource -> reverseResolve(sourceReference: Reference): collection<Resource>
```

In FHIR, resource references are unidirectional, and often the source of the
reference will be a resource type which is not the subject of the current path.
The `reverseResolve` function takes a resource as input, and a Reference as the
argument. It returns a collection of all the parent resources of the
`sourceReference`s that resolve to the input resource.

Example:

```
Patient.reverseResolve(Encounter.subject).reasonCode
```

<div class="callout warning">
    The <code>reverseResolve</code> function is not within the FHIRPath specification, and is currently unique to the 
    Pathling implementation.
</div>

<div class="callout warning">
    Resolution of resources by identifier is not currently supported.
</div>

## ofType

Source:
[https://hl7.org/fhirpath/2018Sep/index.html#oftypetype-identifier-collection](https://hl7.org/fhirpath/2018Sep/index.html#oftypetype-identifier-collection)

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
    Only resource types are currently supported by this function.
</div>

## count

Source:
[https://hl7.org/fhirpath/2018Sep/index.html#count-integer](https://hl7.org/fhirpath/2018Sep/index.html#count-integer)

```
collection -> count() : Integer
```

Returns a collection with a single value which is the integer count of the
number of items in the input collection. Returns 0 when the input collection is
empty.

Example:

```
Patient.name.given.count()
```

<div class="callout info">
    The <code>count</code> function is an <em>aggregate function</em>, which means it can be used as the final 
    path component in contexts that require this, e.g. aggregation expressions within the 
    <a href="../aggregate.html">aggregate operation</a>.
</div>

## first

Source:
[https://hl7.org/fhirpath/2018Sep/index.html#first-collection](https://hl7.org/fhirpath/2018Sep/index.html#first-collection)

```
collection -> first() : collection
```

Returns a collection with a single value which is the integer count of the
number of items in the input collection. Returns 0 when the input collection is
empty.

Example:

```
Patient.name.given.count()
```

<div class="callout info">
    The <code>first</code> function is an <em>aggregate function</em>, which means it can be used as the final 
    path component in contexts that require this, e.g. aggregation expressions within the 
    <a href="../aggregate.html">aggregate operation</a>.
</div>

## memberOf

Source:
[https://hl7.org/fhir/fhirpath.html#functions](https://hl7.org/fhir/fhirpath.html#functions)

```
collection<Coding|CodeableConcept> -> memberOf() : collection<Boolean>
```

The `memberOf` function can be invoked on a collection of `Coding` or
`CodeableConcept` values, returning a collection of Boolean values based on
whether the concept is a member of the ValueSet with the specified `url`.

<div class="callout info">
    The <code>memberOf</code> function is an <em>terminology function</em>, which means that it requires a configured
    terminology service. See <a href="../deployment.html">Configuration and deployment</a> for details.
</div>
