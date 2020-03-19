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

- [resolve](#resolve)
- [reverseResolve](#reverseresolve)
- [ofType](#oftype)
- [count](#count)
- [first](#first)
- [empty](#empty)
- [where](#where)
- [memberOf](#memberof)
- [subsumes](#subsumes)
- [subsumedBy](#subsumedby)

The notation used to describe the type signature of each function is as follows:

```
[input type] -> [function name]([argument name]: [argument type], ...): [return type]
```

See also: [Functions](https://hl7.org/fhirpath/2018Sep/index.html#functions-2)

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
    Resolution of <code>uri</code> and <code>canonical</code> types (outside of <code>Reference.reference</code> is not 
    currently supported.
</div>

<div class="callout warning">
    Resolution of resources by identifier is not currently supported.
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
    The <code>reverseResolve</code> function is not within the FHIRPath specification, and is currently unique to the 
    Pathling implementation.
</div>

<div class="callout warning">
    Resolution of resources by identifier is not currently supported.
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
    Only resource types are currently supported by this function.
</div>

See also:
[ofType](https://hl7.org/fhirpath/2018Sep/index.html#oftypetype-identifier-collection)

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

<div class="callout info">
    The <code>count</code> function is an <em>aggregate function</em>, which means it can be used as the final 
    path component in contexts that require this, e.g. aggregation expressions within the 
    <a href="../aggregate.html">aggregate operation</a>.
</div>

See also: [count](https://hl7.org/fhirpath/2018Sep/index.html#count-integer)

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

<div class="callout info">
    The <code>first</code> function is an <em>aggregate function</em>, which means it can be used as the final 
    path component in contexts that require this, e.g. aggregation expressions within the 
    <a href="../aggregate.html">aggregate operation</a>.
</div>

See also: [first](https://hl7.org/fhirpath/2018Sep/index.html#first-collection)

## empty

```
collection -> empty() : Boolean
```

Returns `true` if the input collection is empty, and `false` otherwise.

Example:

```
Patient.reverseResolve(Condition.subject).empty()
```

See also: [empty](https://hl7.org/fhirpath/2018Sep/index.html#empty-boolean)

## where

```
collection -> where(criteria: expression) : collection
```

Returns a collection containing only those elements in the input collection for
which the `criteria` expression evaluates to `true`. Elements for which the
expression evaluates to `false` or an empty collection will not be included in
the result.

The `$this` keyword is used within the criteria expression to refer to the item
from the input collection currently under evaluation.

Example:

```
Patient.reverseResolve(Condition.subject).where($this.recordedDate > @1960).severity
```

See also:
[where](https://hl7.org/fhirpath/2018Sep/index.html#wherecriteria-expression-collection)

## memberOf

```
collection<Coding|CodeableConcept> -> memberOf() : collection<Boolean>
```

The `memberOf` function can be invoked on a collection of
[Coding](./data-types.html#coding) or
[CodeableConcept](https://hl7.org/fhir/R4/datatypes.html#CodeableConcept)
values, returning a collection of [Boolean](./data-types.html#boolean) values
based on whether the concept is a member of the
[ValueSet](https://hl7.org/fhir/R4/valueset.html) with the specified
[url](https://hl7.org/fhir/R4/valueset-definitions.html#ValueSet.url).

<div class="callout info">
    The <code>memberOf</code> function is a <em>terminology function</em>, which means that it requires a configured
    <a href="https://hl7.org/fhir/R4/terminology-service.html">terminology service</a>. See 
    <a href="../deployment.html">Configuration and deployment</a> for details.
</div>

See also:
[Additional functions](https://hl7.org/fhir/R4/fhirpath.html#functions)

## subsumes

```
collection<Coding|CodeableConcept> -> subsumes(code: Coding|CodeableConcept) : collection<Boolean>
```

When invoked on a [Coding](./data-types.html#coding)-valued element and the
given code is Coding-valued, returns true if the source code is equivalent to
the given code, or if the source code subsumes the given code (i.e. the source
code is an ancestor of the given code in a subsumption hierarchy), and false
otherwise.

If the Codings are from different code systems, the relationships between the
code systems must be well-defined or a run-time error is thrown.

When the source or given elements are
[CodeableConcepts](https://hl7.org/fhir/R4/datatypes.html#CodeableConcept),
returns true if any Coding in the source or given elements is equivalent to or
subsumes the given code.

Example:

```
Patient.reverseResolve(Condition.subject).code.subsumes(http://snomed.info/sct|770581008)
```

<div class="callout info">
    The <code>subsumes</code> function is a <em>terminology function</em>, which means that it requires a configured
    <a href="https://hl7.org/fhir/R4/terminology-service.html">terminology service</a>. See 
    <a href="../deployment.html">Configuration and deployment</a> for details.
</div>

See also:
[Additional functions](https://hl7.org/fhir/R4/fhirpath.html#functions)

## subsumedBy

```
collection<Coding|CodeableConcept> -> subsumedBy(code: Coding|CodeableConcept) : collection<Boolean>
```

When invoked on a [Coding](./data-types.html#coding)-valued element and the
given code is Coding-valued, returns true if the source code is equivalent to
the given code, or if the source code is subsumed by the given code (i.e. the
given code is an ancestor of the source code in a subsumption hierarchy), and
false otherwise.

If the Codings are from different code systems, the relationships between the
code systems must be well-defined or a run-time error is thrown.

When the source or given elements are
[CodeableConcepts](https://hl7.org/fhir/R4/datatypes.html#CodeableConcept),
returns true if any Coding in the source or given elements is equivalent to or
subsumed by the given code.

Example:

```
Patient.reverseResolve(Condition.subject).code.subsumedBy(http://snomed.info/sct|73211009)
```

<div class="callout info">
    The <code>subsumes</code> function is a <em>terminology function</em>, which means that it requires a configured
    <a href="https://hl7.org/fhir/R4/terminology-service.html">terminology service</a>. See 
    <a href="../deployment.html">Configuration and deployment</a> for details.
</div>

See also:
[Additional functions](https://hl7.org/fhir/R4/fhirpath.html#functions)

Next: [Configuration and deployment](../deployment.html)
