---
sidebar_position: 3
description: Functions defined in the SQL on FHIR specification for producing keys that join the rows of one view to another.
---

# SQL on FHIR functions

The following functions are defined in
the [SQL on FHIR specification](https://sql-on-fhir.org/ig/2.0.0/StructureDefinition-ViewDefinition.html#required-additional-functions)
as additional FHIRPath functions for use within views. They produce keys that
can be used to join the rows of one view to the rows of another.

The notation used to describe the type signature of each function is as follows:

```
[input type] -> [function name]([argument name]: [argument type], ...): [return type]
```

## getResourceKey

```
Resource -> getResourceKey() : String
```

Returns a key for the resource, in the form `[type]/[id]`, e.g. `Patient/123`.
The version of the resource is not part of the key.

Example:

```
getResourceKey()
```

## getReferenceKey

```
collection<Reference> -> getReferenceKey(type?: TypeSpecifier) : collection<String>
```

For each Reference in the input collection, returns a key that is equal to the
`getResourceKey()` value of the resource that it refers to.

If a type is given, only references to resources of exactly that type produce a
key. For example, `getReferenceKey(Person)` returns an empty collection for a
reference to `RelatedPerson/r1`.

The key is derived from `Reference.reference`:

| Reference                             | Key                                   | Joins to `getResourceKey()` |
| ------------------------------------- | ------------------------------------- | --------------------------- |
| `Patient/123`                         | `Patient/123`                         | Yes                         |
| `Patient/123/_history/2`              | `Patient/123`                         | Yes                         |
| `http://example.org/fhir/Patient/123` | `http://example.org/fhir/Patient/123` | No                          |
| `#p1` (contained resource)            | `#p1`                                 | No                          |
| `urn:uuid:9d0a6e7c-...`               | `urn:uuid:9d0a6e7c-...`               | No                          |
| Not present (identifier only)         | Empty collection                      | No                          |

A [version-specific reference](https://hl7.org/fhir/R4/references.html#literal)
joins to whichever version of the resource is present in the data. The version
named in the reference is not checked.

Absolute, contained and URN references produce a key that never equals a
resource key. A future major release will return an empty collection for these
instead (see [#2808](https://github.com/aehrc/pathling/issues/2808)).

Example:

```
subject.getReferenceKey(Patient)
```
