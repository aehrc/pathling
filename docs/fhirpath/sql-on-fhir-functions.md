# SQL on FHIR functions

The following functions are defined in the [SQL on FHIR specification](https://sql-on-fhir.org/ig/2.0.0/StructureDefinition-ViewDefinition.html#required-additional-functions) as additional FHIRPath functions for use within views. They produce keys that can be used to join the rows of one view to the rows of another.

The notation used to describe the type signature of each function is as follows:

```
[input type] -> [function name]([argument name]: [argument type], ...): [return type]
```

## getResourceKey[​](#getresourcekey "Direct link to getResourceKey")

```
Resource -> getResourceKey() : String
```

Returns a key for the resource, in the form `[type]/[id]`, e.g. `Patient/123`. The version of the resource is not part of the key.

Example:

```
getResourceKey()
```

## getReferenceKey[​](#getreferencekey "Direct link to getReferenceKey")

```
collection<Reference> -> getReferenceKey(type?: TypeSpecifier) : collection<String>
```

For each Reference in the input collection, returns a key that is equal to the `getResourceKey()` value of the resource that it refers to.

If a type is given, only references to resources of exactly that type produce a key. For example, `getReferenceKey(Person)` returns an empty collection for a reference to `RelatedPerson/r1`.

The key is derived from `Reference.reference`:

| Reference                             | Key              | Joins to `getResourceKey()` |
| ------------------------------------- | ---------------- | --------------------------- |
| `Patient/123`                         | `Patient/123`    | Yes                         |
| `Patient/123/_history/2`              | `Patient/123`    | Yes                         |
| `http://example.org/fhir/Patient/123` | Empty collection | No                          |
| `#p1` (contained resource)            | Empty collection | No                          |
| `urn:uuid:9d0a6e7c-...`               | Empty collection | No                          |
| Not present (identifier only)         | Empty collection | No                          |

A [version-specific reference](https://hl7.org/fhir/R4/references.html#literal) joins to whichever version of the resource is present in the data. The version named in the reference is not checked.

Only relative references produce a key, as absolute, contained and URN references can never equal a resource key. A reference whose id or version contains characters outside the [FHIR id](https://hl7.org/fhir/R4/datatypes.html#id) character set (`A-Z`, `a-z`, `0-9`, `-` and `.`) also produces an empty collection.

Example:

```
subject.getReferenceKey(Patient)
```
