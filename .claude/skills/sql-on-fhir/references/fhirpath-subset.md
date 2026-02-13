# FHIRPath Subset Reference

SQL on FHIR uses a minimal FHIRPath subset for portability. This reference covers all supported expressions.

## Path Navigation

### Dot Notation

Navigate nested elements with dots:

```
name.family           # Patient's family name(s)
name.given            # Patient's given name(s)
address.city          # City from address(es)
code.coding.code      # Code value from CodeableConcept
```

### Indexing

Access specific elements by zero-based index:

```
name[0].family        # First name's family
name[1].given[0]      # Second name's first given
telecom[0].value      # First telecom value
```

Out-of-bounds indexing returns null.

### $this

Reference the current context element (used with forEach):

```
{
  "forEach": "name.given",
  "column": [{"name": "given", "path": "$this"}]
}
```

## Collection Functions

### first()

Return the first element of a collection:

```
name.first().family           # First name's family
identifier.first().value      # First identifier value
```

Returns null for empty collections.

### exists()

Return true if collection has any elements:

```
name.exists()                 # Has at least one name
deceased.exists()             # Deceased element is present
```

### empty()

Return true if collection has no elements:

```
identifier.empty()            # No identifiers
contact.empty()               # No contacts
```

### where(criteria)

Filter collection by boolean expression:

```
name.where(use = 'official')              # Names with official use
telecom.where(system = 'phone')           # Phone telecoms
identifier.where(system = 'http://...').value   # Specific identifier
```

Combine conditions:

```
name.where(use = 'official' and family.exists())
telecom.where(system = 'phone' or system = 'email')
```

### ofType(type)

Filter to specific FHIR type (polymorphic elements):

```
value.ofType(Quantity)        # Get value as Quantity
value.ofType(string)          # Get value as string
value.ofType(CodeableConcept) # Get value as CodeableConcept
deceased.ofType(boolean)      # Get deceased as boolean
deceased.ofType(dateTime)     # Get deceased as dateTime
```

### join(separator)

Concatenate collection elements into a single string:

```
name.given.join(' ')          # "John Paul"
address.line.join(', ')       # "123 Main St, Apt 4"
```

Without separator, elements are concatenated directly:

```
name.given.join()             # "JohnPaul"
```

## Extension Functions

### extension(url)

Access extensions by URL:

```
extension('http://hl7.org/fhir/StructureDefinition/patient-birthPlace')
```

Chain with value extraction:

```
extension('http://hl7.org/fhir/us/core/StructureDefinition/us-core-birthsex').value.ofType(code)
```

### Nested Extensions

Access sub-extensions:

```
extension('http://hl7.org/fhir/us/core/StructureDefinition/us-core-race')
  .extension('ombCategory')
  .value.ofType(Coding).code
```

## Reference Functions

### getResourceKey()

Get the resource's logical ID (indirection for portability):

```
getResourceKey()              # Returns resource ID
```

### getReferenceKey(type?)

Extract the target ID from a Reference:

```
subject.getReferenceKey()             # ID from any reference
subject.getReferenceKey(Patient)      # ID only if Patient reference
managingOrganization.getReferenceKey(Organization)
```

Returns null if type doesn't match.

## Comparison Operators

```
=       # Equals
!=      # Not equals
<       # Less than
<=      # Less than or equal
>       # Greater than
>=      # Greater than or equal
```

Examples:

```
gender = 'male'
value.ofType(integer) > 100
birthDate >= @2000-01-01
```

## Logical Operators

```
and     # Logical AND
or      # Logical OR
not     # Logical NOT (prefix)
```

Examples:

```
active = true and deceased.empty()
gender = 'male' or gender = 'female'
name.where(use = 'official').exists().not()
```

## Literals

### Strings

Single-quoted:

```
use = 'official'
system = 'http://loinc.org'
```

### Numbers

```
value.ofType(integer) > 100
component.where(value.ofType(Quantity).value < 140)
```

### Booleans

```
active = true
deceased.ofType(boolean) = false
```

### Dates

Use `@` prefix:

```
birthDate > @1990-01-01
period.start >= @2024-01-01T00:00:00Z
```

## Constant References

Reference constants defined in the ViewDefinition:

```
%constant_name
```

Example:

```json
{
    "constant": [{ "name": "loinc_bp", "valueString": "85354-9" }],
    "where": [
        {
            "path": "code.coding.where(system = 'http://loinc.org' and code = %loinc_bp).exists()"
        }
    ]
}
```

## Boundary Functions

### lowBoundary() / highBoundary()

Get boundaries of date/time ranges (implementation-specific support):

```
effectivePeriod.start.lowBoundary()
effectivePeriod.end.highBoundary()
```

## Unsupported Features

These FHIRPath features are NOT supported in ViewDefinitions:

- `iif()` conditional function
- `select()` projection (use ViewDefinition select instead)
- `repeat()` at path level (use ViewDefinition repeat instead)
- `aggregate()` function
- `trace()` debugging function
- Type casting with `as`
- Arithmetic operators (`+`, `-`, `*`, `/`)
- String concatenation (`&`)
- `matches()` regex function
- `contains()`, `startsWith()`, `endsWith()` (except via where)

Use the analytics layer (SQL) for complex transformations.
