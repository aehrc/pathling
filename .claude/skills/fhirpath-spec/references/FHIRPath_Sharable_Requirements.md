# FHIRPath Requirements for ShareableViewDefinition

**References:**
- [ViewDefinition](https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-ViewDefinition.html)
- [ShareableViewDefinition](https://build.fhir.org/ig/FHIR/sql-on-fhir-v2/StructureDefinition-ShareableViewDefinition.html)

## Required FHIRPath Capabilities

### Literals
- String
- Integer
- Decimal

### Functions
- `where()` - filtering
- `exists()` - existence check
- `empty()` - empty collection check
- `extension()` - extension access
- `ofType()` - type filtering
- `first()` - first element

### Operators
- **Boolean**: `and`, `or`, `not`
- **Math**: `+`, `-`, `*`, `/`
- **Comparison**: `=`, `!=`, `>`, `<=`
- **Indexer expressions**: `collection[index]`

### Required Additional Functions (SQL on FHIR)
- `getResourceKey()` - returns primary key for resource row
- `getReferenceKey([type])` - returns foreign key for referenced resource
  - MUST support relative literal references (e.g., `Patient/123`)
  - MAY support other reference types
  - Returns empty collection `{}` if unsupported/unresolvable

## Experimental Functions

Intended for eventual inclusion (not yet normative):
- `join()` - collection joining
- `lowBoundary()`, `highBoundary()` - boundary functions (including on Period)
