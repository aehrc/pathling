# Supported FHIRPath Functions and Operators

## Operators
- `and`
- `or`
- `xor`
- `implies`
- `=`
- `!=`
- `<`
- `<=`
- `>`
- `>=`
- `+`
- `-`
- `*`
- `/`
- `mod`
- `in`
- `contains`
- `[]`
- `combine`

## Functions

### Existence Functions
- `exists([criteria : expression]) : Boolean`
- `empty() : Boolean`
- `count() : Integer`
- `allTrue() : Boolean`
- `allFalse() : Boolean`
- `anyTrue() : Boolean`
- `anyFalse() : Boolean`

### Filtering and Projection Functions
- `where(criteria : expression) : collection`
- `select(projection: expression) : collection`
- `ofType(type : type specifier) : collection`

### Subsetting Functions
- `first() : collection`

### Conversion Functions
- `toString() : String`
- `iif(criterion: expression, true-result: collection [, otherwise-result: collection]) : collection`

### String Functions
- `join([separator: String]) : String`

### DateTime Functions
- `until(to, calendarDuration)`

### Boundary Functions
- `lowBoundary([precision: Integer]): Decimal | Date | DateTime | Time`
- `highBoundary([precision: Integer]): Decimal | Date | DateTime | Time`

### FHIR-specific Functions
- `extension(url)`

### Terminology Functions
- `display([language])`
- `property(code, [type], [language])`
- `designation([use], [language])`
- `memberOf(valueSetURL)`
- `subsumes(codes)`
- `subsumedBy(codes)`
- `translate(conceptMapUrl, [reverse], [equivalence], [target])`

### Reference Resolution Functions
- `resolve()`
- `reverseResolve(subjectPath)`

### Join Key Functions
- `getResourceKey()`
- `getReferenceKey([typeSpecifier])`

### Pathling Functions
- `sum()`
