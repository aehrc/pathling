# Supported FHIRPath Functions

This document lists all the FHIRPath functions implemented in Pathling.

## Boolean Logic Functions

- `not()` - Returns `true` if the input collection evaluates to `false`, and `false` if it evaluates to `true`.

## Existence Functions

- `exists([criteria])` - Returns `true` if the input collection has any elements (optionally filtered by criteria), and `false` otherwise.
- `empty()` - Returns `true` if the input collection is empty and `false` otherwise.
- `count()` - Returns the integer count of the number of items in the input collection.
- `sum()` - Returns the sum of the numbers in the input collection.
- `allTrue()` - Takes a collection of Boolean values and returns `true` if all the items are `true`.
- `allFalse()` - Takes a collection of Boolean values and returns `true` if all the items are `false`.
- `anyTrue()` - Returns `true` if any of the items in the input collection are `true`.
- `anyFalse()` - Returns `true` if any of the items in the input collection are `false`.

## Filtering and Projection Functions

- `where(criteria)` - Returns a collection containing only those elements for which the criteria expression evaluates to `true`.
- `select(projection)` - Evaluates the projection expression for each item in the input collection.
- `ofType(type)` - Returns a collection that contains all items in the input collection that are of the given type or a subclass thereof.

## Subsetting Functions

- `first()` - Returns a collection containing only the first item in the input collection.

## Conversion Functions

- `toString()` - Converts the input to a string representation.
- `iif(criterion, trueResult[, falseResult])` - Conditional operator that returns one of two values based on a condition.

## String Functions

- `join([separator])` - Joins a collection of strings into a single string, optionally using the given separator.

## DateTime Functions

- `until(to, calendarDuration)` - Computes the time interval (duration) between two dates or datetimes.

## Boundary Functions

- `lowBoundary([precision])` - The least possible value of the input to the specified precision.
- `highBoundary([precision])` - The greatest possible value of the input to the specified precision.

## FHIR-specific Functions

- `extension(url)` - Filters the input collection for items named "extension" with the given url.

## Terminology Functions

- `display([language])` - Returns the preferred display term for a Coding.
- `property(code, [type], [language])` - Returns property values for a Coding.
- `designation([use], [language])` - Returns designation values from the lookup operation.
- `memberOf(valueSetURL)` - Checks if concepts are members of a ValueSet.
- `subsumes(codes)` - Checks if concepts subsume other concepts.
- `subsumedBy(codes)` - Checks if concepts are subsumed by other concepts.
- `translate(conceptMapUrl, [reverse], [equivalence], [target])` - Translates concepts using a ConceptMap.

## Reference Resolution Functions

- `resolve()` - Resolves references from one resource to another.
- `reverseResolve(subjectPath)` - Finds resources that reference a particular resource.

## Join Key Functions

- `getResourceKey()` - Returns a collection of keys for a resource collection.
- `getReferenceKey([typeSpecifier])` - Returns a collection of keys for a reference collection.
