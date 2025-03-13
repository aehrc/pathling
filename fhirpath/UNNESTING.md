# FHIRPath Extract Unnesting Rules

## Overview

The `ImplicitUnnester` class transforms a flat list of FHIRPath expressions into a hierarchical tree structure that preserves the relationships between nested elements in the source data. This ensures that when extracting data, only valid combinations of values are included in the result.

## Purpose

When extracting multiple related fields from a FHIR resource (like both given names and family names from a Patient), we need to ensure that the extracted data maintains the proper relationships. Without proper unnesting:

- We might get a Cartesian product of all values (incorrect combinations)
- We might lose the hierarchical structure of the original data

The unnester creates a tree structure where:
- Each non-leaf node represents a common prefix shared by multiple paths
- Each leaf node represents a path that should be evaluated as a column selection

## Unnesting Rules

### Basic Rules

1. The root of the tree is always `%resource`
2. Paths with common prefixes are grouped together
3. `$this` is represented as a leaf node
4. Function calls and operators receive special handling

### Examples

Below are examples of how different FHIRPath expressions are unnested, based on the test cases in `ImplicitUnnesterTest`.

## Example 1: Empty List

**Input:**
```
(empty list)
```

**Unnested Tree:**
```
%resource
```

## Example 2: Single $this

**Input:**
```
$this
```

**Unnested Tree:**
```
%resource
  $this
```

## Example 3: Multiple $this References

**Input:**
```
$this
$this
```

**Unnested Tree:**
```
%resource
  $this
  $this
```

## Example 4: Single Property

**Input:**
```
name
```

**Unnested Tree:**
```
%resource
  name
    $this
```

## Example 5: Multiple Same Properties

**Input:**
```
name
name
```

**Unnested Tree:**
```
%resource
  name
    $this
    $this
```

## Example 6: Single Traversal Path

**Input:**
```
name.family
```

**Unnested Tree:**
```
%resource
  name.family
    $this
```

## Example 7: Multiple Same Traversal Paths

**Input:**
```
name.family
name.family
```

**Unnested Tree:**
```
%resource
  name.family
    $this
    $this
```

## Example 8: Single Aggregation Function

**Input:**
```
count()
```

**Unnested Tree:**
```
%resource
  count()
    $this
```

## Example 9: Multiple Same Aggregation Functions

**Input:**
```
count()
count()
```

**Unnested Tree:**
```
%resource
  count()
    $this
    $this
```

## Example 10: Common Aggregate with Property

**Input:**
```
first().id
first().gender
```

**Unnested Tree:**
```
%resource
  first()
    id
      $this
    gender
      $this
```

## Example 11: Common Aggregate Path with Property

**Input:**
```
first().last().id
first().last().gender
```

**Unnested Tree:**
```
%resource
  first().last()
    id
      $this
    gender
      $this
```

## Example 12: Common Property with Aggregate

**Input:**
```
name.count()
name.exists()
```

**Unnested Tree:**
```
%resource
  name.count()
    $this
  name.exists()
    $this
```

## Example 13: Common Aggregate with Aggregate

**Input:**
```
first().count()
first().exists()
```

**Unnested Tree:**
```
%resource
  first().count()
    $this
  first().exists()
    $this
```

## Example 14: Single Operator

**Input:**
```
$this = 'John'
```

**Unnested Tree:**
```
%resource
  $this = 'John'
    $this
```

## Example 15: Multiple Same Operators

**Input:**
```
$this = 'John'
$this = 'John'
```

**Unnested Tree:**
```
%resource
  $this = 'John'
    $this
    $this
```

## Example 16: $this with $this Operator

**Input:**
```
$this
$this = 'John'
```

**Unnested Tree:**
```
%resource
  $this
  $this = 'John'
    $this
```

## Example 17: Property with Property Operator

**Input:**
```
name
name = 'John'
```

**Unnested Tree:**
```
%resource
  name
    $this
    $this = 'John'
      $this
```

## Example 18: Unrelated Simple Columns

**Input:**
```
id
gender
```

**Unnested Tree:**
```
%resource
  id
    $this
  gender
    $this
```

## Example 19: Nested Simple Columns

**Input:**
```
id
name
name.given
name.family
maritalStatus.coding.system
maritalStatus.coding.code
```

**Unnested Tree:**
```
%resource
  id
    $this
  name
    $this
    given
      $this
    family
      $this
  maritalStatus.coding
    system
      $this
    code
      $this
```

## Example 20: Nested Simple and Aggregate Columns

**Input:**
```
id
name.given
name.family
name.count()
name.exists()
```

**Unnested Tree:**
```
%resource
  id
    $this
  name
    given
      $this
    family
      $this
  name.count()
    $this
  name.exists()
    $this
```

## Example 21: Non-Branched Paths Should Not Be Unnested

**Input:**
```
id
name.given.count()
```

**Unnested Tree:**
```
%resource
  id
    $this
  name.given.count()
    $this
```

## Example 22: Mixed Nested Aggregation and Value

**Input:**
```
id
name.family
name.given.count()
```

**Unnested Tree:**
```
%resource
  id
    $this
  name
    family
      $this
    given.count()
      $this
```

## Example 23: Nested Simple and Nested Aggregate Columns

**Input:**
```
id
name.given
name.family
name.family.count()
name.family.count().count()
name.family.count().exists()
name.family.count().exists().first()
```

**Unnested Tree:**
```
%resource
  id
    $this
  name
    given
      $this
    family
      $this
    family.count()
      $this
    family.count().count()
      $this
    family.count().exists()
      $this
    family.count().exists().first()
      $this
```

## Example 24: Double Nested Aggregation

**Input:**
```
id
name.count()
name.family
name.given.count()
name.given.empty()
```

**Unnested Tree:**
```
%resource
  id
    $this
  name
    family
      $this
    given.count()
      $this
    given.empty()
      $this
  name.count()
    $this
```

## Example 25: Unnest Simple Operator with Common Prefix

**Input:**
```
id
name.family
name.given = 'John'
```

**Unnested Tree:**
```
%resource
  id
    $this
  name
    family
      $this
    given.($this = 'John')
      $this
```

## Example 26: Unnest Simple Operator with Common Prefix in Right Argument

**Input:**
```
id
name.family
'John' = name.given
```

**Unnested Tree:**
```
%resource
  id
    $this
  name.family
    $this
  'John'.($this = name.given)
    $this
```

## Example 27: Unnest Simple Operator with Full Common Prefix

**Input:**
```
id
name.given
name.given = 'John'
```

**Unnested Tree:**
```
%resource
  id
    $this
  name.given
    $this
    $this = 'John'
      $this
```

## Example 28: Unnest Simple Operator with Full Common Prefix and Aggregation

**Input:**
```
id
name.given.first()
name.given = 'John'
```

**Unnested Tree:**
```
%resource
  id
    $this
  name
    given.($this = 'John')
      $this
    given.first()
      $this
```

## Implementation Details

The unnesting process works by:

1. Separating paths into leaf paths (like $this) and unnestable paths
2. Grouping unnestable paths by their common prefixes
3. Processing function paths separately from regular paths
4. Recursively processing the suffixes of each group
5. Combining the results into a unified tree structure

## Special Cases

- **Function Calls**: Functions like `first()` or `count()` change the context and require special handling
- **Operators**: Expressions with operators (like `=`) are treated specially to maintain their semantics
- **Aggregations**: Aggregation functions are not unnested in the same way as regular paths

## Benefits

This unnesting approach ensures:

1. Data integrity - only valid combinations of values appear in the result
2. Efficient queries - by avoiding unnecessary joins and Cartesian products
3. Proper handling of FHIR's hierarchical data model
