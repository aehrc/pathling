---
layout: page
title: Operators
nav_order: 1
parent: FHIRPath
grand_parent: Documentation
---

# Operators

Operators are special symbols or keywords that take a left and right operand,
returning some sort of result.

The following operators are supported by the FHIRPath implementation within
Pathling:

- [Comparison](#comparison) (`<=`, `<`, `>` and `>=`)
- [Equality](#equality) (`=` and `!=`)
- [Math](#math) (`+`, `-`, `*`, `/` and `mod`)
- [Boolean logic](#boolean-logic) (`and`, `or`, `xor` and `implies`)
- [Membership](#membership) (`in` and `contains`)
- [combine](#combine)

See also: [Operations](https://hl7.org/fhirpath/#operations)

## Comparison

The following comparison operators are supported:

- `<=` - Less than or equal to
- `<` - Less than
- `>` - Greater than
- `>=` - Greater than or equal to

Both operands must be must be singular, the table below shows the valid types 
and their combinations.

|          | Boolean | String | Integer | Decimal | Date  | DateTime | Time  |
| -------- | ------- | ------ | ------- | ------- | ----- | -------- | ----- | 
| Boolean  | true    | false  | false   | false   | false | false    | false |
| String   | false   | true   | false   | false   | false | false    | false |
| Integer  | false   | false  | true    | true    | false | false    | false |
| Decimal  | false   | false  | true    | true    | false | false    | false |
| Date     | false   | false  | false   | false   | true  | true     | false |
| DateTime | false   | false  | false   | false   | true  | true     | false |
| Time     | false   | false  | false   | false   | false | false    | true  |

If one or both of the operands is an empty collection, the operator will return
an empty collection.

String ordering is strictly lexical and is based on the Unicode value of the
individual characters.

All comparison operators return a [Boolean](./data-types.html#boolean) value.

See also: [Comparison](https://hl7.org/fhirpath/#comparison)

## Equality

The `=` operator returns `true` if the left operand is equal to the right
operand, and a `false` otherwise. The `!=` is the inverse of the `=` operator.

Both operands must be singular. The valid types and their combinations is the 
same as for the [Comparison operators](#comparison). In addition to this, 
[Coding](./data-types.html#coding) types can 
be compared using the equality operators.

If one or both of the operands is an empty collection, the operator will return
an empty collection.

See also: [Equality](https://hl7.org/fhirpath/#equality)

## Math

The following math operators are supported:

- `+` - Addition
- `-` - Subtraction
- `*` - Multiplication
- `/` - Division
- `mod` - Modulus

Math operators support only [Integer](./data-types.html#integer) and
[Decimal](./data-types.html#decimal) operands.

The type of the two operands can be mixed. `+`, `-` and `*` return the same type
as the left operand, `/` returns [Decimal](./data-types.html#decimal) and `mod`
returns [Integer](./data-types.html#integer).

Both operands must be singular.

If one or both of the operands is an empty collection, the operator will return
an empty collection.

See also: [Math](https://hl7.org/fhirpath/#math)

## Boolean logic

The following Boolean operations are supported:

- `and`
- `or`
- `xor` - Exclusive OR
- `implies` - Material implication

Both operands to a Boolean operator must be singular
[Boolean](./data-types.html#boolean) values.

All Boolean operators return a [Boolean](./data-types.html#boolean) value.

See also:
[Boolean logic](https://hl7.org/fhirpath/#boolean-logic)

## Membership

The following membership operators are supported:

- `in` (`[element] in [collection]`)
- `contains` (`[collection] contains [element]`)

If the element operand is a collection with a single item, the operator
returns `true` if the item is in the collection using [equality](#equality)
semantics.

If the element is empty, the result is empty. If the collection is empty, the 
result is `false`. If the element has multiple items, an error is returned.

See also:
[Collections](https://hl7.org/fhirpath/#collections-2)

## combine

The `combine` operator merges the left and right operands into a single 
collection, preserving duplicates. The result is not ordered.

This reflects the semantics of the [combine function](https://hl7.org/fhirpath/#combineother-collection-collection) 
within the FHIRPath specification, but implemented as an operator.

Next: [Functions](./functions.html)
