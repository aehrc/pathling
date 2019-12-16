---
layout: page
title: Operators
nav_order: 2
parent: FHIRPath
---

# Operators

Source:
[https://hl7.org/fhirpath/2018Sep/index.html#operations](https://hl7.org/fhirpath/2018Sep/index.html#operations)

Operators are special symbols or keywords that take a left and right operand,
returning some sort of result.

The following operators are supported by the FHIRPath implementation within
Pathling:

- [Equality](#equality) (`=` and `!=`)
- [Comparison](#comparison) (`<=`, `<`, `>` and `>=`)
- [Math](#math) (`+`, `-`, `*`, `/` and `mod`)
- [Boolean logic](#boolean-logic) (`and`, `or`, `xor` and `implies`)
- [Membership](#membership) (`in` and `contains`)

<div class="callout warning">Operations described within the FHIRPath specification that are not covered on this page are not currently supported within Pathling.</div>

## Equality

The `=` operator returns `true` if the left collection is equal to the right
collection, and a `false` otherwise. The `!=` is the inverse of the `=`
operator.

<div class="callout warning">The equality operator in Pathling currently only supports singular, primitive operands of the same type.</div>

## Comparison

The following comparison operators are supported:

- `<=` - Less than or equal to
- `<` - Less than
- `>` - Greater than
- `>=` - Greater than or equal to

The following rules apply to the use of comparison operators:

- The comparison operators are defined for strings, integers, decimals,
  datetimes and times.
- If one or both of the arguments is an empty collection, a comparison operator
  will return an empty collection.
- Both arguments must be collections with single values, and the evaluator will
  throw an error if either collection has more than one item.
- Both arguments must be of the same type, and the evaluator will throw an error
  if the types differ.
- When comparing integers and decimals, the integer will be converted to a
  decimal to make comparison possible.
- String ordering is strictly lexical and is based on the Unicode value of the
  individual characters.

All comparison operators return a Boolean value.

## Math

The following math operators are supported:

- `+` - Addition
- `-` - Subtraction
- `*` - Multiplication
- `/` - Division
- `mod` - Modulus

All math operators in Pathling support only Integer and Decimal operands. `+`,
`-` and `*` return the same type as the left operand, `/` returns Decimal and
`mod` returns Integer.

## Boolean logic

The following boolean operations are supported:

- `and`
- `or`
- `xor` - Exclusive OR
- `implies` - Material implication

Both operands to a boolean operator must be a singular Boolean value. All
boolean operators return a Boolean value.

## Membership

The following membership operators are supported:

- `in`
- `contains`

If the left operand is a collection with a single item, the `in` operator
returns `true` if the item is in the right operand using equality semantics. If
the left-hand side of the operator is empty, the result is empty, if the
right-hand side is empty, the result is `false`. If the left operand has
multiple items, an error is returned.

The `contains` operator is the inverse of `in`.
