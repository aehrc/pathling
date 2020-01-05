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

- [Equality](#equality) (`=` and `!=`)
- [Comparison](#comparison) (`<=`, `<`, `>` and `>=`)
- [Math](#math) (`+`, `-`, `*`, `/` and `mod`)
- [Boolean logic](#boolean-logic) (`and`, `or`, `xor` and `implies`)
- [Membership](#membership) (`in` and `contains`)

See also: [Operations](https://hl7.org/fhirpath/2018Sep/index.html#operations)

## Equality

The `=` operator returns `true` if the left operand is equal to the right
operand, and a `false` otherwise. The `!=` is the inverse of the `=` operator.

The equality operators can accept operands of type
[String](./data-types.html#string), [Integer](./data-types.html#integer),
[Decimal](./data-types.html#decimal), [Boolean](./data-types.html#boolean),
[Date](./data-types.html#date) and [DateTime](./data-types.html#datetime).

Both operands must be of the same type, and must be singular.

If one or both of the operands is an empty collection, the operator will return
an empty collection.

See also: [Equality](https://hl7.org/fhirpath/2018Sep/index.html#equality)

## Comparison

The following comparison operators are supported:

- `<=` - Less than or equal to
- `<` - Less than
- `>` - Greater than
- `>=` - Greater than or equal to

The comparison operators can accept operands of type
[String](./data-types.html#string), [Integer](./data-types.html#integer),
[Decimal](./data-types.html#decimal), [Date](./data-types.html#date) and
[DateTime](./data-types.html#datetime).

Both operands must be of the same type, and must be singular.

If one or both of the operands is an empty collection, the operator will return
an empty collection.

String ordering is strictly lexical and is based on the Unicode value of the
individual characters.

All comparison operators return a [Boolean](./data-types.html#boolean) value.

See also: [Comparison](https://hl7.org/fhirpath/2018Sep/index.html#comparison)

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

Both operands must be of the same type, and must be singular.

If one or both of the operands is an empty collection, the operator will return
an empty collection.

See also: [Math](https://hl7.org/fhirpath/2018Sep/index.html#math)

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
[Boolean logic](https://hl7.org/fhirpath/2018Sep/index.html#boolean-logic)

## Membership

The following membership operators are supported:

- `in`
- `contains`

If the left operand is a collection with a single item, the `in` operator
returns `true` if the item is in the right operand using [equality](#equality)
semantics.

If the left-hand side of the operator is empty, the result is empty. If the
right-hand side is empty, the result is `false`. If the left operand has
multiple items, an error is returned.

The `contains` operator is the inverse of `in`.

See also:
[Collections](https://hl7.org/fhirpath/2018Sep/index.html#collections-2)

Next: [Functions](./functions.html)
