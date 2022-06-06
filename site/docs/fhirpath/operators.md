---
sidebar_position: 3
---

# Operators

Operators are special symbols or keywords that take a left and right operand,
returning some sort of result.

## Comparison

The following comparison operators are supported:

- `<=` - Less than or equal to
- `<` - Less than
- `>` - Greater than
- `>=` - Greater than or equal to

Both operands must be singular, the table below shows the valid types and their
combinations.

|          | Boolean | String | Integer | Decimal | Date  | DateTime | Time  | Quantity         |
|----------|---------|--------|---------|---------|-------|----------|-------|------------------|
| Boolean  | true    | false  | false   | false   | false | false    | false | false            |
| String   | false   | true   | false   | false   | false | false    | false | false            |
| Integer  | false   | false  | true    | true    | false | false    | false | false            |
| Decimal  | false   | false  | true    | true    | false | false    | false | false            |
| Date     | false   | false  | false   | false   | true  | true     | false | false            |
| DateTime | false   | false  | false   | false   | true  | true     | false | false            |
| Time     | false   | false  | false   | false   | false | false    | true  | false            |
| Quantity | false   | false  | false   | false   | false | false    | false | true<sup>*</sup> |

If one or both of the operands is an empty collection, the operator will return
an empty collection.

String ordering is strictly lexical and is based on the Unicode value of the
individual characters.

All comparison operators return a [Boolean](./data-types#boolean) value.

<sup>*</sup> Not all Quantity values are comparable, it depends upon the
comparability of the units. See the
[FHIRPath specification](https://hl7.org/fhirpath/#comparison) for details on
how Quantity values are compared. Quantities with a `comparator` are treated as
not comparable by this implementation.

See also: [Comparison](https://hl7.org/fhirpath/#comparison)

## Equality

The `=` operator returns `true` if the left operand is equal to the right
operand, and a `false` otherwise. The `!=` is the inverse of the `=` operator.

Both operands must be singular. The valid types and their combinations is the
same as for the [Comparison operators](#comparison). In addition to this,
[Coding](./data-types#coding) types can be compared using the equality
operators.

If one or both of the operands is an empty collection, the operator will return
an empty collection.

Not all Quantity, Date and DateTime values can be compared for equality, it
depends upon the comparability of the units within the Quantity values. See the
[FHIRPath specification](https://hl7.org/fhirpath/#quantity-equality) for
details on how equality works with Quantity values. Quantities with a
`comparator` are treated as not comparable by this implementation.

See also: [Equality](https://hl7.org/fhirpath/#equality)

## Math

The following math operators are supported:

- `+` - Addition
- `-` - Subtraction
- `*` - Multiplication
- `/` - Division
- `mod` - Modulus

Math operators support only [Integer](./data-types#integer) and
[Decimal](./data-types#decimal) operands.

The type of the two operands can be mixed. `+`, `-` and `*` return the same type
as the left operand, `/` returns [Decimal](./data-types#decimal) and `mod`
returns [Integer](./data-types#integer).

Both operands must be singular.

If one or both of the operands is an empty collection, the operator will return
an empty collection.

See also: [Math](https://hl7.org/fhirpath/#math)

## Date/time arithmetic

The following operators are supported for date arithmetic:

- `+` - Add a duration to a [Date](./data-types#date) or
  [DateTime](./data-types#datetime)
- `-` - Subtract a duration from a [Date](./data-types#date) or
  [DateTime](./data-types#datetime)

Date arithmetic always has a `DateTime` or `Date` on the left-hand side, and a
duration on the right-hand side. The duration operand is a
[calendar duration literal](./data-types#quantity). The use of UCUM units
is not supported with these operators.

The `Date` or `DateTime` operand must be singular. If it is an empty collection,
the operator will return an empty collection.

The use of arithmetic with the [Time](./data-types#time) type is not
supported.

See also: [Date/Time Arithmetic](https://hl7.org/fhirpath/#datetime-arithmetic)

## Boolean logic

The following Boolean operations are supported:

- `and`
- `or`
- `xor` - Exclusive OR
- `implies` - Material implication

Both operands to a Boolean operator must be singular
[Boolean](./data-types#boolean) values.

All Boolean operators return a [Boolean](./data-types#boolean) value.

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

The two operands provided to the `combine` operator must share the same type.

This implementation has the same semantics as
the [combine function](https://hl7.org/fhirpath/#combineother-collection-collection)
within the FHIRPath specification, but is implemented as an operator.
