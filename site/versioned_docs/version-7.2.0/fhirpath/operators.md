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

All comparison operators return a [Boolean](/docs/fhirpath/data-types#boolean) value.

:::caution
The comparability of units within Quantities is defined
within <a href="https://unitsofmeasure.org/ucum">UCUM</a>.
You can use the <a href="https://ucum.nlm.nih.gov/ucum-lhc/demo.html">NLM Converter Tool</a> to
check whether your units are comparable to each other.
:::

See also: [Comparison](https://hl7.org/fhirpath/#comparison)

## Equality

The `=` operator returns `true` if the left operand is equal to the right
operand, and a `false` otherwise. The `!=` is the inverse of the `=` operator.

Both operands must be singular. The valid types and their combinations is the 
same as for the [Comparison operators](#comparison). In addition to this, 
[Coding](/docs/fhirpath/data-types#coding) types can 
be compared using the equality operators.

If one or both of the operands is an empty collection, the operator will return
an empty collection.

If the operands are Quantity values and are not comparable, an empty collection
will be returned.

:::caution
The comparability of units within Quantities is defined 
within <a href="https://unitsofmeasure.org/ucum">UCUM</a>.
You can use the <a href="https://ucum.nlm.nih.gov/ucum-lhc/demo.html">NLM Converter Tool</a> to
check whether your units are comparable to each other.
:::

See also: [Equality](https://hl7.org/fhirpath/#equality)

## Math

The following math operators are supported:

- `+` - Addition
- `-` - Subtraction
- `*` - Multiplication
- `/` - Division
- `mod` - Modulus

Math operators support [Integer](/docs/fhirpath/data-types#integer),
[Decimal](/docs/fhirpath/data-types#decimal)
and [Quantity](/docs/fhirpath/data-types#quantity) operands. The modulus 
operator is not supported for Quantity types.

Both operands must be singular.

If one or both of the operands is an empty collection, the operator will return
an empty collection.

Integer and Decimal types can be mixed, while Quantity types can only be used
with other Quantity types. 

For Integer and Decimal, `+`, `-` and `*` return the same type as the left
operand, `/` returns [Decimal](/docs/fhirpath/data-types#decimal) and `mod`
returns [Integer](/docs/fhirpath/data-types#integer).

For Quantity types, math operators return a new Quantity with the canonical unit 
common to both operands. If the units are not comparable, an empty collection is 
returned.

See also: [Math](https://hl7.org/fhirpath/#math-2)

## Date/time arithmetic

The following operators are supported for date arithmetic:

- `+` - Add a duration to a [Date](/docs/fhirpath/data-types#date) or
  [DateTime](/docs/fhirpath/data-types#datetime)
- `-` - Subtract a duration from a [Date](/docs/fhirpath/data-types#date) or
  [DateTime](/docs/fhirpath/data-types#datetime)

Date arithmetic always has a `DateTime` or `Date` on the left-hand side, and a
duration on the right-hand side. The duration operand is a
[calendar duration literal](/docs/fhirpath/data-types#quantity). The use of UCUM units
is not supported with these operators.

The `Date` or `DateTime` operand must be singular. If it is an empty collection,
the operator will return an empty collection.

:::note
The use of arithmetic with the <a href="/docs/fhirpath/data-types#time">Time</a> 
type is not supported.
:::

:::caution
Arithmetic
involving <a href="https://hl7.org/fhir/datatypes.html#instant">instant</a> 
values is limited to a precision of seconds.
:::

See also: [Date/Time Arithmetic](https://hl7.org/fhirpath/#datetime-arithmetic)

## Boolean logic

The following Boolean operations are supported:

- `and`
- `or`
- `xor` - Exclusive OR
- `implies` - Material implication

Both operands to a Boolean operator must be singular
[Boolean](/docs/fhirpath/data-types#boolean) values.

All Boolean operators return a [Boolean](/docs/fhirpath/data-types#boolean) value.

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
