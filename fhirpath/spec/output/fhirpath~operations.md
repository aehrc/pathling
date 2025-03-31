## Operations

Operators are allowed to be used between any kind of path expressions
(e.g. expr op expr). Like functions, operators will generally propagate
an empty collection in any of their operands. This is true even when
comparing two empty collections using the equality operators, e.g.

``` fhirpath
{} = {}
true > {}
{} != 'dummy'
```

all result in `{}`.

### Equality

#### []= (Equals) 

Returns `true` if the left
collection is equal to the right collection:

As noted above, if either operand is an empty collection, the result is
an empty collection. Otherwise:

If both operands are collections with a single item, they must be of the
same type (or be implicitly convertible to the same type), and:

-   For primitives:
    -   `String`: comparison is
        based on Unicode values
    -   `Integer`: values must
        be exactly equal
    -   `Decimal`: values must
        be equal, trailing zeroes after the decimal are ignored
    -   `Boolean`: values must
        be the same
    -   `Date`: must be exactly
        the same
    -   `DateTime`: must be
        exactly the same, respecting the timezone offset (though +00:00
        = -00:00 = Z)
    -   `Time`: must be exactly
        the same
-   For complex types, equality requires all child properties to be
    equal, recursively.

If both operands are collections with multiple items:

-   Each item must be equal
-   Comparison is order dependent

Otherwise, equals returns `false`.

Note that this implies that if the collections have a different number
of items to compare, the result will be `false`.

Typically, this operator is used with single fixed values as operands.
This means that `Patient.telecom.system = 'phone'`
.fhirpath .highlighter-rouge} will result in an error if there is more
than one `telecom` with a
`use`. Typically, you\'d want
`Patient.telecom.where(system = 'phone')` .fhirpath

If one or both of the operands is the empty collection, this operation
returns an empty collection.

##### Quantity Equality

When comparing quantities for equality, the dimensions of each quantity
must be the same, but not necessarily the unit. For example, units of
`'cm'` and
`'m'` can be compared, but units
of `'cm2'` and
`'cm'` cannot. The comparison
will be made using the most granular unit of either input. Attempting to
operate on quantities with invalid units will result in empty
(`{ }`).

For time-valued quantities, note that calendar durations and definite
quantity durations above days (and weeks) are considered un-comparable:

``` fhirpath
1 year = 1 'a' // {} an empty collection
1 second = 1 's' // true
```

Implementations are not required to fully support operations on units,
but they must at least respect units, recognizing when units differ.

Implementations that do support units shall do so as specified by
[\[UCUM\]](#UCUM), as well as the calendar durations as defined in the
toQuantity function.

##### Date/Time Equality

For `Date`,
`DateTime` and
`Time` equality, the comparison
is performed by considering each precision in order, beginning with
years (or hours for time values), and respecting timezone offsets. If
the values are the same, comparison proceeds to the next precision; if
the values are different, the comparison stops and the result is
`false`. If one input has a
value for the precision and the other does not, the comparison stops and
the result is empty (`{ }`); if
neither input has a value for the precision, or the last precision has
been reached, the comparison stops and the result is
`true`. For the purposes of
comparison, seconds and milliseconds are considered a single precision
using a decimal, with decimal equality semantics.

For example:

``` fhirpath
@2012 = @2012 // returns true
@2012 = @2013 // returns false
@2012-01 = @2012 // returns empty ({ })
@2012-01-01T10:30 = @2012-01-01T10:30 // returns true
@2012-01-01T10:30 = @2012-01-01T10:31 // returns false
@2012-01-01T10:30:31 = @2012-01-01T10:30 // returns empty ({ })
@2012-01-01T10:30:31.0 = @2012-01-01T10:30:31 // returns true
@2012-01-01T10:30:31.1 = @2012-01-01T10:30:31 // returns false
```

For `DateTime` values that do
not have a timezone offsets, whether or not to provide a default
timezone offset is a policy decision. In the simplest case, no default
timezone offset is provided, but some implementations may use the
client\'s or the evaluating system\'s timezone offset.

To support comparison of DateTime values, either both values have no
timezone offset specified, or both values are converted to a common
timezone offset. The timezone offset to use is an implementation
decision. In the simplest case, it\'s the timezone offset of the local
server. The following examples illustrate expected behavior:

``` fhirpath
@2017-11-05T01:30:00.0-04:00 > @2017-11-05T01:15:00.0-05:00 // false
@2017-11-05T01:30:00.0-04:00 < @2017-11-05T01:15:00.0-05:00 // true
@2017-11-05T01:30:00.0-04:00 = @2017-11-05T01:15:00.0-05:00 // false
@2017-11-05T01:30:00.0-04:00 = @2017-11-05T00:30:00.0-05:00 // true
```

Additional functions to support more sophisticated timezone offset
comparison (such as .toUTC()) may be defined in a future version.

#### \~ (Equivalent) 

Returns `true` if the
collections are the same. In particular, comparing empty collections for
equivalence `{ } ~ { }` .fhirpath will result in `true`.

If both operands are collections with a single item, they must be of the
same type (or implicitly convertible to the same type), and:

-   For primitives
    -   `String`: the strings
        must be the same, ignoring case and locale, and normalizing
        whitespace (see [String Equivalence](#string-equivalence) for
        more details).
    -   `Integer`: exactly equal
    -   `Decimal`: values must
        be equal, comparison is done on values rounded to the precision
        of the least precise operand. Trailing zeroes after the decimal
        are ignored in determining precision.
    -   `Date`,
        `DateTime` and
        `Time`: values must be
        equal, except that if the input values have different levels of
        precision, the comparison returns `false`
        .highlighter-rouge}, not empty (`{ }`
        .highlighter-rouge}).
    -   `Boolean`: the values
        must be the same
-   For complex types, equivalence requires all child properties to be
    equivalent, recursively.

If both operands are collections with multiple items:

-   Each item must be equivalent
-   Comparison is not order dependent

Note that this implies that if the collections have a different number
of items to compare, or if one input is a value and the other is empty
(`{ }`), the result will be
`false`.

##### Quantity Equivalence

When comparing quantities for equivalence, the dimensions of each
quantity must be the same, but not necessarily the unit. For example,
units of `'cm'` and
`'m'` can be compared, but units
of `'cm2'` and
`'cm'` cannot. The comparison
will be made using the most granular unit of either input. Attempting to
operate on quantities with invalid units will result in
`false`.

For time-valued quantities, calendar durations and definite quantity
durations are considered equivalent:

``` fhirpath
1 year ~ 1 'a' // true
1 second ~ 1 's' // true
```

Implementations are not required to fully support operations on units,
but they must at least respect units, recognizing when units differ.

Implementations that do support units shall do so as specified by
[\[UCUM\]](#UCUM) as well as the calendar durations as defined in the
toQuantity function.

##### Date/Time Equivalence

For `Date`,
`DateTime` and
`Time` equivalence, the
comparison is the same as for equality, with the exception that if the
input values have different levels of precision, the result is
`false`, rather than empty
(`{ }`). As with equality, the
second and millisecond precisions are considered a single precision
using a decimal, with decimal equivalence semantics.

For example:

``` fhirpath
@2012 ~ @2012 // returns true
@2012 ~ @2013 // returns false
@2012-01 ~ @2012 // returns false as well
@2012-01-01T10:30 ~ @2012-01-01T10:30 // returns true
@2012-01-01T10:30 ~ @2012-01-01T10:31 // returns false
@2012-01-01T10:30:31 ~ @2012-01-01T10:30 // returns false as well
@2012-01-01T10:30:31.0 ~ @2012-01-01T10:30:31 // returns true
@2012-01-01T10:30:31.1 ~ @2012-01-01T10:30:31 // returns false
```

##### String Equivalence

For strings, equivalence returns true if the strings are the same value
while ignoring case and locale, and normalizing whitespace. Normalizing
whitespace means that all whitespace characters are treated as
equivalent, with whitespace characters as defined in the
[Whitespace](#whitespace) lexical category.

#### != (Not Equals) 

The converse of the equals operator, returning
`true` if equal returns
`false`;
`false` if equal returns
`true`; and empty
(`{ }`) if equal returns empty.
In other words, `A != B` .fhirpath is short-hand for
`(A = B).not()` .fhirpath .highlighter-rouge}.

#### !\~ (Not Equivalent) 

The converse of the equivalent operator, returning
`true` if equivalent returns
`false` and
`false` is equivalent returns
`true`. In other words,
`A !~ B` .fhirpath .highlighter-rouge} is short-hand
for `(A ~ B).not()` .fhirpath .highlighter-rouge}.

### Comparison

-   The comparison operators are defined for strings, integers,
    decimals, quantities, dates, datetimes and times.
-   If one or both of the arguments is an empty collection, a comparison
    operator will return an empty collection.
-   Both arguments must be collections with single values, and the
    evaluator will throw an error if either collection has more than one
    item.
-   Both arguments must be of the same type (or implicitly convertible
    to the same type), and the evaluator will throw an error if the
    types differ.
-   When comparing integers and decimals, the integer will be converted
    to a decimal to make comparison possible.
-   String ordering is strictly lexical and is based on the Unicode
    value of the individual characters.

When comparing quantities, the dimensions of each quantity must be the
same, but not necessarily the unit. For example, units of
`'cm'` and
`'m'` can be compared, but units
of `'cm2'` and
`'cm'` cannot. The comparison
will be made using the most granular unit of either input. Attempting to
operate on quantities with invalid units will result in empty
(`{ }`).

For time-valued quantities, note that calendar durations and definite
quantity durations above days (and weeks) are considered un-comparable:

``` fhirpath
1 year > 1 `a` // { } (empty)
10 seconds > 1 's' // true
```

Implementations are not required to fully support operations on units,
but they must at least respect units, recognizing when units differ.

Implementations that do support units shall do so as specified by
[\[UCUM\]](#UCUM) as well as the calendar durations as defined in the
toQuantity function.

For partial Date, DateTime, and Time values, the comparison is performed
by comparing the values at each precision, beginning with years, and
proceeding to the finest precision specified in either input, and
respecting timezone offsets. If one value is specified to a different
level of precision than the other, the result is empty
(`{ }`) to indicate that the
result of the comparison is unknown. As with equality and equivalence,
the second and millisecond precisions are considered a single precision
using a decimal, with decimal comparison semantics.

See the [Equals](#equals) operator for discussion on respecting timezone
offsets in comparison operations.

#### \> (Greater Than) 

The greater than operator (`>`)
returns true if the first operand is strictly greater than the second.
The operands must be of the same type, or convertible to the same type
using an implicit conversion.

``` fhirpath
10 > 5 // true
10 > 5.0 // true; note the 10 is converted to a decimal to perform the comparison
'abc' > 'ABC' // true
4 'm' > 4 'cm' // true (or { } if the implementation does not support unit conversion)
@2018-03-01 > @2018-01-01 // true
@2018-03 > @2018-03-01 // empty ({ })
@2018-03-01T10:30:00 > @2018-03-01T10:00:00 // true
@2018-03-01T10 > @2018-03-01T10:30 // empty ({ })
@2018-03-01T10:30:00 > @2018-03-01T10:30:00.0 // false
@T10:30:00 > @T10:00:00 // true
@T10 > @T10:30 // empty ({ })
@T10:30:00 > @T10:30:00.0 // false
```

#### \< (Less Than) 

The less than operator (`<`)
returns true if the first operand is strictly less than the second. The
operands must be of the same type, or convertible to the same type using
implicit conversion.

``` fhirpath
10 < 5 // false
10 < 5.0 // false; note the 10 is converted to a decimal to perform the comparison
'abc' < 'ABC' // false
4 'm' < 4 'cm' // false (or { } if the implementation does not support unit conversion)
@2018-03-01 < @2018-01-01 // false
@2018-03 < @2018-03-01 // empty ({ })
@2018-03-01T10:30:00 < @2018-03-01T10:00:00 // false
@2018-03-01T10 < @2018-03-01T10:30 // empty ({ })
@2018-03-01T10:30:00 < @2018-03-01T10:30:00.0 // false
@T10:30:00 < @T10:00:00 // false
@T10 < @T10:30 // empty ({ })
@T10:30:00 < @T10:30:00.0 // false
```

#### \<= (Less or Equal) 

The less or equal operator (`\<=`) returns true if the first operand is less than or
equal to the second. The operands must be of the same type, or
convertible to the same type using implicit conversion.

``` fhirpath
10 <= 5 // true
10 <= 5.0 // true; note the 10 is converted to a decimal to perform the comparison
'abc' <= 'ABC' // true
4 'm' <= 4 'cm' // false (or { } if the implementation does not support unit conversion)
@2018-03-01 <= @2018-01-01 // false
@2018-03 <= @2018-03-01 // empty ({ })
@2018-03-01T10:30:00 <= @2018-03-01T10:00:00 // false
@2018-03-01T10 <= @2018-03-01T10:30 // empty ({ })
@2018-03-01T10:30:00 <= @2018-03-01T10:30:00.0 // true
@T10:30:00 <= @T10:00:00 // false
@T10 <= @T10:30 // empty ({ })
@T10:30:00 <= @T10:30:00.0 // true
```

#### \>= (Greater or Equal) 

The greater or equal operator (`>=`) returns true if the first operand is greater than
or equal to the second. The operands must be of the same type, or
convertible to the same type using implicit conversion.

``` fhirpath
10 >= 5 // false
10 >= 5.0 // false; note the 10 is converted to a decimal to perform the comparison
'abc' >= 'ABC' // false
4 'm' >= 4 'cm' // true (or { } if the implementation does not support unit conversion)
@2018-03-01 >= @2018-01-01 // true
@2018-03 >= @2018-03-01 // empty ({ })
@2018-03-01T10:30:00 >= @2018-03-01T10:00:00 // true
@2018-03-01T10 >= @2018-03-01T10:30 // empty ({ })
@2018-03-01T10:30:00 >= @2018-03-01T10:30:00.0 // true
@T10:30:00 >= @T10:00:00 // true
@T10 >= @T10:30 // empty ({ })
@T10:30:00 >= @T10:30:00.0 // true
```

### Types

#### is *type specifier*

If the left operand is a collection with a single item and the second
operand is a type identifier, this operator returns
`true` if the type of the left
operand is the type specified in the second operand, or a subclass
thereof. If the input value is not of the type, this operator returns
`false`. If the identifier
cannot be resolved to a valid type identifier, the evaluator will throw
an error. If the input collections contains more than one item, the
evaluator will throw an error. In all other cases this operator returns
`false`.

A *type specifier* is an identifier that must resolve to the name of a
type in a model. Type specifiers can have qualifiers, e.g.
`FHIR.Patient`, where the
qualifier is the name of the model.

``` fhirpath
Bundle.entry.resource.all($this is Observation implies status = 'finished')
```

This example returns `true` if
all Observation resources in the bundle have a status of finished.

#### is(type : *type specifier*) 

The `is()` function is supported
for backwards compatibility with previous implementations of FHIRPath.
Just as with the `is` keyword,
the `type` argument is an
identifier that must resolve to the name of a type in a model. For
implementations with compile-time typing, this requires special-case
handling when processing the argument to treat it as a type specifier
rather than an identifier expression:

``` fhirpath
Bundle.entry.resource.all($this.is(Observation) implies status = 'finished')
```

> **Note:** The `is()` function
> is defined for backwards compatibility only and may be deprecated in a
> future release.

#### as *type specifier*

If the left operand is a collection with a single item and the second
operand is an identifier, this operator returns the value of the left
operand if it is of the type specified in the second operand, or a
subclass thereof. If the identifier cannot be resolved to a valid type
identifier, the evaluator will throw an error. If there is more than one
item in the input collection, the evaluator will throw an error.
Otherwise, this operator returns the empty collection.

A *type specifier* is an identifier that must resolve to the name of a
type in a model. Type specifiers can have qualifiers, e.g.
`FHIR.Patient`, where the
qualifier is the name of the model.

``` fhirpath
Observation.component.where(value as Quantity > 30 'mg')
```

#### as(type : *type specifier*) 

The `as()` function is supported
for backwards compatibility with previous implementations of FHIRPath.
Just as with the `as` keyword,
the `type` argument is an
identifier that must resolve to the name of a type in a model. For
implementations with compile-time typing, this requires special-case
handling when processing the argument to treat is a type specifier
rather than an identifier expression:

``` fhirpath
Observation.component.where(value.as(Quantity) > 30 'mg')
```

> **Note:** The `as()` function
> is defined for backwards compatibility only and may be deprecated in a
> future release.

### Collections

#### \| (union collections) 

Merge the two collections into a single collection, eliminating any
duplicate values (using [equals](#equals) (`=`)) to determine equality). There is no expectation of
order in the resulting collection.

See the [union](#unionother-collection) function for more detail.

#### in (membership)

If the left operand is a collection with a single item, this operator
returns true if the item is in the right operand using equality
semantics. If the left-hand side of the operator is empty, the result is
empty, if the right-hand side is empty, the result is false. If the left
operand has multiple items, an exception is thrown.

The following example returns true if `'Joe'` is in the list of given names for the Patient:

``` fhirpath
'Joe' in Patient.name.given
```

#### contains (containership)

If the right operand is a collection with a single item, this operator
returns true if the item is in the left operand using equality
semantics. If the right-hand side of the operator is empty, the result
is empty, if the left-hand side is empty, the result is false. This is
the converse operation of in.

The following example returns true if the list of given names for the
Patient has `'Joe'` in it:

``` fhirpath
Patient.name.given contains 'Joe'
```

### Boolean logic

For all boolean operators, the collections passed as operands are first
evaluated as Booleans (as described in [Singleton Evaluation of
Collections](#singleton-evaluation-of-collections)). The operators then
use three-valued logic to propagate empty operands.

> **Note:** To ensure that FHIRPath expressions can be freely rewritten
> by underlying implementations, there is no expectation that an
> implementation respect short-circuit evaluation. With regard to
> performance, implementations may use short-circuit evaluation to
> reduce computation, but authors should not rely on such behavior, and
> implementations must not change semantics with short-circuit
> evaluation. If short-circuit evaluation is needed to avoid effects
> (e.g. runtime exceptions), use the [`iif()`
> .highlighter-rouge}](#iif) function.

#### and

Returns `true` if both operands
evaluate to `true`,
`false` if either operand
evaluates to `false`, and the
empty collection (`{ }`)
otherwise.

  and         true                                                    false                                             empty
  ----------- ------------------------------------------------------- ------------------------------------------------- -------------------------------------------------------
  **true**    `true`          `false`   empty (`{ }`)
  **false**   `false`         `false`   `false`
  **empty**   empty (`{ }`)   `false`   empty (`{ }`)

#### or

Returns `false` if both operands
evaluate to `false`,
`true` if either operand
evaluates to `true`, and empty
(`{ }`) otherwise:

  or          true                                             false                                                   empty
  ----------- ------------------------------------------------ ------------------------------------------------------- -------------------------------------------------------
  **true**    `true`   `true`          `true`
  **false**   `true`   `false`         empty (`{ }`)
  **empty**   `true`   empty (`{ }`)   empty (`{ }`)

#### not() : Boolean 

Returns `true` if the input
collection evaluates to `false`,
and `false` if it evaluates to
`true`. Otherwise, the result is
empty (`{ }`):

  not          
  ----------- -------------------------------------------------------
  **true**    `false`
  **false**   `true`
  **empty**   empty (`{ }`)

#### xor

Returns `true` if exactly one of
the operands evaluates to `true`, `false` if
either both operands evaluate to `true` or both operands evaluate to
`false`, and the empty
collection (`{ }`) otherwise:

  xor         true                                                    false                                                   empty
  ----------- ------------------------------------------------------- ------------------------------------------------------- -------------------------------------------------------
  **true**    `false`         `true`          empty (`{ }`)
  **false**   `true`          `false`         empty (`{ }`)
  **empty**   empty (`{ }`)   empty (`{ }`)   empty (`{ }`)

#### implies

If the left operand evaluates to `true`, this operator returns the boolean evaluation of the
right operand. If the left operand evaluates to
`false`, this operator returns
`true`. Otherwise, this operator
returns `true` if the right
operand evaluates to `true`, and
the empty collection (`{ }`)
otherwise.

  implies     true                                             false                                                   empty
  ----------- ------------------------------------------------ ------------------------------------------------------- -------------------------------------------------------
  **true**    `true`   `false`         empty (`{ }`)
  **false**   `true`   `true`          `true`
  **empty**   `true`   empty (`{ }`)   empty (`{ }`)

The implies operator is useful for testing conditionals. For example, if
a given name is present, then a family name must be as well:

``` fhirpath
Patient.name.given.exists() implies Patient.name.family.exists()
CareTeam.onBehalfOf.exists() implies (CareTeam.member.resolve() is Practitioner)
StructureDefinition.contextInvariant.exists() implies StructureDefinition.type = 'Extension'
```

Note that implies may use short-circuit evaluation in the case that the
first operand evaluates to false.

### Math

The math operators require each operand to be a single element. Both
operands must be of the same type, or of compatible types according to
the rules for implicit conversion. Each operator below specifies which
types are supported.

If there is more than one item, or an incompatible item, the evaluation
of the expression will end and signal an error to the calling
environment.

As with the other operators, the math operators will return an empty
collection if one or both of the operands are empty.

When operating on quantities, the dimensions of each quantity must be
the same, but not necessarily the unit. For example, units of
`'cm'` and
`'m'` can be compared, but units
of `'cm2'` and
`'cm'` cannot. The unit of the
result will be the most granular unit of either input. Attempting to
operate on quantities with invalid units will result in empty
(`{ }`).

Implementations are not required to fully support operations on units,
but they must at least respect units, recognizing when units differ.

Implementations that do support units shall do so as specified by
[\[UCUM\]](#UCUM) as well as the calendar durations as defined in the
toQuantity function.

Operations that cause arithmetic overflow or underflow will result in
empty (`{ }`).

#### \* (multiplication) 

Multiplies both arguments (supported for Integer, Decimal, and
Quantity). For multiplication involving quantities, the resulting
quantity will have the appropriate unit:

``` fhirpath
12 'cm' * 3 'cm' // 36 'cm2'
3 'cm' * 12 'cm2' // 36 'cm3'
```

#### / (division) 

Divides the left operand by the right operand (supported for Integer,
Decimal, and Quantity). The result of a division is always Decimal, even
if the inputs are both Integer. For integer division, use the
`div` operator.

If an attempt is made to divide by zero, the result is empty.

For division involving quantities, the resulting quantity will have the
appropriate unit:

``` fhirpath
12 'cm2' / 3 'cm' // 4.0 'cm'
12 / 0 // empty ({ })
```

#### + (addition) 

For Integer, Decimal, and quantity, adds the operands. For strings,
concatenates the right operand to the left operand.

When adding quantities, the dimensions of each quantity must be the
same, but not necessarily the unit.

``` fhirpath
3 'm' + 3 'cm' // 303 'cm'
```

#### - (subtraction) 

Subtracts the right operand from the left operand (supported for
Integer, Decimal, and Quantity).

When subtracting quantities, the dimensions of each quantity must be the
same, but not necessarily the unit.

``` fhirpath
3 'm' - 3 'cm' // 297 'cm'
```

#### div

Performs truncated division of the left operand by the right operand
(supported for Integer and Decimal). In other words, the division that
ignores any remainder:

``` fhirpath
5 div 2 // 2
5.5 div 0.7 // 7
5 div 0 // empty ({ })
```

#### mod

Computes the remainder of the truncated division of its arguments
(supported for Integer and Decimal).

``` fhirpath
5 mod 2 // 1
5.5 mod 0.7 // 0.6
5 mod 0 // empty ({ })
```

#### & (String concatenation) 

For strings, will concatenate the strings, where an empty operand is
taken to be the empty string. This differs from `+` on two strings, which will result in an empty
collection when one of the operands is empty. This operator is
specifically included to simplify treating an empty collection as an
empty string, a common use case in string manipulation.

``` fhirpath
'ABC' + 'DEF' // 'ABCDEF'
'ABC' + { } + 'DEF' // { }
'ABC' & 'DEF' // 'ABCDEF'
'ABC' & { } & 'DEF' // 'ABCDEF'
```

### Date/Time Arithmetic

Date and time arithmetic operators are used to add time-valued
quantities to date/time values. The left operand must be a
`Date`,
`DateTime`, or
`Time` value, and the right
operand must be a `Quantity`
with a time-valued unit:

-   `year`,
    `years`
-   `month`,
    `months`
-   `week`,
    `weeks`
-   `day`,
    `days`
-   `hour`,
    `hours`
-   `minute`,
    `minutes`
-   `second`,
    `seconds`, or
    `'s'`
-   `millisecond`,
    `milliseconds`, or
    `'ms'`

To avoid the potential confusion of calendar-based date/time arithmetic
with definite duration date/time arithmetic, FHIRPath defines
definite-duration date/time arithmetic for seconds and below, and
calendar-based date/time arithmetic for seconds and above. At the
second, calendar-based and definite-duration-based date/time arithmetic
are identical. If a definite-quantity duration above seconds appears in
a date/time arithmetic calculation, the evaluation will end and signal
an error to the calling environment.

Within FHIRPath, calculations involving date/times and calendar
durations shall use calendar semantics as specified in
[\[ISO8601\]](#ISO8601). Specifically:

  ------------- -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
  year          The year, positive or negative, is added to the year component of the date or time value. If the resulting year is out of range, an error is thrown. If the month and day of the date or time value is not a valid date in the resulting year, the last day of the calendar month is used.
  month         The month, positive or negative is divided by 12, and the integer portion of the result is added to the year component. The remaining portion of months is added to the month component. If the resulting date is not a valid date in the resulting year, the last day of the resulting calendar month is used.
  week          The week, positive or negative, is multiplied by 7, and the resulting value is added to the day component, respecting calendar month and calendar year lengths.
  day           The day, positive or negative, is added to the day component, respecting calendar month and calendar year lengths.
  hour          The hours, positive or negative, are added to the hour component, with each 24 hour block counting as a calendar day, and respecting calendar month and calendar year lengths.
  minute        The minutes, positive or negative, are added to the minute component, with each 60 minute block counting as an hour, and respecting calendar month and calendar year lengths.
  second        The seconds, positive or negative, are added to the second component, with each 60 second block counting as a minute, and respecting calendar month and calendar year lengths.
  millisecond   The milliseconds, positive or negative, are added to the millisecond component, with each 1000 millisecond block counting as a second, and respecting calendar month and calendar year lengths.
  ------------- -----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

If there is more than one item, or an item of an incompatible type, the
evaluation of the expression will end and signal an error to the calling
environment.

If either or both arguments are empty (`{ }`), the result is empty (`{ }`).

#### + (addition) 

Returns the value of the given `Date`, `DateTime`,
or `Time`, incremented by the
time-valued quantity, respecting variable length periods for calendar
years and months.

For `Date` values, the quantity
unit must be one of: `years`,
`months`,
`weeks`, or
`days`

For `DateTime` values, the
quantity unit must be one of: `years`, `months`,
`weeks`,
`days`,
`hours`,
`minutes`,
`seconds`, or
`milliseconds` (or an equivalent
unit), or the evaluation will end and signal an error to the calling
environment.

For `Time` values, the quantity
unit must be one of: `hours`,
`minutes`,
`seconds`, or
`milliseconds` (or an equivalent
unit), or the evaluation will end and signal an error to the calling
environment.

For precisions above `seconds`,
the decimal portion of the time-valued quantity is ignored, since
date/time arithmetic above seconds is performed with calendar duration
semantics.

For partial date/time values where the time-valued quantity is more
precise than the partial date/time, the operation is performed by
converting the time-valued quantity to the highest precision in the
partial (removing any decimal value off) and then adding to the
date/time value. For example:

``` fhirpath
@2014 + 24 months
@2019-03-01 + 24 months // @2021-03-01
```

The first example above will evaluate to the value
`@2016` even though the
date/time value is not specified to the level of precision of the
time-valued quantity. The second example will evaluate to
`@2021-03-01`.

``` fhirpath
@2014 + 23 months
@2016 + 365 days
```

The first example above returns \@2015, because 23 months only
constitutes one year. The second example returns 2017 because even
though 2016 is a leap-year, the time-valued quantity
(`365 days`) is converted to
`1 year`, a standard calendar
year of 365 days.

Calculations involving weeks are equivalent to multiplying the number of
weeks by 7 and performing the calculation for the resulting number of
days.

#### - (subtraction) 

Returns the value of the given `Date`, `DateTime`,
or `Time`, decremented by the
time-valued quantity, respecting variable length periods for calendar
years and months.

For `Date` values, the quantity
unit must be one of: `years`,
`months`,
`weeks`, or
`days`

For `DateTime` values, the
quantity unit must be one of: `years`, `months`,
`weeks`,
`days`,
`hours`,
`minutes`,
`seconds`, or
`milliseconds` (or an equivalent
unit), or the evaluation will end and signal an error to the calling
environment.

For `Time` values, the quantity
unit must be one of: `hours`,
`minutes`,
`seconds`, or
`milliseconds` (or an equivalent
unit), or the evaluation will end and signal an error to the calling
environment.

For precisions above `seconds`,
the decimal portion of the time-valued quantity is ignored, since
date/time arithmetic above seconds is performed with calendar duration
semantics.

For partial date/time values where the time-valued quantity is more
precise than the partial date/time, the operation is performed by
converting the time-valued quantity to the highest precision in the
partial (removing any decimal value off) and then subtracting from the
date/time value. For example:

``` fhirpath
@2014 - 24 months
@2019-03-01 - 24 months // @2017-03-01
```

The first example above will evaluate to the value
`@2012` even though the
date/time value is not specified to the level of precision of the
time-valued quantity. The second example will evaluate to
`@2017-03-01`.

Calculations involving weeks are equivalent to multiplying the number of
weeks by 7 and performing the calculation for the resulting number of
days.

### Operator precedence

Precedence of operations, in order from high to low:

``` txt
#01 . (path/function invocation)
#02 [] (indexer)
#03 unary + and -
#04: *, /, div, mod
#05: +, -, &
#06: is, as
#07: |
#08: >, <, >=, <=
#09: =, ~, !=, !~
#10: in, contains
#11: and
#12: xor, or
#13: implies
```

As customary, precedence may be established explicitly using parentheses
(`( )`).

As an example, consider the following expression:

``` fhirpath
-7.combine(3)
```

Because the invocation operator (`.`) has a higher precedence than the unary negation
(`-`), the unary negation will
be applied to the result of the combine of 7 and 3, resulting in an
error (because unary negation cannot be applied to a list):

``` fhirpath
-(7.combine(3)) // ERROR
```

Use parentheses to ensure the unary negation applies to the
`7`:

``` fhirpath
(-7).combine(3) // { -7, 3 }
```