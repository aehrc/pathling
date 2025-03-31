## Expressions

FHIRPath expressions can consist of *paths*, *literals*, *operators*,
and *function invocations*, and these elements can be chained together,
so that the output of one operation or function is the input to the
next. This is the core of the *fluent* [\[Fluent\]](#fluent) syntactic
style and allows complex paths and expressions to be built up from
simpler components.

### Literals

In addition to paths, FHIRPath expressions may contain *literals*,
*operators*, and *function invocations*. FHIRPath supports the following
types of literals:

``` txt
Boolean: true, false
String: 'test string', 'urn:oid:3.4.5.6.7.8'
Integer: 0, 45
Long: 0L, 45L    // Long is defined as STU
Decimal: 0.0, 3.14159265
Date: @2015-02-04 (@ followed by ISO8601 compliant date)
DateTime: @2015-02-04T14:34:28+09:00 (@ followed by ISO8601 compliant date/time)
Time: @T14:34:28 (@ followed by ISO8601 compliant time beginning with T, no timezone offset)
Quantity: 10 'mg', 4 days
```

For each type of literal, FHIRPath defines a named system type to allow
operations and functions to be defined, as well as an ultimate root
type, `System.Any`. For example,
the multiplication operator (`*`) is defined for the numeric types Integer and
Decimal, as well as the Quantity type. See the discussion on
[Models](#models) for a more detailed discussion of how these types are
used within evaluation contexts.

#### Boolean

The `Boolean` type represents
the logical Boolean values `true` and `false`.
These values are used as the result of comparisons, and can be combined
using logical operators such as `and` and `or`.

``` fhirpath
true
false
```

#### String

The `String` type represents
string values up to 2^31^-1 characters in length. String literals are
surrounded by single-quotes and may use `\`-escapes to escape quotes and represent Unicode
characters:

  Escape                                             Character
  -------------------------------------------------- ----------------------------------------------------------------------------------
  `\'`       Single-quote
  `\"`       Double-quote
  `` \` ``   Backtick
  `\r`       Carriage Return
  `\n`       Line Feed
  `\t`       Tab
  `\f`       Form Feed
  `\\`       Backslash
  `\uXXXX`   Unicode character, where XXXX is the hexadecimal representation of the character

No other escape sequences besides those listed above are recognized.

Note that Unicode is supported in both string literals and delimited
[Identifiers](#identifiers).

``` fhirpath
'test string'
'urn:oid:3.4.5.6.7.8'
```

If a `\` is used at the
beginning of a non-escape sequence, it will be ignored and will not
appear in the sequence.

``` txt
define TestEscape1: '\p' // 'p'
define TestEscape2: '\\p' // '\p'
define TestEscape3: '\3' // '3'
define TestEscape4: '\u005' // 'u005'
define TestEscape5: '\' // ''
```

#### Integer

The `Integer` type represents
whole numbers in the range -2^31^ to 2^31^-1.

``` fhirpath
0
45
-5
```

> Note that the minus sign (`-`)
> in the representation of a negative integer is not part of the
> literal, it is the unary negation operator defined as part of FHIRPath
> syntax.

##### Long

> **Note:** The contents of this section are Standard for Trial Use
> (STU)

The `Long` type represents whole
numbers in the range -2^63^ to 2^63^-1.

``` stu
0L
45L
-5L
```

This type corresponds to System.Long

#### Decimal

The `Decimal` type represents
real values in the range (-10^28^+1)/10^8^ to (10^28^-1)/10^8^ with a
step size of 10^-8^. This range is defined based on a survey of
decimal-value implementations and is based on the most useful lowest
common denominator. Implementations can provide support for larger
decimals and higher precision, but must provide at least the range and
precision defined here. In addition, implementations should use
[fixed-precision
decimal](https://en.wikipedia.org/wiki/Fixed-point_arithmetic) formats
to ensure that decimal values are accurately represented.

``` fhirpath
0.0
3.14159265
```

Decimal literals cannot use exponential notation. There is enough
additional complexity associated with enabling exponential notation that
this is outside the scope of what FHIRPath is intended to support
(namely graph traversal).

#### Date

The `Date` type represents date
and partial date values in the range \@0001-01-01 to \@9999-12-31 with a
1 day step size.

The `Date` literal is a subset
of [\[ISO8601\]](#ISO8601):

-   A date literal begins with an `@`
    .highlighter-rouge}
-   It uses the format **YYYY-MM-DD** format, though month and day parts
    are optional, and a separator is required between provided
    components
-   Week dates and ordinal dates are not allowed
-   Years must be present (e.g. `@-10-20`
    .highlighter-rouge} is not a valid Date in FHIRPath)
-   Months must be present if a day is present
-   To specify a date and time together, see the description of
    `DateTime` below

The following examples illustrate the use of the
`Date` literal:

``` fhirpath
@2014-01-25
@2014-01
@2014
```

Consult the [formal grammar](grammar.html) for more details.

#### Time

The `Time` type represents
time-of-day and partial time-of-day values in the range \@T00:00:00.000
to \@T23:59:59.999 with a step size of 1 millisecond. This range is
defined based on a survey of time implementations and is based on the
most useful lowest common denominator. Implementations can provide
support for higher precision, but must provide at least the range and
precision defined here. Time values in FHIRPath do not have a timezone
or timezone offset.

The `Time` literal uses a subset
of [\[ISO8601\]](#ISO8601):

-   A time begins with a `@T`
-   It uses the **Thh:mm:ss.fff** format

The following examples illustrate the use of the
`Time` literal:

``` fhirpath
@T12:00
@T14:30:14.559
```

Consult the [formal grammar](grammar.html) for more details.

#### DateTime

The `DateTime` type represents
date/time and partial date/time values in the range
`@0001-01-01T00:00:00.000 to @9999-12-31T23:59:59.999` with a 1 millisecond step size. This range is
defined based on a survey of datetime implementations and is based on
the most useful lowest common denominator. Implementations can provide
support for larger ranges and higher precision, but must provide at
least the range and precision defined here.

The `DateTime` literal combines
the `Date` and
`Time` literals and is a subset
of [\[ISO8601\]](#ISO8601):

-   A datetime literal begins with an `@`
    .highlighter-rouge}
-   It uses the **YYYY-MM-DDThh:mm:ss.fff(+\|-)hh:mm** format
-   Timezone offset is optional, but if present the notation
    **(+\|-)hh:mm** is used (so must include both minutes and hours)
-   **Z** is allowed as a synonym for the zero (+00:00) UTC offset.
-   A `T` can be used at the end
    of any date (year, year-month, or year-month-day) to indicate a
    partial DateTime.

The following example illustrates the use of the
`DateTime` literal:

``` fhirpath
@2014-01-25T14:30:14.559
@2014-01-25T14:30:14.559Z // A date time with UTC timezone offset
@2014-01-25T14:30 // A partial DateTime with year, month, day, hour, and minute
@2014-03-25T // A partial DateTime with year, month, and day
@2014-01T // A partial DateTime with year and month
@2014T // A partial DateTime with only the year
```

The suffix `T` is allowed after
a year, year-month, or year-month-day literal because without it, there
would be no way to specify a partial DateTime with only a year, month,
or day; the literal would always result in a Date value.

Consult the [formal grammar](grammar.html) for more details.

#### Quantity

The `Quantity` type represents
quantities with a specified unit, where the `value` component is defined as a
`Decimal`, and the
`unit` element is represented as
a `String` that is required to
be either a valid Unified Code for Units of Measure [\[UCUM\]](#UCUM)
unit or one of the calendar duration keywords, singular or plural.

The `Quantity` literal is a
number (integer or decimal), followed by a (single-quoted) string
representing a valid Unified Code for Units of Measure [\[UCUM\]](#UCUM)
unit or calendar duration keyword. If the value literal is an Integer,
it will be implicitly converted to a Decimal in the resulting Quantity
value:

``` fhirpath
4.5 'mg'
100 '[degF]'
```

> Implementations must respect UCUM units, meaning that they must not
> ignore UCUM units in calculations involving quantities, including
> comparison, conversion, and arithmetic operations. For implementations
> that do not support unit conversion, this means that the calculation
> need only be supported if the units are the same value,
> case-sensitively.
>
> When using [\[UCUM\]](#UCUM) units within FHIRPath, implementations
> shall use case-sensitive comparisons.
>
> Implementations shall support comparison and arithmetic operations on
> quantities with units where the units are the same.
>
> Implementations should support other unit functionality as specified
> by UCUM, including unit conversion.
>
> Implementations that do not support complete UCUM functionality may
> return empty (`{ }`) for
> calculations involving quantities with units where the units are
> different.

##### Time-valued Quantities

For time-valued quantities, in addition to the definite duration UCUM
units, FHIRPath defines calendar duration keywords for calendar duration
units:

  Calendar Duration                                                                                              Unit Representation                                       Relationship to Definite Duration UCUM Unit
  -------------------------------------------------------------------------------------------------------------- --------------------------------------------------------- -----------------------------------------------------
  `year`/`years`                 `'year'`          `~ 1 'a'`
  `month`/`months`               `'month'`         `~ 1 'mo'`
  `week`/`weeks`                 `'week'`          `~ 1 'wk'`
  `day`/`days`                   `'day'`           `~ 1 'd'`
  `hour`/`hours`                 `'hour'`          `~ 1 'h'`
  `minute`/`minutes`             `'minute'`        `~ 1 'min'`
  `second`/`seconds`             `'second'`        `= 1 's'`
  `millisecond`/`milliseconds`   `'millisecond'`   `= 1 'ms'`

For example, the following quantities are *calendar duration*
quantities:

``` fhirpath
1 year
4 days
```

Whereas the following quantities are *definite duration* quantities:

``` fhirpath
1 'a'
4 'd'
```

The table above defines the equality/equivalence relationship between
calendar and definite duration quantities. For example,
`1 year` is not equal to
`1 'a'`, but it is equivalent to
`1 'a'`. See [Date/Time
Arithmetic](#datetime-arithmetic) for more information on using
time-valued quantities in FHIRPath.

### Operators

Expressions can also contain *operators*, like those for mathematical
operations and boolean logic:

``` fhirpath
Appointment.minutesDuration / 60 > 5
MedicationAdministration.wasNotGiven implies MedicationAdministration.reasonNotGiven.exists()
name.given | name.family // union of given and family names
'sir ' + name.given
```

Operators available in FHIRPath are covered in detail in the
[Operations](#operations) section.

### Function Invocations

Finally, FHIRPath supports the notion of functions, which operate on a
collection of values (referred to as the *input collection*), optionally
taking arguments, and return another collection (referred to as the
*output collection*). For example:

``` fhirpath
name.given.substring(0,4)
identifier.where(use = 'official')
```

Since all functions work on input collections, constants will first be
converted to a collection when functions are invoked on constants:

``` fhirpath
(4+5).count()
```

will return `1`, since the input
collection is implicitly a collection with one constant number
`9`.

In general, functions in FHIRPath operate on collections and return new
collections. This property, combined with the syntactic style of *dot
invocation* enables functions to be chained together, creating a
*fluent*-style syntax:

``` fhirpath
Patient.telecom.where(use = 'official').union(Patient.contact.telecom.where(use = 'official')).exists().not()
```

For a complete listing of the functions defined in FHIRPath, refer to
the [Functions](#functions) section.

### Null and empty

There is no literal representation for *null* in FHIRPath. This means
that when, in an underlying data object (i.e. they physical data on
which the implementation is operating) a member is null or missing,
there will simply be no corresponding node for that member in the tree,
e.g. `Patient.name` .fhirpath .highlighter-rouge}
will return an empty collection (not null) if there are no name elements
in the instance.

In expressions, the empty collection is represented as
`{ }`.

#### Propagation of empty results in expressions

FHIRPath functions and operators both propagate empty results, but the
behavior is in general different when the argument to the function or
operator expects a collection (e.g. `select()`, `where()`
and `|` (union)) versus when the
argument to the function or operator takes a single value as input (e.g.
`+` and
`substring()`).

For functions or operators that take a single values as input, this
means in general if the input is empty, then the result will be empty as
well. More specifically:

-   If a single-input operator or function operates on an empty
    collection, the result is an empty collection
-   If a single-input operator or function is passed an empty collection
    as an argument, the result is an empty collection
-   If any operand to a single-input operator or function is an empty
    collection, the result is an empty collection.

For operator or function arguments that expect collections, in general
the empty collection is treated as any other collection would be. For
example, the union (`|`) of an
empty collection with some non-empty collection is that non-empty
collection.

When functions or operators behave differently from these general
principles, (for example the `count()` and `empty()` functions), this is clearly documented in the next
sections.

### Singleton Evaluation of Collections

In general, when a collection is passed as an argument to a function or
operator that expects a single item as input, the collection is
implicitly converted to a singleton as follows:

``` txt
IF the collection contains a single node AND the node's value can be implicitly converted to the expected input type THEN
  The collection evaluates to the value of that single node
ELSE IF the collection contains a single node AND the expected input type is Boolean THEN
  The collection evaluates to true
ELSE IF the collection is empty THEN
  The collection evaluates to an empty collection
ELSE
  The evaluation will end and signal an error to the calling environment
```

For example:

``` fhirpath
Patient.name.family + ', ' + Patient.name.given
```

If the `Patient` instance has a
single `name`, and that name has
a single `given`, then this will
evaluate without any issues. However, if the
`Patient` has multiple
`name` elements, or the single
name has multiple `given`
elements, then it\'s ambiguous which of the elements should be used as
the input to the `+` operator,
and the result is an error.

As another example:

``` fhirpath
Patient.active and Patient.gender and Patient.telecom
```

Assuming the `Patient` instance
has an `active` value of
`true`, a
`gender` of
`female` and a single
`telecom` element, this
expression will result in true. However, consider a different instance
of `Patient` that has an
`active` value of
`true`, a
`gender` of
`male`, and multiple
`telecom` elements, then this
expression will result in an error because of the multiple telecom
elements.

Note that for repeating elements like `telecom` in the above example, the logic *looks* like an
existence check. To avoid confusion and reduce unintended errors,
authors should use the explicit form of these checks when appropriate.
For example, a more explicit rendering of the same logic that more
clearly indicates the actual intent and avoids the run-time error is:

``` fhirpath
Patient.active and Patient.gender and Patient.telecom.count() = 1
```