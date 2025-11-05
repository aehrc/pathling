---
sidebar_position: 2
---

# Data types

The FHIRPath implementation within Pathling supports the following types of
literal expressions:

See also: [Literals](https://hl7.org/fhirpath/#literals) and
[Using FHIR types in expressions](https://hl7.org/fhir/R4/fhirpath.html#types)

## Boolean

The Boolean type represents the logical Boolean values `true` and `false`.

Examples:

```
true
false
```

## String

String literals are surrounded by single-quotes and may use `\`-escapes to
escape quotes and represent Unicode characters:

- Unicode characters may be escaped using \u followed by four hex digits.
- Additional escapes are those supported in JSON:
    - `\\` (backslash),
    - `\/` (slash),
    - `\f` (form feed - `\u000c`),
    - `\n` (newline - `\u000a`),
    - `\r` (carriage return - `\u000d`),
    - `\t` (tab - `\u0009`)
    - <code>\``</code> (backtick)
    - `\'` (single-quote)

Unicode is supported in both string literals and delimited identifiers.

Examples:

```
'test string'
'urn:oid:3.4.5.6.7.8'
'M\u00fcller'           // Includes a Unicode character, evaluates to Müller
```

## Integer

The Integer type represents whole numbers.

Examples:

```
352
-14
```

## Decimal

The Decimal type represents real values.

Examples:

```
14.25
-3.333
```

:::note
The implementation of Decimal within Pathling supports a precision of 32 and a
scale of 6.
:::

## DateTime

The DateTime literal combines the [Date](#date) and [Time](#time) literals and
is a subset of [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601). It uses the
`YYYY-MM-DDThh:mm:ss.ffff±hh:mm` format. `Z` is allowed as a synonym for the
zero (`+00:00`) UTC offset.

Time zone is optional - if it is omitted, the system-configured time zone will
be assumed. Seconds and milliseconds precision are supported. Hours precision,
minutes precision and partial DateTime values (ending with `T`) are not
supported.

Example:

```
@2014-01-25T14:30:14         // Seconds precision
@2014-01-25T14:30:14+10:00   // Seconds precision with UTC+10 timezone offset
@2014-01-25T14:30:14.559     // Milliseconds precision
@2014-01-25T14:30:14.559Z    // Milliseconds precision with UTC timezone offset
```

## Date

The Date type represents date and partial date values, without a time component.

The Date literal is a subset of
[ISO 8601](https://en.wikipedia.org/wiki/ISO_8601). It uses the `YYYY-MM-DD`
format, though month and day parts are optional.

Some operations implicitly convert Date values to DateTime values, such as
comparison and arithmetic. Note that the Date will be assumed to be in the
system-configured time zone in these instances.

Examples:

```
@2014-01-25    // Year, month and day
@2014-01       // Year and month only
@2014          // Year only
```

## Time

The Time type represents time-of-day and partial time-of-day values.

The Time literal uses a subset of
[ISO 8601](https://en.wikipedia.org/wiki/ISO_8601):

- A time begins with a `@T`
- It uses the `Thh:mm:ss.fff` format, minutes, seconds and milliseconds are
  optional

Examples:

```
@T07:30:14.350    // Milliseconds precision
@T07:30:14        // Seconds precision
@T14:30:14        // Minutes precision
@T14              // Hours precision
```

## Quantity

The Quantity type represents quantities with a specified unit, where the value
component is defined as a Decimal, and the unit element is represented as a
String that is required to be either a valid
[Unified Code for Units of Measure (UCUM)](https://ucum.org/ucum.html) unit or
one of the calendar duration keywords, singular or plural.

The Quantity literal is a number (integer or decimal), followed by a
(single-quoted) string representing a valid UCUM unit or calendar duration
keyword. If the value literal is an Integer, it will be implicitly converted to
a Decimal in the resulting Quantity value.

The calendar duration keywords that are supported are:

- `year` / `years`
- `month` / `months`
- `week` / `weeks`
- `day` / `days`
- `hour` / `hours`
- `minute` / `minutes`
- `second` / `seconds`
- `millisecond` / `milliseconds`

Example:

```
4.5 'mg'      // UCUM Quantity of 4.5 mg
100 '[degF]'  // UCUM Quantity of 100 degrees Fahrenheit
6 months      // Calendar duration of 6 months
30 days       // Calendar duration of 30 days
```

See: [Quantity](https://hl7.org/fhirpath/#quantity)

## Coding

A [Coding](https://hl7.org/fhir/R4/datatypes.html#Coding) is a representation of
a defined concept using a symbol from a defined
[code system](https://hl7.org/fhir/R4/codesystem.html) - see
[Using Codes in resources](https://hl7.org/fhir/R4/terminologies.html) for more
details.

The Coding literal comprises a minimum of `system` and `code`, as well as
optional `version`, `display`, `userSelected` components:

```
<system>|<code>[|<version>][|<display>[|<userSelected>]]]
```

Not all code systems require the use of a version to unambiguously specify a
code - see
[Versioning Code Systems](https://hl7.org/fhir/R4/codesystem.html#versioning).

You can also optionally single-quote each of the components within the Coding
literal, in cases where certain characters might otherwise confuse the parser.

Examples:

```
http://snomed.info/sct|52101004
http://snomed.info/sct|52101004||Present
http://terminology.hl7.org/CodeSystem/condition-category|problem-list-item|4.0.1|'Problem List Item'
http://snomed.info/sct|'397956004 |Prosthetic arthroplasty of the hip|: 363704007 |Procedure site| = ( 24136001 |Hip joint structure|: 272741003 |Laterality| =  7771000 |Left| )'
```

:::note
The Coding literal is not within the FHIRPath specification, and is currently
unique to the Pathling implementation.
:::

## Materializable types

There is a subset of all possible FHIR types that can be "materialized", i.e.
used as the result of an aggregation or grouping expression in
the [aggregate](/docs/7.2.0/server/operations/aggregate)
operation, or a column expression within
the [extract](/docs/7.2.0/server/operations/extract)
operation. These types are:

- [Boolean](#boolean)
- [String](#string)
- [Integer](#integer)
- [Decimal](#decimal)
- [Date](#date)
- [DateTime](#datetime)
- [Time](#time)
- [Coding](#coding)
