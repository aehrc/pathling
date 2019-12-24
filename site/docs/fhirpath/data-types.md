---
layout: page
title: Data types
nav_order: 1
parent: FHIRPath
grand_parent: Documentation
---

# Data types

Source: [Literals](https://hl7.org/fhirpath/2018Sep/index.html#literals) and
[Using FHIR types in expressions](https://hl7.org/fhir/R4/fhirpath.html#types)

The FHIRPath implementation within Pathling supports the following types of
literal expressions:

- [Boolean](#boolean)
- [String](#string)
- [Integer](#integer)
- [Decimal](#decimal)
- [Date](#date)
- [DateTime](#datetime)
- [Time](#time)
- [Coding](#coding)

<div class="callout warning">The <code>Quantity</code> literal in the FHIRPath specification is not currently supported.</div>

<div class="callout warning">The <code>Coding</code> literal is not within the FHIRPath specification, and is currently unique to the Pathling implementation.</div>

## Boolean

The Boolean type represents the logical Boolean values true and false. These
values are used as the result of comparisons, and can be combined using logical
operators such as and and or.

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
'M\u00fcller'
```

## Integer

The Integer type represents whole numbers in the range -2<sup>31</sup> to
2<sup>31</sup>-1.

Examples:

```
352
-14
```

## Decimal

The Decimal type represents real values in the range
-10<sup>28</sup>-10<sup>-8</sup> to 10<sup>28</sup>-10<sup>-8</sup> with a step
size of 10<sup>-8</sup>.

Note that decimal literals cannot use exponential notation.

Examples:

```
14.25
-3.333
```

## Date

The Date type represents date and partial date values in the range `@0001-01-01`
to `@9999-12-31` with a 1 day step size.

The Date literal is a subset of ISO8601:

- It uses the `YYYY-MM-DD` format, though month and day parts are optional
- Week dates and ordinal dates are not allowed
- Years must be present (`-MM-DD` is not a valid Date in FHIRPath)
- Months must be present if a day is present
- The date may be followed by a time as described in the next section.
- Consult the [formal grammar](https://hl7.org/fhirpath/2018Sep/grammar.html)
  for more details.

Examples:

```
@2014-01-25
@2014-01
@2014
```

## Time

The Time type represents time-of-day and partial time-of-day values in the range
`@T00:00:00.0` to `@T23:59:59.999` with a step size of 1 millisecond.

The Time literal uses a subset of ISO8601:

- A time begins with a `@T`
- It uses the `Thh:mm:ss.ffff±hh:mm` format, though minute, second, millisecond
  parts are optional
- Timezone is optional, but if present the notation `±hh:mm` is used (so must
  include both minutes and hours)
- `Z` is allowed as a synonym for the zero (+00:00) UTC offset.

Examples:

```
@T07:30:14.559-07:00
@T14:30:14.559Z
@T14:30
@T14
```

Consult the [formal grammar](https://hl7.org/fhirpath/2018Sep/grammar.html) for
more details.

## DateTime

The DateTime type represents date/time and partial date/time values in the range
`@0001-01-01T00:00:00.0` to `@9999-12-31T23:59:59.999` with a 1 millisecond step
size.

The DateTime literal combines the Date and Time literals and is a subset of
ISO8601:

- It uses the `YYYY-MM-DDThh:mm:ss.ffff±hh:mm` format

```
@2014-01-25T14:30:14.559
```

Consult the [formal grammar](https://hl7.org/fhirpath/2018Sep/grammar.html) for
more details.

## Coding

A [Coding](https://www.hl7.org/fhir/datatypes.html#Coding) is a representation
of a defined concept using a symbol from a defined "code system" - see
[Using Codes in resources](https://www.hl7.org/fhir/terminologies.html) for more
details.

The Coding literal can take two forms:

- `[system]|[version]|[code]`
- `[system]|[code]`

Not all code systems require the use of a version to unambiguously specify a
code - see
[Versioning Code Systems](https://www.hl7.org/fhir/codesystem.html#versioning).

Next: [Operators](./operators.html)
