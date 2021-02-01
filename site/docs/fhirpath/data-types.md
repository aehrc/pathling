---
layout: page
title: Data types
nav_order: 0
parent: FHIRPath
grand_parent: Documentation
---

# Data types

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
'M\u00fcller'
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

## Date

The Date type represents date and partial date values, without a time component.

The Date literal is a subset of
[ISO 8601](https://en.wikipedia.org/wiki/ISO_8601). It uses the `YYYY-MM-DD`
format, though month and day parts are optional.

Examples:

```
@2014-01-25
@2014-01
@2014
```

## Time

The Time type represents time-of-day and partial time-of-day values.

The Time literal uses a subset of
[ISO 8601](https://en.wikipedia.org/wiki/ISO_8601):

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

## DateTime

The DateTime literal combines the [Date](#date) and [Time](#time) literals and
is a subset of [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601). It uses the
`YYYY-MM-DDThh:mm:ss.ffff±hh:mm` format.

Example:

```
@2014-01-25T14:30:14.559
```

## Coding

A [Coding](https://hl7.org/fhir/R4/datatypes.html#Coding) is a representation of
a defined concept using a symbol from a defined
[code system](https://hl7.org/fhir/R4/codesystem.html) - see
[Using Codes in resources](https://hl7.org/fhir/R4/terminologies.html) for more
details.

The Coding literal can take two forms:

- `[system]|[version]|[code]`
- `[system]|[code]`

Not all code systems require the use of a version to unambiguously specify a
code - see
[Versioning Code Systems](https://hl7.org/fhir/R4/codesystem.html#versioning).

You can also optionally single-quote each of the components within the Coding 
literal, in cases where certain characters might otherwise confuse the parser.

<div class="callout warning">The <code>Coding</code> literal is not within the FHIRPath specification, and is currently unique to the Pathling implementation.</div>

Next: [Operators](./operators.html)
