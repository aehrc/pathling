# Data model: what each shape reads as

Each row says what ingest stores and what the engine answers, for JSON, for XML where it applies, and for
Parquet written by any producer. "Absent" means that ingest does not store the element and the engine reads it
as an empty collection. The levels are those of [`docs/conformance.md`](../../docs/conformance.md).

## Level 2: encoding

| Content                                                              | Reads as                                             | Requirement    |
| -------------------------------------------------------------------- | ---------------------------------------------------- | -------------- |
| `null` as an element's value                                         | absent                                               | FR-004         |
| `null` in a primitive array, paired with an id or extensions in `_x` | a positional null with its metadata                  | FR-007         |
| `null` in a primitive array, with nothing paired                     | removed from the array                               | FR-007         |
| `null` in a complex array                                            | removed from the array                               | FR-007         |
| `[]`                                                                 | absent                                               | FR-004         |
| `{}`, or a stored all-null structure                                 | absent (OQ-A, target)                                | FR-004, FR-014 |
| `""`, for every type                                                 | absent                                               | FR-004         |
| more `_x` groups than `x` values                                     | each extra group a positional null with its metadata | FR-008         |
| `x: []` beside a `_x` holding groups                                 | each group a positional null with its metadata       | FR-008         |

## Level 3: structure

| Content                                                                                | Reads as                                   | Requirement    |
| -------------------------------------------------------------------------------------- | ------------------------------------------ | -------------- |
| a JSON type the safe cast converts (`123` for a string, `"5"` for an integer)          | the converted value; exact text not kept   | FR-005, FR-020 |
| a JSON type the safe cast does not convert (`"abc"` for an integer)                    | absent                                     | FR-005         |
| an array where a singular element is declared                                          | absent; in a widened column, its JSON text | FR-006, FR-020 |
| a single value or object where an array is declared                                    | absent                                     | FR-006         |
| an object where a primitive is declared                                                | absent                                     | FR-006         |
| an element the definitions do not describe                                             | left out, as today                         | —              |
| a Parquet column of another type, a missing or extra field, a malformed metadata group | OQ-B                                       | FR-017         |

## Level 4: value

| Content                                                   | Ingest                       | Engine operation that parses it | Other operations | Requirement            |
| --------------------------------------------------------- | ---------------------------- | ------------------------------- | ---------------- | ---------------------- |
| an invalid date, dateTime, instant or time                | text stored, null annotation | empty                           | OQ-C             | FR-010, FR-011, FR-012 |
| a decimal the safe cast does not read                     | text stored, null annotation | empty                           | OQ-C             | FR-010, FR-011         |
| a decimal the safe cast reads but FHIR forbids (`" 1.5"`) | the number                   | the number                      | the text         | FR-020                 |
| a text-only type breaking its regex                       | stored as received           | as received                     | as received      | FR-009                 |
| a stored annotation that disagrees with its value         | —                            | OQ-B                            | —                | FR-017                 |

## Exceptions (conformant, not stored)

`contained`, `Bundle`, an inline abstract `Resource`, content beyond a dense schema's bounds,
`base64Binary` whitespace, byte identity, and the decimal precision losses of spec 001's OQ-1 (FR-002).
