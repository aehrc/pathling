# Element types and cardinality come from the definitions, never from the data

The stored schema is derived from the FHIR definition set, which alone decides
every element's type and whether it repeats. The data decides only which
elements a fitted schema carries. The stored type of an element is never taken
from the data: content whose JSON type contradicts the definitions is not
stored as that element, and a finding reports it.

An absent element is represented by the type the definitions give it where that
type is unambiguous, and by the bottom type where a concrete shape would
over-constrain later combination: a singular primitive takes the definition's
type, a repeating primitive an array of it, a singular complex element the
bottom type (`NullType`), and a repeating complex element an array of the bottom
type.

## Considered options

- **Infer types from the data**, as a generic JSON-to-Parquet pipeline would.
  Rejected: the same element would get a different type in different files (an
  integer here, a double there; a singleton object here, an array there), and
  the engine could not tell a repeating element from a singular one.
- **A minimal concrete struct for an absent complex element.** Rejected: it fails
  to combine with a populated value of the same type, which a bottom type
  reconciles into (ADR-0002).

## Consequences

An absent complex element yields a column that cannot be written to Parquet and
is omitted from JSON output. This is documented.
