# Annotations are optional fast paths, chosen per schema

An annotation (a decimal's numeric value, a date's range bounds, a quantity's
canonical form) is a derived value stored beside an element. The engine computes
correctly when any or all annotations are absent, and uses one only as a fast
path. Whether to use it is decided from the schema at analysis time, never per
row. Each annotation kind is emitted by default and individually disableable.

Quantities carry two canonical annotations: the Parquet on FHIR specification's
own, at its fixed-point `DECIMAL(38,6)`, so that other consumers find it under
the specification's name; and immediately after it an exact one with the
canonicalised unit, because the fixed-point type makes quantities that differ by
orders of magnitude compare equal, which no engine can compare on.

## Consequences

- Data from any producer of the PoF layout, annotated or not, gives the same
  answers. The suite is run with every annotation disabled to prove it.
- The layout could ship before any annotation existed, and each kind lands
  independently.
- Every annotated operation has two implementations that must agree.
