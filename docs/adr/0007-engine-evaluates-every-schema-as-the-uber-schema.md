# The engine evaluates every PoF schema as though it were the uber-schema

A query's result depends on the data alone, never on which subset of the
uber-schema a resource table's schema carries. An absent element is
indistinguishable from a present element that is empty everywhere, whatever its
position: a singular or repeating primitive, a singular or repeating complex
element, a choice type, a primitive metadata group or an annotation, at any
depth, and on either or both sides of a combination.

Inferred and defined schemas are therefore the same thing to the engine, as is
any schema a later source produces, such as a generated schema. Correctness is
established by testing absence at every kind of position, and by testing that
each way of loading data produces the schema it should, not by running the
conformance suites once for every way of obtaining a schema.

## Considered options

- **Schema modes with their own guarantees**, as the Parquet on FHIR
  specification first had (fitted and dense, with a guarantee that held only
  for dense). Rejected: each new source of schemas would add a mode and a
  guarantee to keep, and a user-defined schema can be any subset of the
  uber-schema, so modes no longer partition the cases.
- **Mandatory parity runs** of the FHIRPath, SQL on FHIR and conformance suites
  for each schema source. Rejected as the default because they multiply the
  build time of the slowest suites. Kept as an opt-in profile that is not part
  of the regular build.

## Consequences

Every engine feature needs tests over absent elements at the positions it
reaches. A difference in results between two schemas holding the same content is
an engine defect, never a schema defect.
