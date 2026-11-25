# A defined schema takes the place of the inferred schema

A defined schema is processed exactly as an inferred schema is. It becomes the
observed shape the ingest walk is given, which decides only which elements are
present. Types, cardinality and field order come from the definition set
(ADR-0005), so a load always produces a PoF schema. There is no validation step
and no cast: a defined schema that is not a PoF schema yields the PoF schema its
elements select, as inference would from data of that shape.

Annotations in a defined schema are ignored, and the output carries those the
configuration enables (ADR-0003). Primitive metadata groups are part of the base
schema and follow the defined schema. Data already in the PoF layout, read from
Parquet, Delta, tables or datasets, is projected by name (ADR-0002) to the
schema the same walk yields, so every path lands on the same schema.

## Considered options

- **Infer, then project the result to the defined schema.** Rejected: inference
  needs a pass over the data, which a stream cannot provide, so streams would
  need a second path that could diverge from the first.
- **Validate the defined schema and refuse one that is not a PoF schema.**
  Deferred to a separate validator. The load itself stays total.
- **Cast values to the types the defined schema gives.** Rejected: it lets the
  user's schema decide a type, which ADR-0005 reserves for the definition set.

## Consequences

A streaming encode needs no inference pass and works whenever a schema is given.
Content outside the defined schema is dropped without a report, which the
documentation states. The fitted and dense schema modes of the Parquet on FHIR
specification are retired: dense returns as a generated schema, one more source
of defined schemas.
