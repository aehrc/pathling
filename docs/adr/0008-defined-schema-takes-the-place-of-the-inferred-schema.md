# A defined schema takes the place of the inferred schema

A defined schema is processed exactly as an inferred schema is. It becomes the
observed shape the ingest walk is given, which decides which elements are
present. Types and cardinality come from the definition set (ADR-0005), so a
load always produces a PoF schema. Field order comes from the defined schema:
the output's fields are in the order the defined schema gives them, at every
depth, so loading with a schema gives that schema. A user who wants canonical
field order canonicalises the schema once, with a function Pathling provides,
before using it. The defined schema is checked against the uber-schema as an
inferred schema is, by the fail-fast check at ingest that #2884 adds once it is
re-scoped, and a defined schema that is not a PoF schema is refused with an
error naming the path. The projection still never casts.

Annotations in a defined schema are ignored, and the output carries those the
configuration enables (ADR-0003). Primitive metadata groups are part of the base
schema and follow the defined schema. Data already in the PoF layout, read from
Parquet, Delta, tables or datasets, is projected by name (ADR-0002) to the
schema the same walk yields, so every path lands on the same schema. Its stored
schema is still checked against the uber-schema as in an inferred read, by the
check #2884 adds: a defined schema does not bypass that check, and a stored
schema that departs from the definitions is handled as #2884 decides.

## Considered options

- **Infer, then project the result to the defined schema.** Rejected: inference
  needs a pass over the data, which a stream cannot provide, so streams would
  need a second path that could diverge from the first.
- **Accept any defined schema and narrow it to the PoF schema its elements
  select.** Rejected: the output would then differ from the schema the user
  gave, silently, and the check that refuses a non-PoF inferred schema, which
  #2884 adds, costs nothing on a schema alone.
- **Cast values to the types the defined schema gives.** Rejected: it lets the
  user's schema decide a type, which ADR-0005 reserves for the definition set.
- **Always produce canonical field order, whatever the defined schema's order.**
  Rejected: a schema taken from an existing table is often not canonical,
  because storage keeps its first writer's order and schema evolution adds
  fields last. Matching that table by position (`union`, `INSERT INTO …
SELECT`, struct casts, row hashes) would then need a reorder such as
  `df.to(schema)` after every load. Order never affects correctness inside
  Pathling, which matches fields by name everywhere, so the order can follow the
  user. Canonicalising is done once, when a schema is written, instead.

## Consequences

A streaming encode needs no inference pass and works whenever a schema is given.
A table loaded with a non-canonical schema is not in canonical order, so two
lakes loaded with schemas in different orders can differ physically; matching
them by name, as Pathling does, is unaffected.
Content outside the defined schema is dropped without a report, which the
documentation states. NDJSON, bundle, XML and streaming reads with a defined
schema run no inference, so they do not check the data: content that a read
without a schema would fail on, by the check #2884 adds, is dropped silently
instead. The fitted
and dense schema modes of the Parquet on FHIR specification are retired: dense
returns as a generated schema, one more source of defined schemas.
