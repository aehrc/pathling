# The encoding path uses no internal Catalyst API and no HAPI per row

FHIR definition, schema-derivation and encoding code must not depend on internal
Spark Catalyst API, enforced by the build: by a dependency prohibition in
`fhir-schema`, and by an import check in `io`, which needs the wider Spark SQL
dependency. The encoding path contains no expression encoder, no hand-authored
serializer or deserializer expression tree, and no FHIR object in a per-row
plan. That includes HAPI as a per-row parser for XML and bundles: those routes
are reworked so that no HAPI object is built per row. HAPI remains only as the
source of the FHIR structural definitions.

The legacy `encoders` module, which is built on exactly what is prohibited here,
is left in place unmodified at its Maven coordinates, because the legacy export
(ADR-0001) decodes through it.

## Considered options

- **Modify the legacy encoder.** Rejected: it is hand-authored Catalyst
  expression trees against internal, partly deprecated API, which breaks on
  Spark upgrades. This is why the encoder was replaced rather than modified.
- **Keep HAPI as a per-row parser for XML and bundles.** Rejected: it gives
  those routes different behaviour from JSON ingest (no findings, silent
  coercion, whole-job failure on unreadable content, a lost `_x` array), parses
  each document twice, and needs reflection into HAPI to register custom
  resource types.

## Consequences

The query-time expression toolkit is out of scope: it may still use and extend
internal Catalyst API.
