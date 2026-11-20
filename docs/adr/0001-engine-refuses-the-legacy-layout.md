# The engine refuses the legacy layout; migration is by export and re-import

The query engine reads only the PoF layout. Data in the legacy
layout (written by the Catalyst-based `encoders` module) is refused at the
source boundary, with no supported route into the engine. We retire the
engine's legacy-layout reader before any further layout features land (M5
onward), rather than at the end of the programme as originally planned.

Migration is by export and re-import. A standalone module, depending only on
the retained `encoders` module, decodes a legacy-layout Parquet or Delta table
to FHIR NDJSON; the ordinary NDJSON source re-imports it. The module is both a
reusable component (which the server's startup migration calls) and a CLI. It
is not part of `library-runtime` or the Python and R bindings, so the public
library API carries no legacy-layout surface.

## Considered options

- **Route legacy-layout data through the engine for a while** (decision 101's
  first option). Rejected: it keeps the normalisation branches, a second test
  dimension and a second CI run alive through M5 to M7, and makes them product
  code needing product-grade coverage. Since M5 to M7 release together, keeping
  the reader buys users nothing that the export tool does not.
- **Convert in SQL, legacy schema to PoF schema, as a public API.** Rejected
  as too costly: it is a second implementation of the layout mapping.
- **Leave migration to the server.** Rejected: library users with
  legacy-layout warehouses need a route too, and the server can reuse the same
  component.

## Consequences

- The conversion is lossy. The legacy layout stored decimals at a fixed scale,
  and content beyond the old encoder's nesting, extension and open-type bounds
  was never stored. A re-import from source carries the losslessness guarantee;
  an export of legacy-layout data does not.
- Layout detection outlives the reader: it is what makes the refusal
  actionable. Legacy-layout data that bypasses it (the per-source opt-out,
  `DatasetSource`, datasets built with `getFhirEncoders()`) is unsupported, and
  the engine fails loudly, once per schema, where a stored column's type
  contradicts the PoF layout rather than answering from it. _Amended by
  decision 132 (2026-10-15)_: the engine makes no such check. Data that bypasses
  detection is unsupported and the result is undefined; detection is made exact
  instead, by a versioned layout column
  ([ADR-0006](0006-layout-identified-by-a-versioned-column.md)).
- The before-and-after oracle the legacy-layout test arm provided is gone.
  Expected values are literals or come from the conformance suites.
- The server's existing startup migration (additive schema drift against the
  encoder schema) cannot carry a change of layout. The intended route, not yet
  agreed with the server, is to call the export component, re-read through
  `FhirReader` and overwrite each table.
