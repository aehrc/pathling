# The PoF layout is identified by a versioned layout column

Every PoF dataset Pathling writes carries a layout column at its root,
`__layout_v<n>`, where `<n>` is the version of the layout: 1 for the first
release of the PoF layout. The column is a nullable boolean whose every value
is null. Detection recognises it by its name and type alone, so it reads only
the schema and identifies an empty table as well as a full one. A version the
release supports is read; a higher version is refused as written by a newer
release; a dataset with no layout column is classified by structural markers as
before.

This replaces a second line of defence inside the engine. The engine makes no
check for the legacy layout beyond detection, and data that bypasses detection
is unsupported (decision 132, amending
[ADR-0001](0001-engine-refuses-the-legacy-layout.md)).

## Considered options

- **Spark field metadata on a root column.** Rejected: it is stored with the
  schema in Parquet and Delta, but some Spark expressions drop it, and tools
  other than Spark do not write it. A column survives wherever a column does.
- **The version in the column's value.** Rejected: reading it takes a Spark
  job per table, and an empty table has no value to read.
- **Delta table properties.** Rejected: Delta only, with no Parquet
  equivalent.
- **Key-value metadata in the Parquet footer.** Rejected: Spark's writer
  offers no way to set it.
- **A file beside the data.** Rejected: it does not exist for a catalog table
  or a single file, and is lost when files are copied.
- **A check in the engine where a legacy-typed column is traversed** (the
  withdrawn FR-053). Rejected: it keeps legacy-layout knowledge in the engine
  indefinitely, and cannot be complete, because the legacy layout's
  extensions are not distinguishable by type.

## Consequences

- The column is not an annotation. It is derived from nothing, always written,
  outside the annotation switches, and never pruned by the fitted schema,
  although it holds no value.
- It is visible to anyone who reads the tables directly, for example with
  `SELECT *`. It is never emitted as FHIR content, and a source key named like
  it is reported as undescribed content.
- A later change to the layout raises the version. A release refuses data
  written by a layout newer than it knows, rather than reading it as though it
  were its own.
- A projection that leaves the column out, and data written by other tools,
  carry no layout column. They are classified by markers, and accepted where no
  marker contradicts the PoF layout, as before.
