# Catalog-table appends when batch schemas differ

Under the pruned storage layout, two batches of the same resource type can have
different schemas. A later batch can be **narrower**, lacking fields the table
has, or **wider**, carrying fields the table lacks. Either can happen at the
top level, inside a struct, or inside an `array<struct>`. This spike measures
what a `CatalogSink`-style write does with each case:

```java
dataset.write().mode(mode).format(f).saveAsTable(name)
```

It covers Parquet, Delta and the session default format, and checks the other
public routes into a catalog table.

**Result, in short.**

- **A Parquet catalog table takes neither a narrower nor a wider batch through
  `saveAsTable`, at any level.** It refuses every one. `mergeSchema=true` on the
  writer has no effect. The refusal happens at analysis, so nothing is written
  and no rows are lost.
- **A narrower batch can be absorbed if the writer pads it first.**
  `spark.table(t).limit(0).unionByName(batch, true)` fills the missing fields,
  including those inside `array<struct>`, and the padded batch then appends
  cleanly. On a Parquet table, no Spark write route fills a missing nested
  field itself. Only `INSERT INTO … BY NAME` fills a missing top-level column.
- **A wider batch has only one public route into a Parquet table, and it is
  partial.** `ALTER TABLE ADD COLUMNS` works for a new top-level column. Every
  public way of adding a nested field is refused: a qualified `ADD COLUMNS`,
  `REPLACE COLUMNS`, and `ALTER/CHANGE COLUMN`.
- **An external Parquet table can be widened by dropping it and re-creating it
  over its own location**, with merging enabled. This is the only measured way
  to expose a new nested field. Files have to be written straight into the
  location, because `saveAsTable` rejects them, and the column order of the
  re-created table follows the file listing.
- **A Delta catalog append takes narrower batches at every level, with or
  without `mergeSchema`.** The missing fields read back as null.
- **A Delta catalog append widens at every level, top-level, struct and
  `array<struct>`, if and only if `mergeSchema=true`.** Without it the append
  is refused. This includes a mixed batch, which lacks one field and adds
  another at the same level. The path-based Delta write that `DeltaSink` uses
  behaves identically, with the same values read back.
- **Reordered fields are resolved by name in both formats**, at the top level,
  inside a struct, and inside an `array<struct>`. Every value lands in the
  right field.

## Conditions

- Spark 4.0.2 (Scala 2.13) and Delta 4.0.0 (`delta-spark_2.13-4.0.0.jar`), which
  are `pathling.sparkVersion` and `pathling.deltaVersion` in the root `pom.xml`.
  The probe runs on the third-party test classpath of `library-api`, on
  Java 21.
- The session is configured like `TestHelpers.sparkBuilder()`:
    - `local[2]`;
    - `DeltaSparkSessionExtension`;
    - `DeltaCatalog` as `spark_catalog`;
    - a temporary warehouse directory.
- Run twice:
    - **in-memory**: `spark.sql.catalogImplementation=in-memory`, which is
      `TestHelpers.spark()`;
    - **hive**: `spark.sql.catalogImplementation=hive` with a Derby metastore,
      which is `DataSourcesTest`, the test that covers `CatalogSink`.
- `spark.sql.sources.default` is `parquet` even with `DeltaCatalog`, so the
  "default" rows below are Parquet tables. The recorded `Provider` confirms this.
- ANSI is on, and `spark.sql.storeAssignmentPolicy=ANSI`, the Spark 4 defaults.
- Measured at commit `e8d1189420`.

**The two catalogs agree on every outcome.** The transcripts differ in only
three places, none of them caused by the catalog:

- the column order of a schema merged across files, in case 6a's reference
  read, which follows the file listing;
- the schema of a table re-created without merging, in case 6b, which follows
  whichever file inference samples (see below);
- expression IDs in one analysed plan, printed inside an error message.

The probe was run three times on the in-memory catalog and twice on hive. The
mixed cases (M) were added for the last run, so they ran once per catalog.
Apart from those three places, the outcomes were identical every time.

### Data

The table is created by a first batch with this schema:

```
id string, gender string,
meta struct<versionId string, source string>,
name array<struct<family string, text string>>
```

Each case then appends one batch with `id = 'b'`:

| Case | Batch differs from the table by                                   |
| ---- | ----------------------------------------------------------------- |
| 1    | no `gender` (narrower, top level)                                 |
| 2s   | `meta` lacks `source` (narrower, struct)                          |
| 2a   | `name` element lacks `text` (narrower, `array<struct>`)           |
| 3    | extra `birthDate` (wider, top level)                              |
| 4s   | `meta` has an extra `lastUpdated` (wider, struct)                 |
| 4a   | `name` element has an extra `use` (wider, `array<struct>`)        |
| Mt   | no `gender`, extra `birthDate` (mixed, top level)                 |
| Ms   | `meta` lacks `source`, has `lastUpdated` (mixed, struct)          |
| Ma   | `name` element lacks `text`, has `use` (mixed, `array<struct>`)   |
| 8s   | `meta` as `struct<source, versionId>` (reordered, struct)         |
| 8a   | `name` element as `struct<text, family>` (reordered, `array<struct>`) |
| 8t   | top-level columns as `name, meta, gender, id` (reordered)         |

Every table is new, so cases do not interact. A null field is absent from the
JSON rows in the transcript, because `toJSON` drops nulls.

## Results: `saveAsTable` append (cases 1–5, M, 8) and path-based Delta (case 9)

Parquet and "default" (Parquet) are identical in every cell. Each cell gives
the result without `mergeSchema` / with `.option("mergeSchema","true")` on the
writer.

| Case | Parquet / default catalog table                       | Delta catalog table               | Delta by path (`DeltaSink`)       |
| ---- | ----------------------------------------------------- | --------------------------------- | --------------------------------- |
| 1    | FAIL `_LEGACY_ERROR_TEMP_1161` / same                 | OK, `gender` null / same          | OK, `gender` null / same          |
| 2s   | FAIL `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA` (`meta`.`source`) / same | OK, `meta.source` null / same | OK / same |
| 2a   | FAIL `INCOMPATIBLE_DATA_FOR_TABLE.CANNOT_FIND_DATA` (`name`.`element`.`text`) / same | OK, `text` null / same | OK / same |
| 3    | FAIL `_LEGACY_ERROR_TEMP_1161` / same                 | FAIL `_LEGACY_ERROR_TEMP_DELTA_0007` / **OK, `birthDate` added** | FAIL / **OK, added** |
| 4s   | FAIL `INCOMPATIBLE_DATA_FOR_TABLE.EXTRA_STRUCT_FIELDS` (`lastUpdated` to `meta`) / same | FAIL `_LEGACY_ERROR_TEMP_DELTA_0007` / **OK, `meta.lastUpdated` added** | FAIL / **OK, added** |
| 4a   | FAIL `INCOMPATIBLE_DATA_FOR_TABLE.EXTRA_STRUCT_FIELDS` (`use` to `name`.`element`) / same | FAIL `_LEGACY_ERROR_TEMP_DELTA_0007` / **OK, `name.element.use` added** | FAIL / **OK, added** |
| Mt   | FAIL `_LEGACY_ERROR_TEMP_1162` (*Cannot resolve 'gender' given input columns*) / same | FAIL `_LEGACY_ERROR_TEMP_DELTA_0007` / **OK, `birthDate` added, `gender` null** | FAIL / **OK, same** |
| Ms   | FAIL `CANNOT_FIND_DATA` (`meta`.`source`) / same      | FAIL `_LEGACY_ERROR_TEMP_DELTA_0007` / **OK, `lastUpdated` added, `source` null** | FAIL / **OK, same** |
| Ma   | FAIL `CANNOT_FIND_DATA` (`name`.`element`.`text`) / same | FAIL `_LEGACY_ERROR_TEMP_DELTA_0007` / **OK, `use` added, `text` null** | FAIL / **OK, same** |
| 8s   | OK, values by name / same                             | OK, by name / same                | OK, by name / same                |
| 8a   | OK, values by name / same                             | OK, by name / same                | OK, by name / same                |
| 8t   | OK, values by name / same                             | OK, by name / same                | OK, by name / same                |

All exceptions are `org.apache.spark.sql.AnalysisException`, or
`org.apache.spark.sql.delta.DeltaAnalysisException` for Delta. The messages in
full:

- `_LEGACY_ERROR_TEMP_1161`: *The column number of the existing table … doesn't
  match the data schema …*
- `_LEGACY_ERROR_TEMP_DELTA_0007`: *A schema mismatch detected when writing to
  the Delta table … To enable schema migration using DataFrameWriter or
  DataStreamWriter, please set: '.option("mergeSchema", "true")'*

Every failed append left the table with its original schema and its one
original row. Every successful append left both rows, with no value lost or
moved:

- after a Delta widening, the first row reads the new field as null;
- after a Delta narrower append, the second row reads the missing field as
  null.

For the reordered cases, row `b` reads back as `versionId=2, source=s2`,
`family=F2, text=T2` in both formats. That is placement by name. Placement by
position would have swapped the values.

## Case 6: files written straight into a Parquet table's location

The base files are written, and then the table is created in one of four ways.
After that:

- a wider batch is written with `df.write().mode("append").parquet(location)`;
  it carries `birthDate`, `meta.lastUpdated` and `name.element.use`;
- a narrower batch is written the same way; it lacks `gender`, `meta.source` and
  `name.element.text`;
- the table is read after `REFRESH TABLE` and after
  `spark.catalog.refreshTable`.

| Variant | How the table was created                                                     | `SHOW CREATE TABLE` keeps the option | Schema after `REFRESH TABLE` / `refreshTable` |
| ------- | ----------------------------------------------------------------------------- | ------------------------------------ | --------------------------------------------- |
| 6a      | `CREATE TABLE … (declared schema) USING parquet OPTIONS (mergeSchema 'true') LOCATION …` | yes                     | unchanged, original                           |
| 6b      | `CREATE TABLE … USING parquet OPTIONS (mergeSchema 'true') LOCATION …`, schema inferred | yes                       | unchanged, the schema inferred at creation    |
| 6c      | `saveAsTable` with `.option("path", …)` and `.option("mergeSchema","true")`    | yes                                  | unchanged, original                           |
| 6d      | managed, `saveAsTable` with `.option("mergeSchema","true")`                   | yes                                  | unchanged, original                           |

In every variant:

- **No row is lost.** All three rows (base, wide and narrow) are returned.
- **The narrower file's missing fields read as null**, at the top level, in the
  struct, and in the array element.
- **The wider file's extra fields are invisible.** The stored table schema is
  served, and the extra fields are dropped from the read.
- **Neither refresh re-infers the schema**, even with `mergeSchema` recorded in
  the table's options.
- `spark.catalog.recoverPartitions` refuses with `NOT_A_PARTITIONED_TABLE`.
- For reference, `spark.read().option("mergeSchema","true").parquet(location)`
  returns the merged schema with every new field and its values.

### Drop and re-create (6b only, external table)

The table is dropped and created again over the same location, with no
declared schema:

| Re-creation                                                         | Resulting schema                                                                                                                  |
| ------------------------------------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| with `OPTIONS (mergeSchema 'true')`, session `spark.sql.parquet.mergeSchema=false` | **merged**: `id, meta<versionId,source,lastUpdated>, name<family,text,use>, gender, birthDate`, with all values |
| no option, session default (`false`)                                | **one file's schema, and the file varies from run to run.** In four runs: the narrow file twice (`id, meta<versionId>, name<family>`), the base file once, and the wide file once |
| no option, session `spark.sql.parquet.mergeSchema=true`             | **merged**, as in the first row                                                                                                 |

So the table option, or the session setting, is what makes re-creation merge.
Without either, the table takes the schema of whichever file inference samples,
and which file that is is not stable. When it samples a narrower file, data
that is still on disk silently disappears from the read: `gender`, `source` and
`text` in the narrow-file runs.

The merged column order is not the original order: `gender` moved after
`name`. The two catalog runs ordered the same merge differently in case 6a's
reference read. **The column order of a table re-created this way depends on
the file listing.**

The following is reasoning, not measurement. The route works only for external
tables, because dropping a managed table deletes its data. The table's schema
is also only as current as its last re-creation.

## Case 7: `ALTER TABLE`

Each statement is run against a fresh table. Where it succeeds, three batches
are then appended through `saveAsTable`:

- `3-wide-top`;
- a batch wider in every place (`wide-all`);
- a batch narrower in every place (`narrow-all`).

| Statement                                                                  | Parquet                                                       | Delta                                         |
| -------------------------------------------------------------------------- | ------------------------------------------------------------- | --------------------------------------------- |
| `ADD COLUMNS (birthDate STRING)`                                           | OK                                                            | OK                                            |
| `ADD COLUMNS (meta.lastUpdated STRING)`                                    | FAIL `UNSUPPORTED_FEATURE.TABLE_OPERATION` (*does not support ADD COLUMN with qualified column*) | OK, nested field added |
| `ADD COLUMNS (name.element.use STRING)`                                    | FAIL `UNSUPPORTED_FEATURE.TABLE_OPERATION`, same message       | OK, field added inside the array element      |
| `REPLACE COLUMNS (… widened nested types …)`                               | FAIL `UNSUPPORTED_FEATURE.TABLE_OPERATION` (*does not support REPLACE COLUMNS*) | OK, every widening applied |
| `ALTER COLUMN meta TYPE STRUCT<…, lastUpdated>`                            | FAIL `NOT_SUPPORTED_CHANGE_COLUMN`                             | FAIL `CANNOT_UPDATE_FIELD.STRUCT_TYPE` (*Update a struct by updating its fields*) |
| `CHANGE COLUMN meta meta STRUCT<…, lastUpdated>`                           | FAIL `NOT_SUPPORTED_CHANGE_COLUMN`                             | FAIL `CANNOT_UPDATE_FIELD.STRUCT_TYPE`        |

After a Parquet table's `ADD COLUMNS (birthDate STRING)`:

- `3-wide-top` appends, and `birthDate` is stored;
- `wide-all` still fails on `meta.lastUpdated`;
- `narrow-all` still fails on the column count.

After a Delta table's `REPLACE COLUMNS`, `wide-all` appends without
`mergeSchema`. After the other Delta `ADD COLUMNS` statements, `wide-all` still
needs `mergeSchema` for the fields not yet added. `narrow-all` appends to every
Delta table.

**On a Parquet catalog table, a nested field cannot be added by any public DDL
measured.** The one widening `ALTER` that Parquet accepts is a new top-level
column.

## Case 10: other public write routes

These were measured on the in-memory and hive catalogs, with the same results.

| Route                                                                  | Parquet                                                                                                  | Delta                                                    |
| ---------------------------------------------------------------------- | -------------------------------------------------------------------------------------------------------- | -------------------------------------------------------- |
| `INSERT INTO t BY NAME`, narrower: 1 / 2s / 2a                         | **OK** (`gender` null) / FAIL `CANNOT_FIND_DATA` / FAIL `CANNOT_FIND_DATA`                               | OK / FAIL `DELTA_INSERT_COLUMN_ARITY_MISMATCH` / FAIL `DATATYPE_MISMATCH.CAST_WITHOUT_SUGGESTION` |
| `writeTo(t).append()`, narrower: 1 / 2s / 2a                           | FAIL *Cannot write into v1 table* (all)                                                                  | OK / OK / OK, missing fields null                        |
| Pad first: `spark.table(t).limit(0).unionByName(batch, true)`, then `saveAsTable` append; narrower: 1 / 2s / 2a | **OK / OK / OK**, missing fields null, including inside `array<struct>` | OK / OK / OK                                     |
| `writeTo(t).append()`, wider: 3 / 4s / 4a                              | FAIL *Cannot write into v1 table* (all)                                                                  | FAIL `_LEGACY_ERROR_TEMP_DELTA_0007` (all)               |
| `writeTo(t).option("mergeSchema","true").append()`, wider: 3 / 4s / 4a | FAIL *Cannot write into v1 table* (all)                                                                  | **OK / OK / OK**, widened                                |
| `INSERT INTO t BY NAME`, wider: 3 / 4s / 4a                            | FAIL `INSERT_COLUMN_ARITY_MISMATCH.TOO_MANY_DATA_COLUMNS` / `EXTRA_STRUCT_FIELDS` / `EXTRA_STRUCT_FIELDS` | FAIL `_LEGACY_ERROR_TEMP_DELTA_0007` (all)               |

Two different things happen in this table, and they should not be confused:

- **Spark absorbs the batch.** For Parquet, this happens only for a top-level
  missing column through `INSERT INTO … BY NAME`, which fills it with null. It
  never happens for a nested field.
- **The writer pads the batch to the table's schema first.** `unionByName` with
  `allowMissingColumns=true` does this down into `array<struct>` elements, so
  every narrower batch then fits.

Padding can only fill fields the table already has. It does nothing for a
wider batch.

## Conclusions

The following are measured:

1. **Narrower batches.**
    - A Parquet catalog table rejects them on every Spark write route except
      `INSERT INTO … BY NAME`, and that route covers top-level columns only.
    - They are absorbed only when the writer pads them to the table's schema
      first, for example with `unionByName(…, allowMissingColumns = true)`
      against the table's own empty frame. This works at every nesting level
      measured.
    - Narrower files written straight into the location read back correctly,
      with the missing fields null.
2. **Wider batches.**
    - A Parquet catalog table rejects them on every write route, with or
      without `mergeSchema`.
    - A new top-level column can be added first with `ALTER TABLE ADD COLUMNS`,
      after which the append succeeds.
    - Every public DDL route for a nested field is refused.
    - Wider files written straight into the location are read without their
      new fields, and no refresh exposes them.
    - The only measured way to expose a nested widening is to drop and
      re-create an external table over the location, with merging enabled
      through the table option or the session setting. That route also
      re-orders columns by the file listing. Without merging enabled, it hides
      data.
3. **Delta catalog tables.** A `saveAsTable` append with `mergeSchema=true`
   widens the table at the top level, inside a struct and inside an
   `array<struct>`, including for a mixed batch that also lacks a field. A
   Parquet catalog table refuses mixed batches too. Narrower batches append at every level, with or without the
   option. Without the option, any wider batch is refused before anything is
   written. `writeTo(t).option("mergeSchema","true").append()` and the
   path-based write behave the same way.
4. **Field order.** Reordered fields are placed by name in both formats, at
   every level measured.
5. **No silent loss on write.** No failed append wrote anything, and no
   successful append lost or misplaced a row or a value.

The following is inference, not measured here:

- A Parquet catalog sink can support narrower batches by padding each batch to
  the table's schema before `saveAsTable`.
- It cannot support wider nested batches without rewriting the table. Reading
  the old table, merging, and overwriting would be such a rewrite, but it was
  not measured, and an overwrite of a table that is also being read is a known
  Spark restriction.
- The other way is the external-table re-creation above, with the caveats
  recorded there.
- Delta has no such limitation, provided `CatalogSink` passes
  `mergeSchema=true` on append. This held for every shape measured: narrower,
  wider, mixed and reordered, one level at a time. A batch that differs at
  several levels at once was measured only through `wide-all` and
  `narrow-all`, and only on Delta tables altered first (case 7). Type changes
  were not measured.

## Reproduction

`scripts/catalog-append/CatalogAppendProbe.java` is a single-file Java program.
`scripts/catalog-append/run.sh` resolves the `library-api` third-party
classpath offline from `~/.m2`, then runs the program. Run it from the
repository root, once per catalog:

```bash
specs/001-parquet-on-fhir/evidence/scripts/catalog-append/run.sh /tmp/catalog-append in-memory
specs/001-parquet-on-fhir/evidence/scripts/catalog-append/run.sh /tmp/catalog-append-hive hive
```

The transcripts are `catalog-append-schema-in-memory.txt` and
`catalog-append-schema-hive.txt`. Scratch paths are replaced with
`<scratch>/run-N`, Delta table IDs with `<uuid>` and object hashes with
`<hash>`, and Hive metastore warnings are removed.
