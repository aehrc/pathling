# Quickstart: defined schemas

Each scenario below is an end-to-end check. Task T056 runs all of them.

## 1. Daily NDJSON append to a Delta lake (US1, US4)

```python
from pathling import read_schemas, write_schemas

schemas = pc.read.delta("/lake").schemas()
write_schemas(schemas, "lake-schemas.json")

schemas = read_schemas("lake-schemas.json")
day = pc.read.ndjson("/drops/2026-11-25", schemas=schemas)
day.write.delta("/lake", save_mode="merge")

# The test helper of lib/python/tests/schema_equality.py (spec SC-002).
assert same_schema_set(pc.read.delta("/lake").schemas(), schemas)
```

**Expected**: the lake's schema set is unchanged after the append, whatever the day's data
carries. The lake was written under the same annotation configuration as this session (SC-002).

## 2. Kafka bundles to Delta and Parquet through a Pathling sink (US2)

```python
schema = read_schemas("lake-schemas.json")["Patient"]
stream = spark.readStream.format("kafka")...load().selectExpr("CAST(value AS STRING) AS value")
patients = pc.encode_bundle(stream, "Patient", schema=schema)

patients.writeStream.foreachBatch(
    lambda df, _: pc.read.datasets({"Patient": df}).write.delta("/lake", save_mode="merge")
).option("checkpointLocation", "/chk/patient").start()

# Later: Pathling reads the table.
pc.read.delta("/lake").read("Patient").count()
```

The same with `write.parquet("/lake-parquet", save_mode="append")` in the batch function is read
by `pc.read.parquet`. Each micro-batch goes through the sink, so the sink refuses a legacy-layout
table or a wrong timestamp setting as it does for a batch write.

**Expected**: both locations are read back with every streamed resource.

## 3. Streaming without a schema is refused (US2)

```python
pc.encode(stream, "Patient")
```

**Expected**: an error telling the user to supply a schema or to encode each batch within
`foreachBatch`.

## 4. Schema round trip through every source (US4)

For each source (ndjson, bundles, parquet, delta, tables, bulk, datasets):

1. Get `schemas()`.
2. Write the set to a schema set file with `write_schemas` and read it back with `read_schemas`.
3. Pass the set to every source.

**Expected**: every source accepts the set, and the output's schema set is the same as it by
`same_schema_set` (SC-002).
