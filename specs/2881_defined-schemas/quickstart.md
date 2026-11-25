# Quickstart: defined schemas

Each scenario below is an end-to-end check. Task T057 runs all of them.

## 1. Daily NDJSON append to a Delta lake (US1, US4)

```python
import json

schemas = pc.read.delta("/lake").schemas()
json.dump(schemas, open("lake-schemas.json", "w"))

schemas = json.load(open("lake-schemas.json"))
day = pc.read.ndjson("/drops/2026-11-25", schemas=schemas)
day.write.delta("/lake", save_mode="merge")

assert pc.read.delta("/lake").schemas() == schemas
```

**Expected**: the lake's schema set is unchanged after the append, whatever the day's data
carries.

## 2. Kafka bundles to Delta and Parquet with Spark's streaming writer (US2, US3)

```python
schema = json.load(open("lake-schemas.json"))["Patient"]
stream = spark.readStream.format("kafka")...load().selectExpr("CAST(value AS STRING) AS value")
patients = pc.encode_bundle(stream, "Patient", schema=schema)

paths = pc.check_destination("delta", "/lake", {"Patient": patients})
patients.writeStream.format("delta") \
    .option("checkpointLocation", "/chk/patient") \
    .start(paths["Patient"])

# Later: Pathling reads the table.
pc.read.delta("/lake").read("Patient").count()
```

The same with `"parquet"` and `writeStream.format("parquet")` is read by `pc.read.parquet`.

**Expected**: both locations are read back with every streamed resource.

## 3. Streaming without a schema is refused (US2)

```python
pc.encode(stream, "Patient")
```

**Expected**: an error telling the user to supply a schema.

## 4. Destination check refuses a legacy table (US3)

```python
pc.check_destination("delta", "/legacy-lake", {"Patient": patients})
```

**Expected**: the same legacy-layout refusal as `write.delta`.

## 5. Schema round trip through every source (US4)

For each source (ndjson, bundles, parquet, delta, tables, bulk, datasets):

1. Get `schemas()`.
2. Dump the set to JSON and load it back.
3. Pass the set to every source.

**Expected**: every source accepts the set, and the output's base schema equals it.
