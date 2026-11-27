# The Java streaming example of the documentation runs against a file-based stream

Measured on 2026-11-27 with Spark 4.1.0, the version the build uses, against `library-api` as of
the commit that adds this record, using [streaming-example-probe.java](streaming-example-probe.java).
It was run for T052 and SC-008.

The probe runs the Java example of `site/docs/libraries/examples/streaming.md`, under
"Encoding a stream", with its paths in a temporary directory. It first saves a schema set file
from a table of one sample Patient, which has `gender`, `birthDate` and `name.family`, standing in
for the file the example reads. The stream is a `readStream().text` file source over three NDJSON
files of one Patient each. Two changes to the example make the run finish and show more than one
micro-batch: the source takes one file per micro-batch (`maxFilesPerTrigger`), and the query runs
with an available-now trigger and is awaited. The first streamed Patient carries `active` and
`name.given`, which the schema lacks.

**Result: the example runs. The encoded dataset is a stream, the query ran three micro-batches
(the last is batch 2), each written through a Pathling sink within `foreachBatch`, merged into
Delta and appended to Parquet. Both tables read back through Pathling with the given schema and the
same three resources. `active` and `name.given` are dropped silently, as the documentation says.**

```
Streaming: true
Last micro-batch: 2
Delta schema is the schema: true
Parquet schema is the schema: true
Delta rows:
{"resourceType":"Patient","id":"p1","name":[{"family":"Jones"}],"gender":"male"}
{"resourceType":"Patient","id":"p2","birthDate":"1980-02-03","_birthDate":{"__start":"1980-02-03T00:00:00.000Z","__end":"1980-02-03T23:59:59.999Z"}}
{"resourceType":"Patient","id":"p3","gender":"other"}
Parquet rows:
(the same three rows)
```

The Python example is the same program in the Python binding, which routes to the same Java
methods. Its encode of a stream with a schema, and the refusal without one, are tested in
`lib/python/tests/test_encoders.py`, and the write through a Pathling sink within `foreachBatch` in
`StreamingEncodeTest.streamWrittenThroughPathlingSinkIsReadBack`.

## Reproduction

The probe sits in the package `au.csiro.pathling.library` and runs against the `library-api` module
of the checkout, not `io`, so [run-probe.sh](run-probe.sh) does not run it. From the root of the
checkout, after `scripts/mvn-isolated.sh install -pl library-api -am -DskipTests` (use `mvn` in a
main checkout):

```bash
work=$(mktemp -d)
scripts/mvn-isolated.sh -q dependency:build-classpath -pl library-api -Dmdep.includeScope=test \
  -Dmdep.outputFile="$work/cp.txt"
classpath="library-api/target/classes:$(cat "$work/cp.txt")"
echo '<configuration><root level="OFF"/></configuration>' > "$work/logback.xml"
javac -nowarn -d "$work" -cp "$classpath" \
  specs/2881-defined-schemas/evidence/streaming-example-probe.java
java -Xmx4g -Dlogback.configurationFile="$work/logback.xml" -Duser.timezone=UTC \
  --add-exports=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.net=ALL-UNNAMED \
  --add-opens=java.base/sun.util.calendar=ALL-UNNAMED --add-opens=java.base/java.nio=ALL-UNNAMED \
  --add-opens=java.base/sun.nio.ch=ALL-UNNAMED --add-opens=java.base/java.lang=ALL-UNNAMED \
  --add-opens=java.base/java.util=ALL-UNNAMED --add-opens=java.base/java.lang.invoke=ALL-UNNAMED \
  -cp "$work:$classpath" au.csiro.pathling.library.StreamingExampleProbe 2> "$work/stderr.txt"
```
