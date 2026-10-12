---
sidebar_position: 1
description: The Pathling library can be used to transform FHIR Bundles or NDJSON into Spark data sets.
---

# FHIR encoders

The Pathling library also includes a lower-level interface called the encoders.
The encoders can be used to transform [FHIR](https://hl7.org/fhir) Bundles or NDJSON into Spark
data sets. Once your data is encoded, it can be queried using SQL, or
transformed using the full library of functions that Spark provides. It can also
be written to [Parquet](https://parquet.apache.org/) and other formats that are
compatible with a wide range of tools. See
the [Spark documentation](https://spark.apache.org/docs/latest/) for more
details.

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";

## Reading in NDJSON

[NDJSON](https://hl7.org/fhir/R4/nd-json.html) is a format commonly used for
bulk FHIR data, and consists of files (one per resource type) that contains one
JSON resource per line.

<!--suppress CheckEmptyScriptTag -->
<Tabs>
<TabItem value="python" label="Python">

```python
from pathling import PathlingContext

pc = PathlingContext.create()

# Read each line from the NDJSON into a row within a Spark data set.
ndjson_dir = '/some/path/ndjson/'
json_resources = pc.spark.read.text(ndjson_dir)

# Convert the data set of strings into a structured FHIR data set.
patients = pc.encode(json_resources, 'Patient')

# Do some stuff.
patients.select('id', 'gender', 'birthDate').show()
```

</TabItem>
<TabItem value="r" label="R">

```r
library(sparklyr)
library(pathling)

pc <- pathling_connect()

ndjson <- '/some/path/ndjson/Condition.ndjson'
json_resources <- pathling_spark(pc) %>% spark_read_text(ndjson)

pc %>% pathling_encode(json_resources, 'Condition') %>% show()

pc %>% pathling_disconnect()
```

</TabItem>
<TabItem value="scala" label="Scala">

```scala
import au.csiro.pathling.library.PathlingContext

val spark = SparkSession.builder.getOrCreate()

// Read each line from the NDJSON into a row within a Spark data set.
val ndjsonDir = "/some/path/ndjson/"
val jsonResources = spark.read.text(ndjsonDir)

// Convert the data set of strings into a structured FHIR data set.
val pc = PathlingContext.create(spark)
val patients = pc.encode(jsonResources, "Patient")

// Do some stuff.
patients.select("id", "gender", "birthDate").show()
```

</TabItem>
<TabItem value="java" label="Java">

```java
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import au.csiro.pathling.library.PathlingContext;

class MyApp {

    public static void main(String args[]) {
        SparkSession spark = SparkSession.builder().getOrCreate();

        // Read each line from the NDJSON into a row within a Spark data set.
        String ndjsonDir = "/some/path/ndjson/";
        Dataset<Row> jsonResources = spark.read().text(ndjsonDir);

        // Convert the data set of strings into a structured FHIR data set.
        PathlingContext pc = PathlingContext.create(spark);
        Dataset<Row> patients = pc.encode(jsonResources, "Patient");

        // Do some stuff.
        patients.select("id", "gender", "birthDate").show();
    }

}
```

</TabItem>
</Tabs>

## Reading in Bundles

The FHIR [Bundle](https://hl7.org/fhir/R4/bundle.html) resource can contain a
collection of FHIR resources. It is often used to represent a set of related
resources, perhaps generated as part of the same event.

<Tabs>
<TabItem value="python" label="Python">

```python
from pathling import PathlingContext

pc = PathlingContext.create()

# Read each Bundle into a row within a Spark data set.
bundles_dir = '/some/path/bundles/'
bundles = pc.spark.read.text(bundles_dir, wholetext=True)

# Convert the data set of strings into a structured FHIR data set.
patients = pc.encode_bundle(bundles, 'Patient')

# JSON is the default format, XML Bundles can be encoded using input type.
# patients = pc.encodeBundle(bundles, 'Patient', inputType=MimeType.FHIR_XML)

# Do some stuff.
patients.select('id', 'gender', 'birthDate').show()
```

</TabItem>
<TabItem value="r" label="R">

```r
library(sparklyr)
library(pathling)

pc <- pathling_connect()

bundles_dir <- '/some/path/bundles'
json_bundles <- pathling_spark(pc) %>% spark_read_text(bundles_dir, whole = TRUE)

pc %>% pathling_encode_bundle(json_bundles, 'Condition', column = 'contents') %>% show()

pc %>% pathling_disconnect()
```

</TabItem>
<TabItem value="scala" label="Scala">

```scala
import org.apache.spark.sql.SparkSession
import au.csiro.pathling.library.PathlingContext

val spark = SparkSession.builder.getOrCreate()

// Read each line from the NDJSON into a row within a Spark data set.
val bundlesDir = "/some/path/bundles/"
val bundles = spark.read.option("wholetext", value = true).text(bundlesDir)

// Convert the data set of strings into a structured FHIR data set.
val pc = PathlingContext.create(spark)
val patients = pc.encodeBundle(bundles, "Patient")

// JSON is the default format, XML Bundles can be encoded using input type.
// val patients = pc.encodeBundle(bundles, "Patient", FhirMimeTypes.FHIR_XML)

// Do some stuff.
patients.select("id", "gender", "birthDate").show()
```

</TabItem>
<TabItem value="java" label="Java">

```java
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import au.csiro.pathling.library.PathlingContext;

class MyApp {

    public static void main(String args[]) {
        SparkSession spark = SparkSession.builder().getOrCreate();

        // Read each line from the NDJSON into a row within a Spark data set.
        String bundlesDir = "/some/path/bundles/";
        Dataset<Row> bundles = spark.read()
                .option("wholetext", true)
                .text(bundlesDir);

        // Convert the data set of strings into a structured FHIR data set.
        PathlingContext pc = PathlingContext.create(spark);
        Dataset<Row> patients = pc.encodeBundle(bundles, "Patient");

        // JSON is the default format, XML Bundles can be encoded using input
        // type.
        // Dataset<Row> patients = pc.encodeBundle(bundles, "Patient",
        //     FhirMimeTypes.FHIR_XML);

        // Do some stuff.
        patients.select("id", "gender", "birthDate").show();
    }

}
```

</TabItem>
</Tabs>

## Encoding behaviour

Encoding produces resources in the Parquet on FHIR layout described in
[Storage layout](./schema.md). The same rules apply however the resources
arrive: encoding a dataset of strings, reading NDJSON files or bundles, or
reading through any other data source that encodes FHIR JSON or XML.

### Schemas fitted to the data

The schema of an encoded dataset carries only the elements that the encoded
resources populate, so two datasets of the same resource type can have
different columns. See
[The schema is fitted to the data](./schema.md#the-schema-is-fitted-to-the-data).
FHIRPath expressions and SQL on FHIR views work over any such schema, as
described under [FHIRPath](../fhirpath.md#absent-elements).

Because the schema has to be fitted to the data, a streaming dataset cannot be
encoded: `encode` and `encode_bundle` fail with an error when given one. Encode
each micro-batch of the stream instead, as shown in
[Streaming queries](../examples/streaming.md).

### Decimals

A decimal is stored and returned by its numeric value, not in the lexical form
the source used. The stored value is numerically equal to the source, but
trailing zeros are dropped (`1.50` becomes `1.5`), exponent notation is
normalised, and a value with more significant digits than a double-precision
number holds loses the extra digits. When the resources are written back out as FHIR JSON, each decimal
is written as a number in that normalised form. This applies to every way of
encoding, including encoding a dataset of strings.

An encoded decimal column holds text, as the specification requires, so cast it
to a numeric type if you compute with it directly. FHIRPath expressions and SQL
on FHIR views decode it for you.

### Content that is not stored

Some content is dropped rather than stored:

- `contained` resources;
- elements that the FHIR definitions do not describe;
- values whose JSON type contradicts the type that the definitions give the
  element, such as a string where the definitions call for a boolean;
- the `id` and `extension` of primitive elements.

Dropping content is not silent: a warning naming what was dropped is logged by
`au.csiro.pathling.io.ResourceTransformer`. There is no option that makes
encoding fail instead. Input that is not valid JSON at all does fail.

The type of each element is inferred from all the values encoded together, so a
single value that contradicts the definitions affects every value of that
element encoded alongside it, not only the offending one. For example, if one
resource carries `"active": "yes"`, the `active` element is dropped from every
resource in the same encoding operation. For the NDJSON data source, that is
all the files of a resource type.

### Java and Scala: typed datasets

The `encode` and `encodeBundle` methods that returned a dataset typed by a HAPI
resource class have been removed, along with their `Class` argument. Use the
methods that take a resource type name, as in the examples above, which return a
`Dataset<Row>`.
