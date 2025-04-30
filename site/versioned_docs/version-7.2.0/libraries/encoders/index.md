---
sidebar_position: 2
description: The Pathling library can be used to transform FHIR Bundles or NDJSON into Spark data sets.
---

# FHIR encoders

The Pathling library can be used to transform [FHIR](https://hl7.org/fhir)
Bundles or NDJSON into Spark
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
