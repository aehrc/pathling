---
sidebar_position: 1
sidebar_label: Encoders
---

# Encoders

Pathling provides a set of libraries that can be used to transform data between
FHIR ([JSON](https://www.hl7.org/fhir/R4/json.html)
or [XML](https://www.hl7.org/fhir/R4/xml.html)) and Apache Spark data sets. The
encoders can be used from Python, Scala and Java.

We also have upcoming support for R, subscribe to
[this issue](https://github.com/aehrc/pathling/issues/193) for updates.

Once your data is encoded as a Spark data set, it can be queried using SQL, or
transformed using the full library of functions that Spark provides. It can also
be written to [Parquet](https://parquet.apache.org/) and other formats that are
compatible with a wide range of tools. See
the [Spark documentation](https://spark.apache.org/docs/latest/) for more
details.

:::info
We also have upcoming support for R, subscribe to
[this issue](https://github.com/aehrc/pathling/issues/193) for updates.
:::

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";
import {
JavaInstallation,
PythonInstallation,
ScalaInstallation
} from "../../src/components/installation";

## Reading in NDJSON

[NDJSON](http://ndjson.org) is a format commonly used for bulk FHIR data, and
consists of files (one per resource type) that contains one JSON resource per
line.

<Tabs>
<TabItem value="python" label="Python">

<PythonInstallation/>

```python
from pyspark.sql import SparkSession
from pathling import PathlingContext
from pathling.etc import find_jar

spark = SparkSession.builder
# Tell Spark where to find the bundled Pathling JAR.
.config('spark.jars', find_jar())
.getOrCreate()

# Read each line from the NDJSON into a row within a Spark data set.
ndjson_dir = '/some/path/ndjson/'
json_resources = spark.read.text(ndjson_dir)

# Convert the data set of strings into a structured FHIR data set.
pc = PathlingContext.create(spark)
patients = pc.encode(json_resources, 'Patient')

# Do some stuff.
patients.select('id', 'gender', 'birthDate').show()
```

</TabItem>
<TabItem value="scala" label="Scala">

<ScalaInstallation/>

```scala
import org.apache.spark.sql.SparkSession
import au.csiro.pathling.api.PathlingContext

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

<JavaInstallation/>

```java
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import au.csiro.pathling.api.PathlingContext;

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

<PythonInstallation/>

```python
from pyspark.sql import SparkSession
from pathling import PathlingContext
from pathling.etc import find_jar

spark = SparkSession.builder
# Tell Spark where to find the bundled Pathling JAR.
.config('spark.jars', find_jar())
.getOrCreate()

# Read each Bundle into a row within a Spark data set.
bundles_dir = '/some/path/bundles/'
bundles = spark.read.text(bundles_dir)

# Convert the data set of strings into a structured FHIR data set.
pc = PathlingContext.create(spark)
patients = pc.encodeBundle(bundles, 'Patient')

# JSON is the default format, XML Bundles can be encoded using input type.
# patients = pc.encodeBundle(bundles, 'Patient', inputType=MimeType.FHIR_XML)

# Do some stuff.
patients.select('id', 'gender', 'birthDate').show()
```

</TabItem>
<TabItem value="scala" label="Scala">

<ScalaInstallation/>

```scala
import org.apache.spark.sql.SparkSession
import au.csiro.pathling.api.PathlingContext

val spark = SparkSession.builder.getOrCreate()

// Read each line from the NDJSON into a row within a Spark data set.
val bundlesDir = "/some/path/bundles/"
val bundles = spark.read.text(bundlesDir)

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

<JavaInstallation/>

```java
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.Dataset;
import au.csiro.pathling.api.PathlingContext;

class MyApp {

    public static void main(String args[]) {
        SparkSession spark = SparkSession.builder().getOrCreate();

        // Read each line from the NDJSON into a row within a Spark data set.
        String bundlesDir = "/some/path/bundles/";
        Dataset<Row> bundles = spark.read().text(bundlesDir);

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

## Installation in Databricks

To make the Pathling encoders available within notebooks, navigate to the
"Compute" section and click on the cluster. Click on the "Libraries" tab, and
click "Install new".

Install both the `python` PyPI package, and the `au.csiro.pathling:encoders`
Maven package. Once the cluster is restarted, the libraries should be available
for import and use within all notebooks.

See the Databricks documentation on
[Libraries](https://docs.databricks.com/libraries/index.html) for more
information.

## Spark cluster configuration

If you are running your own Spark cluster, or using a Docker image (such as
[jupyter/all-spark-notebook](https://hub.docker.com/r/jupyter/all-spark-notebook))
,
you will need to configure Pathling as a Spark package.

You can do this by adding the following to your `spark-defaults.conf` file:

```
spark.jars.packages au.csiro.pathling:encoders:[some version]
```

See the [Configuration](https://spark.apache.org/docs/latest/configuration.html)
page of the Spark documentation for more information about `spark.jars.packages`
and other related configuration options.

To create a Pathling notebook Docker image, your `Dockerfile` might look like
this:

```dockerfile
FROM jupyter/all-spark-notebook

USER root
RUN echo "spark.jars.packages au.csiro.pathling:encoders:[some version]" >> /usr/local/spark/conf/spark-defaults.conf

USER ${NB_UID}

RUN pip install --quiet --no-cache-dir pathling && \
    fix-permissions "${CONDA_DIR}" && \
    fix-permissions "/home/${NB_USER}"
```
