---
sidebar_position: 4
description: Instructions for configuring Apache Spark to use the Pathling library.
---

# Spark configuration

## Session configuration

When you create a `PathlingContext` within your Spark application, it will
detect the presence of an existing `SparkSession` and use it. If there is no
existing session, it will create one for you with some sensible default
configuration. You can override this default configuration by passing
a `SparkSession` object to the `PathlingContext` constructor.

This can be useful if you want to set other Spark configuration, for example to
increase the available memory.

The session that you provide must have the Pathling library API on the
classpath. You can also optionally enable [Delta Lake](https://delta.io/)
support. Here is an example of how to programmatically configure a session that
has Delta enabled:

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";

<!--suppress CheckEmptyScriptTag -->
<Tabs>
<TabItem value="python" label="Python">

```python
from pathling import PathlingContext
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.config(
            "spark.jars.packages",
            "au.csiro.pathling:library-runtime:7.1.0,"
            "io.delta:delta-spark_2.12:3.1.0,"
    )
    .config(
            "spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension"
    )
    .config(
            "spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    ).getOrCreate()
)

pc = PathlingContext.create(spark)
```

</TabItem>
<TabItem value="r" label="R">

```r
library(sparklyr)
library(pathling)

sc <- spark_connect(master = "local",
                    packages = c(paste("au.csiro.pathling:library-runtime:", pathling_version()), 
                                 "io.delta:delta-spark_2.12:3.1.0"),
                    config = list("sparklyr.shell.conf" = c(
                      "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
                      "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
                    )), version = "3.4.0")

pc <- pathling_connect(sc)
```

</TabItem>
<TabItem value="scala" label="Scala">

```scala
import au.csiro.pathling.library.PathlingContext

val spark = SparkSession.builder
  .config("spark.jars.packages", "au.csiro.pathling:library-runtime:7.1.0," +
      "io.delta:delta-spark_2.12:3.1.0")
  .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
  .config("spark.sql.catalog.spark_catalog",
    "org.apache.spark.sql.delta.catalog.DeltaCatalog")
  .getOrCreate()

val pc = PathlingContext.create(spark)
```

</TabItem>
<TabItem value="java" label="Java">

```java
import au.csiro.pathling.library.PathlingContext;
import org.apache.spark.sql.SparkSession;

class MyApp {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
            .config("spark.jars.packages", 
                    "au.csiro.pathling:library-runtime:7.1.0," +
                    "io.delta:delta-spark_2.12:3.1.0")
            .config("spark.sql.extensions", 
                    "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog",
                    "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .getOrCreate();
        PathlingContext pc = PathlingContext.create(spark);
    }
}
```

</TabItem>
</Tabs>

## Cluster configuration

If you are running your own Spark cluster, or using a Docker image (such
as [jupyter/all-spark-notebook](https://hub.docker.com/r/jupyter/all-spark-notebook)),
you will need to configure Pathling as a Spark package.

You can do this by adding the following to your `spark-defaults.conf` file:

```
spark.jars.packages au.csiro.pathling:library-runtime:[some version]
```

See the [Configuration](https://spark.apache.org/docs/latest/configuration.html)
page of the Spark documentation for more information about `spark.jars.packages`
and other related configuration options.

To create a Pathling notebook Docker image, your `Dockerfile` might look like
this:

```dockerfile
FROM jupyter/all-spark-notebook

USER root
RUN echo "spark.jars.packages au.csiro.pathling:library-runtime:[some version]" >> /usr/local/spark/conf/spark-defaults.conf

USER ${NB_UID}

RUN pip install --quiet --no-cache-dir pathling && \
    fix-permissions "${CONDA_DIR}" && \
    fix-permissions "/home/${NB_USER}"
```

