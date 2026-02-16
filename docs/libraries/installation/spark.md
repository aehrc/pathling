# Spark configuration

## Supported versions[​](#supported-versions "Direct link to Supported versions")

Pathling is built and tested against **Apache Spark 4.0.x**. The Python library requires PySpark 4.0.x, and the R library requires sparklyr with Spark 4.0.x.

## Session configuration[​](#session-configuration "Direct link to Session configuration")

When you create a `PathlingContext` within your Spark application, it will detect the presence of an existing `SparkSession` and use it. If there is no existing session, it will create one for you with some sensible default configuration. You can override this default configuration by passing a `SparkSession` object to the `PathlingContext` constructor.

This can be useful if you want to set other Spark configuration, for example to increase the available memory.

The session that you provide must have the Pathling library API on the classpath. You can also optionally enable [Delta Lake](https://delta.io/) support. Here is an example of how to programmatically configure a session that has Delta enabled:

<!-- -->

* Python
* R
* Scala
* Java

```
from pathling import PathlingContext
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.config(
        "spark.jars.packages",
        "au.csiro.pathling:library-runtime:9.3.1," +
        "io.delta:delta-spark_2.13:4.0.0"
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

```
library(sparklyr)
library(pathling)

sc <- spark_connect(master = "local",
                    packages = c(paste0("au.csiro.pathling:library-runtime:", pathling_version()),
                                 "io.delta:delta-spark_2.13:4.0.0"),
                    config = list("sparklyr.shell.conf" = c(
                            "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
                            "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
                    )), version = "4.0.2")

pc <- pathling_connect(sc)
```

```
import au.csiro.pathling.library.PathlingContext

val spark = SparkSession.builder
        .config("spark.jars.packages", "au.csiro.pathling:library-runtime:9.3.1," +
                "io.delta:delta-spark_2.13:4.0.0")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .getOrCreate()

val pc = PathlingContext.create(spark)
```

```
import au.csiro.pathling.library.PathlingContext;
import org.apache.spark.sql.SparkSession;

class MyApp {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .config("spark.jars.packages",
                        "au.csiro.pathling:library-runtime:9.3.1," +
                                "io.delta:delta-spark_2.13:4.0.0")
                .config("spark.sql.extensions",
                        "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog",
                        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .getOrCreate();
        PathlingContext pc = PathlingContext.create(spark);
    }
}
```

## Cluster configuration[​](#cluster-configuration "Direct link to Cluster configuration")

If you are running your own Spark cluster, or using a Docker image (such as [jupyter/all-spark-notebook](https://hub.docker.com/r/jupyter/all-spark-notebook)), you will need to configure Pathling as a Spark package.

You can do this by adding the following to your `spark-defaults.conf` file:

```
spark.jars.packages au.csiro.pathling:library-runtime:[some version]
```

See the [Configuration](https://spark.apache.org/docs/latest/configuration.html) page of the Spark documentation for more information about `spark.jars.packages` and other related configuration options.

To create a Pathling notebook Docker image, your `Dockerfile` might look like this:

```
FROM jupyter/all-spark-notebook

USER root
RUN echo "spark.jars.packages au.csiro.pathling:library-runtime:[some version]" >> /usr/local/spark/conf/spark-defaults.conf

USER ${NB_UID}

RUN pip install --quiet --no-cache-dir pathling && \
    fix-permissions "${CONDA_DIR}" && \
    fix-permissions "/home/${NB_USER}"
```
