---
sidebar_position: 4
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

The session that you provide must have the Pathling library on the classpath,
and it must also have `spark.executor.userClassPathFirst` set to `true`. Here is
an example of how to configure a custom session:

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";
import { JavaInstallation, PythonInstallation, ScalaInstallation }
from "../../../src/components/installation";

<!--suppress CheckEmptyScriptTag -->
<Tabs>
<TabItem value="python" label="Python">

```python
from pyspark.sql import SparkSession
from pathling import PathlingContext, find_jar
    
spark = (
    SparkSession.builder
    .config("spark.jars", find_jar())
    .config("spark.executor.userClassPathFirst", "true")
    .getOrCreate()
)

pc = PathlingContext.create(spark)
```

</TabItem>
<TabItem value="scala" label="Scala">

<ScalaInstallation/>

```scala
import au.csiro.pathling.library.PathlingContext

val spark = SparkSession.builder
    .config("spark.executor.userClassPathFirst", "true")
    .getOrCreate()

val pc = PathlingContext.create(spark)
```

</TabItem>
<TabItem value="java" label="Java">

<JavaInstallation/>

```java
import au.csiro.pathling.library.PathlingContext;
import org.apache.spark.sql.SparkSession;

class MyApp {
    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
            .config("spark.executor.userClassPathFirst", "true")
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
spark.jars.packages au.csiro.pathling:library-api:[some version]
```

See the [Configuration](https://spark.apache.org/docs/latest/configuration.html)
page of the Spark documentation for more information about `spark.jars.packages`
and other related configuration options.

To create a Pathling notebook Docker image, your `Dockerfile` might look like
this:

```dockerfile
FROM jupyter/all-spark-notebook

USER root
RUN echo "spark.executor.userClassPathFirst true" >> /usr/local/spark/conf/spark-defaults.conf
RUN echo "spark.jars.packages au.csiro.pathling:library-api:[some version]" >> /usr/local/spark/conf/spark-defaults.conf

USER ${NB_UID}

RUN pip install --quiet --no-cache-dir pathling && \
    fix-permissions "${CONDA_DIR}" && \
    fix-permissions "/home/${NB_USER}"
```

