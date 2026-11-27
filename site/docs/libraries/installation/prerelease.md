---
sidebar_position: 5
description: Instructions for using a pre-release version of the Pathling library.
---

# Pre-release versions

If you are helping to test a pre-release version of the Pathling library, you
can use the following instructions to set up your environment.

You need to configure in the snapshots repository so that the PathlingContext
can find the pre-release version of the library.

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";

<!--suppress CheckEmptyScriptTag -->
<Tabs>
<TabItem value="python" label="Python">

```python
from pathling import PathlingContext

pc = PathlingContext.create(
    spark_conf={
        "spark.jars.packages": "au.csiro.pathling:library-runtime:[some pre-release version]-SNAPSHOT",
        "spark.jars.repositories": "https://central.sonatype.com/repository/maven-snapshots",
    }
)
```

</TabItem>
<TabItem value="r" label="R">

```r
library(sparklyr)
library(pathling)

sc <- spark_connect(
        master = "local",
        version = pathling_spark_info()$spark_version,
        packages = "au.csiro.pathling:library-runtime:[some pre-release version]-SNAPSHOT",
        config = list(
                "sparklyr.shell.repositories" = "https://central.sonatype.com/repository/maven-snapshots",
                "sparklyr.shell.conf" = c(
                        "spark.sql.parquet.outputTimestampType=TIMESTAMP_MICROS",
                        "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
                        "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
                )
        )
)

pc <- pathling_connect(sc)
```

</TabItem>
<TabItem value="scala" label="Scala">

```scala
import au.csiro.pathling.library.PathlingContext
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder
        .config("spark.jars.packages", "au.csiro.pathling:library-runtime:[some pre-release version]-SNAPSHOT")
        .config("spark.jars.repositories", "https://central.sonatype.com/repository/maven-snapshots")
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
                        "au.csiro.pathling:library-runtime:[some pre-release version]-SNAPSHOT")
                .config("spark.jars.repositories",
                        "https://central.sonatype.com/repository/maven-snapshots")
                .getOrCreate();
        PathlingContext pc = PathlingContext.create(spark);
    }
}
```

</TabItem>
</Tabs>
