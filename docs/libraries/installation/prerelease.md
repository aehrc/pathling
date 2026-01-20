# Pre-release versions

If you are helping to test a pre-release version of the Pathling library, you can use the following instructions to set up your environment.

You need to configure in the snapshots repository so that the PathlingContext can find the pre-release version of the library.

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
        "au.csiro.pathling:library-runtime:[some pre-release version]-SNAPSHOT"
    )
    .config(
        "spark.jars.repositories",
        "https://central.sonatype.com/repository/maven-snapshots",
    )
    .getOrCreate()
)

pc = PathlingContext.create(spark)
```

```
library(sparklyr)
library(pathling)

sc <- spark_connect(
        master = "local",
        packages = "au.csiro.pathling:library-runtime:[some pre-release version]-SNAPSHOT",
        repositories = "https://central.sonatype.com/repository/maven-snapshots",
        version = "3.5.6"
)

pc <- pathling_connect(sc)
```

```
import au.csiro.pathling.library.PathlingContext
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder
        .config("spark.jars.packages", "au.csiro.pathling:library-runtime:[some pre-release version]-SNAPSHOT")
        .config("spark.jars.repositories", "https://central.sonatype.com/repository/maven-snapshots")
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
                        "au.csiro.pathling:library-runtime:[some pre-release version]-SNAPSHOT")
                .config("spark.jars.repositories",
                        "https://central.sonatype.com/repository/maven-snapshots")
                .getOrCreate();
        PathlingContext pc = PathlingContext.create(spark);
    }
}
```
