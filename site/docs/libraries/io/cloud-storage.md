---
sidebar_position: 2
description: Configure the Pathling library to read and write FHIR data in Amazon S3, Azure Blob Storage, Azure Data Lake Storage and Google Cloud Storage.
---

# Cloud storage

Every path accepted by the Pathling read and write functions is resolved by
Spark through the [Hadoop FileSystem](https://hadoop.apache.org/docs/r3.4.1/hadoop-project-dist/hadoop-common/filesystem/index.html)
API. This means that a cloud storage location can be used anywhere that a local
path can, provided that the Spark session has been configured with:

1. A **connector** for the storage service on its classpath, usually added with
   the `spark.jars.packages` configuration property.
2. **Credentials** and any other connector settings, passed as Spark
   configuration properties with the `spark.hadoop.` prefix. For example, the
   Hadoop property `fs.s3a.access.key` is set as `spark.hadoop.fs.s3a.access.key`.

The configured session is then passed to `PathlingContext.create`, as described
in [Spark configuration](../installation/spark#session-configuration). The
connector version must match the Hadoop version bundled with Spark. Spark 4.0.x
bundles Hadoop 3.4.1, so the examples on this page use the `3.4.1` connectors.

import Tabs from "@theme/Tabs";
import TabItem from "@theme/TabItem";

## Configuring the session

The following example configures a session that can read from and write to
Amazon S3, and then reads NDJSON from a bucket and writes Parquet back to it.
The same pattern applies to the other services - only the package coordinates
and the `spark.hadoop.` properties change.

<!--suppress CheckEmptyScriptTag -->
<Tabs>
<TabItem value="python" label="Python">

```python
from pathling import PathlingContext
from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.config(
        "spark.jars.packages",
        "au.csiro.pathling:library-runtime:9.9.0,"
        + "io.delta:delta-spark_2.13:4.0.0,"
        + "org.apache.hadoop:hadoop-aws:3.4.1",
    )
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config(
        "spark.sql.catalog.spark_catalog",
        "org.apache.spark.sql.delta.catalog.DeltaCatalog",
    )
    .config("spark.hadoop.fs.s3a.access.key", "AKIA...")
    .config("spark.hadoop.fs.s3a.secret.key", "...")
    .getOrCreate()
)

pc = PathlingContext.create(spark)

data = pc.read.ndjson("s3a://my-bucket/fhir/ndjson")
data.write.parquet("s3a://my-bucket/fhir/parquet")
```

</TabItem>
<TabItem value="r" label="R">

```r
library(sparklyr)
library(pathling)

sc <- spark_connect(master = "local",
                    packages = c(paste0("au.csiro.pathling:library-runtime:", pathling_version()),
                                 "io.delta:delta-spark_2.13:4.0.0",
                                 "org.apache.hadoop:hadoop-aws:3.4.1"),
                    config = list("sparklyr.shell.conf" = c(
                            "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
                            "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog",
                            "spark.hadoop.fs.s3a.access.key=AKIA...",
                            "spark.hadoop.fs.s3a.secret.key=..."
                    )), version = "4.0.2")

pc <- pathling_connect(sc)

data <- pc %>% pathling_read_ndjson("s3a://my-bucket/fhir/ndjson")
data %>% ds_write_parquet("s3a://my-bucket/fhir/parquet")
```

</TabItem>
<TabItem value="scala" label="Scala">

```scala
import au.csiro.pathling.library.PathlingContext
import org.apache.spark.sql.SparkSession

val spark = SparkSession.builder
        .config("spark.jars.packages", "au.csiro.pathling:library-runtime:9.9.0," +
                "io.delta:delta-spark_2.13:4.0.0," +
                "org.apache.hadoop:hadoop-aws:3.4.1")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.hadoop.fs.s3a.access.key", "AKIA...")
        .config("spark.hadoop.fs.s3a.secret.key", "...")
        .getOrCreate()

val pc = PathlingContext.create(spark)

val data = pc.read().ndjson("s3a://my-bucket/fhir/ndjson")
data.write().parquet("s3a://my-bucket/fhir/parquet")
```

</TabItem>
<TabItem value="java" label="Java">

```java
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.source.NdjsonSource;
import org.apache.spark.sql.SparkSession;

class MyApp {

    public static void main(String[] args) {
        SparkSession spark = SparkSession.builder()
                .config("spark.jars.packages",
                        "au.csiro.pathling:library-runtime:9.9.0," +
                                "io.delta:delta-spark_2.13:4.0.0," +
                                "org.apache.hadoop:hadoop-aws:3.4.1")
                .config("spark.sql.extensions",
                        "io.delta.sql.DeltaSparkSessionExtension")
                .config("spark.sql.catalog.spark_catalog",
                        "org.apache.spark.sql.delta.catalog.DeltaCatalog")
                .config("spark.hadoop.fs.s3a.access.key", "AKIA...")
                .config("spark.hadoop.fs.s3a.secret.key", "...")
                .getOrCreate();

        PathlingContext pc = PathlingContext.create(spark);

        NdjsonSource data = pc.read().ndjson("s3a://my-bucket/fhir/ndjson");
        data.write().parquet("s3a://my-bucket/fhir/parquet");
    }
}
```

</TabItem>
</Tabs>

Avoid placing long-lived secrets in source code. Prefer the identity of the
environment that the code runs in (an IAM role, a managed identity or a service
account), or read the secret from an environment variable at the point where the
session is built. Hadoop can also load any `fs.*` property from an encrypted
[credential provider](https://hadoop.apache.org/docs/r3.4.1/hadoop-project-dist/hadoop-common/CredentialProviderAPI.html)
file, referenced with `spark.hadoop.hadoop.security.credential.provider.path`.

The [command line interface](../cli#spark-configuration) accepts the same
properties in its `[spark]` configuration table, and can read secret values
from a file.

## Amazon S3

- URL scheme: `s3a://<bucket>/<path>`
- Package: `org.apache.hadoop:hadoop-aws:3.4.1`
- Reference: [Hadoop-AWS module](https://hadoop.apache.org/docs/r3.4.1/hadoop-aws/tools/hadoop-aws/index.html)

The `hadoop-aws` package brings in the AWS SDK bundle as a transitive
dependency, which is several hundred megabytes and is downloaded the first time
the session starts.

If no credentials are configured, the S3A connector searches, in order: the
`fs.s3a.access.key` and `fs.s3a.secret.key` properties, the `AWS_ACCESS_KEY_ID`
and `AWS_SECRET_ACCESS_KEY` environment variables, and finally the IAM role
attached to the EC2 instance, ECS task or EKS pod that the code is running on.
On AWS infrastructure with an attached role, no credential configuration is
required at all.

```properties
# Static credentials. Omit these when relying on environment variables or an
# IAM role.
spark.hadoop.fs.s3a.access.key=AKIA...
spark.hadoop.fs.s3a.secret.key=...

# Temporary credentials issued by STS also need a session token and the
# temporary credentials provider.
spark.hadoop.fs.s3a.session.token=...
spark.hadoop.fs.s3a.aws.credentials.provider=org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider

# Optional. Set when the bucket region cannot be determined automatically.
spark.hadoop.fs.s3a.endpoint.region=ap-southeast-2
```

Settings can also be scoped to a single bucket by inserting `bucket.<name>`
after `fs.s3a.`, for example `spark.hadoop.fs.s3a.bucket.my-bucket.access.key`.
This allows one session to use different credentials for different buckets.
See [Per-bucket configuration](https://hadoop.apache.org/docs/r3.4.1/hadoop-aws/tools/hadoop-aws/index.html#Configuring_different_S3_buckets_with_Per-Bucket_Configuration)
for details.

## Azure Blob Storage and Azure Data Lake Storage

- URL scheme: `abfss://<container>@<account>.dfs.core.windows.net/<path>`
- Package: `org.apache.hadoop:hadoop-azure:3.4.1`
- Reference: [Hadoop Azure support: ABFS](https://hadoop.apache.org/docs/r3.4.1/hadoop-azure/abfs.html)

Azure Blob Storage and Azure Data Lake Storage Gen2 are the same service: Gen2
is a storage account with the hierarchical namespace feature enabled. Both are
accessed through the ABFS connector in the `hadoop-azure` package, using the
`abfss` scheme. The hierarchical namespace gives fast, atomic directory rename
and delete, which matters for the way that Spark commits output, so enable it on
accounts that Pathling will write to.

The `wasbs` scheme, provided by the legacy WASB connector in the same package,
also works for reading data from accounts without a hierarchical namespace, but
Hadoop deprecates it in favour of ABFS. The `adl` scheme and the
`hadoop-azure-datalake` package are for Azure Data Lake Storage Gen1, which
[Microsoft retired](https://learn.microsoft.com/en-us/previous-versions/azure/data-lake-store/data-lake-store-overview)
in February 2024, and are not covered here.

The authentication mechanism is selected with `fs.azure.account.auth.type`. The
simplest is the storage account access key:

```properties
spark.hadoop.fs.azure.account.auth.type.<account>.dfs.core.windows.net=SharedKey
spark.hadoop.fs.azure.account.key.<account>.dfs.core.windows.net=...
```

A service principal is authenticated with OAuth 2.0 client credentials. The
principal needs a data role on the container, such as Storage Blob Data
Contributor.

```properties
spark.hadoop.fs.azure.account.auth.type.<account>.dfs.core.windows.net=OAuth
spark.hadoop.fs.azure.account.oauth.provider.type.<account>.dfs.core.windows.net=org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider
spark.hadoop.fs.azure.account.oauth2.client.endpoint.<account>.dfs.core.windows.net=https://login.microsoftonline.com/<tenant-id>/oauth2/token
spark.hadoop.fs.azure.account.oauth2.client.id.<account>.dfs.core.windows.net=<client-id>
spark.hadoop.fs.azure.account.oauth2.client.secret.<account>.dfs.core.windows.net=<client-secret>
```

Code running on an Azure VM, in Azure Kubernetes Service or in another Azure
service with a managed identity can authenticate without any secret:

```properties
spark.hadoop.fs.azure.account.auth.type.<account>.dfs.core.windows.net=OAuth
spark.hadoop.fs.azure.account.oauth.provider.type.<account>.dfs.core.windows.net=org.apache.hadoop.fs.azurebfs.oauth2.MsiTokenProvider
```

Each property is shown with an account suffix, which scopes it to one storage
account. Drop the suffix (for example `fs.azure.account.auth.type`) to apply a
setting to every account that the session accesses. See
[Authentication](https://hadoop.apache.org/docs/r3.4.1/hadoop-azure/abfs.html#Authentication)
in the Hadoop documentation for the remaining mechanisms, including SAS tokens
and refresh tokens.

## Google Cloud Storage

- URL scheme: `gs://<bucket>/<path>`
- Connector: [Cloud Storage connector](https://github.com/GoogleCloudDataproc/hadoop-connectors/tree/master/gcs)
- Reference: [Connector configuration](https://github.com/GoogleCloudDataproc/hadoop-connectors/blob/master/gcs/CONFIGURATION.md)

Google Cloud Storage is not covered by a connector in the Hadoop distribution.
Google maintains its own Hadoop connector, which uses the Cloud Storage API
directly and is the recommended way to access it. The connector is published
with a `shaded` classifier that bundles its dependencies, and Google recommends
the shaded jar to avoid Guava conflicts with Spark. `spark.jars.packages` cannot
select a classifier, so add the jar with `spark.jars` instead:

```properties
spark.jars=https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/4.0.4/gcs-connector-4.0.4-shaded.jar
spark.hadoop.fs.gs.impl=com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem
```

The default authentication type is `COMPUTE_ENGINE`, which uses the service
account attached to the Compute Engine VM, GKE node or Dataproc cluster that the
code is running on. Outside Google Cloud, authenticate with a service account
key file, which must be present at the same path on every node:

```properties
spark.hadoop.fs.gs.auth.type=SERVICE_ACCOUNT_JSON_KEYFILE
spark.hadoop.fs.gs.auth.service.account.json.keyfile=/path/to/keyfile.json
```

Setting `fs.gs.auth.type` to `APPLICATION_DEFAULT` uses
[Application Default Credentials](https://cloud.google.com/docs/authentication/application-default-credentials),
which picks up `gcloud auth application-default login` on a workstation.

### Access through the S3A connector

Cloud Storage also exposes an
[S3-compatible XML API](https://cloud.google.com/storage/docs/interoperability),
so it can be reached with the same `hadoop-aws` package used for Amazon S3 and a
pair of [HMAC keys](https://cloud.google.com/storage/docs/authentication/hmackeys).
Hadoop documents this as a fallback for environments that cannot use the Cloud
Storage connector: several S3A features must be disabled, which makes rename and
delete slower, and it is not a commonly tested configuration.

```properties
spark.hadoop.fs.s3a.bucket.<bucket>.access.key=GOOG1E...
spark.hadoop.fs.s3a.bucket.<bucket>.secret.key=...
spark.hadoop.fs.s3a.bucket.<bucket>.endpoint=https://storage.googleapis.com
spark.hadoop.fs.s3a.bucket.<bucket>.endpoint.region=dummy
spark.hadoop.fs.s3a.bucket.<bucket>.path.style.access=true
spark.hadoop.fs.s3a.bucket.<bucket>.bucket.probe=0
spark.hadoop.fs.s3a.bucket.<bucket>.list.version=1
spark.hadoop.fs.s3a.bucket.<bucket>.multiobjectdelete.enable=false
```

The bucket is then addressed as `s3a://<bucket>/<path>`. These settings are
taken from the "Connecting to Google Cloud Storage through the S3A connector"
section of
[Working with third-party S3 stores](https://hadoop.apache.org/docs/r3.4.1/hadoop-aws/tools/hadoop-aws/third_party_stores.html).
