---
sidebar_position: 3
---

# Databricks installation

Pathling has been tested
on [Databricks Runtime 13.3 LTS](https://docs.databricks.com/en/release-notes/runtime/13.3lts.html).

To install the Pathling library on a [Databricks](https://www.databricks.com/)
cluster, navigate to the "Compute" section and click on the cluster. Click on
the "Libraries" tab, and click "Install new".

Install the core Pathling functionality by selecting "Maven" as the library
source and installing
the [library runtime Maven package](https://central.sonatype.com/artifact/au.csiro.pathling/library-runtime).

You can the optionally install
the [PyPI package](https://pypi.org/project/pathling/) for Python support.

You can also install the R package, but as it has not yet been published to CRAN
you will need the follow the [instructions here](#r-installation).

Once the cluster is restarted, Pathling should be available for use within
notebooks.

See the Databricks documentation
on [Libraries](https://docs.databricks.com/libraries/index.html) for more
information.

## Environment variables

By default, Databricks uses Java 8 within its clusters, while Pathling requires
Java 11. To enable Java 11 support within your cluster, navigate to __Advanced
Options > Spark > Environment Variables__ and add the following:

```bash
JNAME=zulu11-ca-amd64
```

## R installation

In a notebook, use the following code to install the R package and connect to
the Databricks cluster:

```r
# Install the Pathling R API, if not installed.
# Replace [package URL] with the URL of the package source distribution.
if (!nzchar(system.file(package = 'pathling'))) {
    remotes::install_url('[package URL]', upgrade = FALSE)
}

library(pathling)
pc <- pathling_connect(sparklyr::spark_connect(method = "databricks"))

# Code that uses Pathling here.
# ...

pathling_disconnect(pc)
```
