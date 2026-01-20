# Databricks installation

To install the Pathling library on a [Databricks](https://www.databricks.com/) cluster, navigate to the "Compute" section and click on the cluster. Click on the "Libraries" tab, and click "Install new".

Pathling has been tested on [Databricks Runtime 17.3 LTS](https://docs.databricks.com/aws/en/release-notes/runtime/17.3lts).

Install the core Pathling functionality by selecting "Maven" as the library source and installing the [library runtime Maven package](https://central.sonatype.com/artifact/au.csiro.pathling/library-runtime).

You can the optionally install the [PyPI package](https://pypi.org/project/pathling/) for Python support, and/or the [R package](https://cran.r-project.org/package=pathling).

Once the cluster is restarted, Pathling should be available for use within notebooks.

See the Databricks documentation on [Libraries](https://docs.databricks.com/libraries/index.html) for more information.

## Environment variables[​](#environment-variables "Direct link to Environment variables")

By default, Databricks uses Java 8 within its clusters, while Pathling requires Java 21. To enable Java 21 support within your cluster, navigate to **Advanced Options > Spark > Environment Variables** and add the following:

```
JNAME=zulu21-ca-amd64
```
