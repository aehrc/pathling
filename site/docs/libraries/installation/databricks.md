---
sidebar_position: 3
---

# Databricks installation

To install the Pathling library on a [Databricks](https://www.databricks.com/)
cluster, navigate to the "Compute" section and click on the cluster. Click on
the "Libraries" tab, and click "Install new".

Install both the `pathling` PyPI package, and
the `au.csiro.pathling:library-api`
Maven package. Once the cluster is restarted, the libraries should be available
for import and use within all notebooks.

Pathling has been tested
on [Databricks Runtime 12.0](https://docs.databricks.com/release-notes/runtime/12.0.html).

## Spark config

Add the following to __Advanced Options > Spark > Spark Config__ - this
prevents problems relating to conflicts between dependencies within Pathling and
the Databricks environment:

```
spark.executor.userClassPathFirst true
```

## Environment variables

By default, Databricks uses Java 8 within its clusters, while Pathling requires
Java 11. To enable Java 11 support within your cluster, navigate to __Advanced
Options > Spark > Environment Variables__ and add the following:

```bash
JNAME=zulu11-ca-amd64
```

See the Databricks documentation on
[Libraries](https://docs.databricks.com/libraries/index.html) for more
information.
