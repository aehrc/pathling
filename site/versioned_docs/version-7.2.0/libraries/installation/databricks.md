---
sidebar_position: 3
description: Instructions for installing the Pathling library on a Databricks cluster.
---

# Databricks installation

Pathling has been tested
on [Databricks Runtime 14.3 LTS](https://docs.databricks.com/en/release-notes/runtime/14.3lts.html).

To install the Pathling library on a [Databricks](https://www.databricks.com/)
cluster, navigate to the "Compute" section and click on the cluster. Click on
the "Libraries" tab, and click "Install new".

Install the core Pathling functionality by selecting "Maven" as the library
source and installing
the [library runtime Maven package](https://central.sonatype.com/artifact/au.csiro.pathling/library-runtime).

You can the optionally install
the [PyPI package](https://pypi.org/project/pathling/) for Python support, 
and/or the [R package](https://cran.r-project.org/package=pathling).

Once the cluster is restarted, Pathling should be available for use within
notebooks.

See the Databricks documentation
on [Libraries](https://docs.databricks.com/libraries/index.html) for more
information.

## Environment variables

By default, Databricks uses Java 8 within its clusters, while Pathling requires
Java 17. To enable Java 17 support within your cluster, navigate to __Advanced
Options > Spark > Environment Variables__ and add the following:

```bash
JNAME=zulu17-ca-amd64
```
