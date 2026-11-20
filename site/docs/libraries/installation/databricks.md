---
sidebar_position: 3
description: Instructions for installing the Pathling library on a Databricks cluster.
---

# Databricks installation

To install the Pathling library on a [Databricks](https://www.databricks.com/)
cluster, navigate to the "Compute" section and click on the cluster. Click on
the "Libraries" tab, and click "Install new".

Pathling is built for
[Databricks Runtime 18 LTS](https://docs.databricks.com/aws/en/release-notes/runtime/18),
which runs Spark 4.1 and uses Java 21 by default, the version Pathling requires.

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
