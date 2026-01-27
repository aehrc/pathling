---
sidebar_position: 1
sidebar_label: Introduction
description: Pathling provides a set of libraries that provide assistance with using FHIR and terminology services from Apache Spark applications and data science workflows.
---

# Libraries

Pathling provides a set of libraries that provide assistance with
using [FHIR](https://hl7.org/fhir/R4)
and [terminology services](https://hl7.org/fhir/R4/terminology-service.html)
from
Apache Spark applications and data science workflows.

The libraries are available for:

- [Python](pathname:///docs/python/pathling.html) (based
  on [PySpark](https://spark.apache.org/docs/latest/api/python/reference/index.html))
- [R](pathname:///docs/r/index.html) (based
  on [sparklyr](https://spark.posit.co/))
- [Java](pathname:///docs/java/index.html)

All the different Pathling library implementations allow you to run queries
locally (which is the default), or connect to a remote Spark cluster for
processing of large datasets.
