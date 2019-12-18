---
layout: page
title: APIs for Python and R
nav_order: 4
parent: Roadmap
grand_parent: Documentation
---

# APIs for Python and R

Language-specific APIs will be developed that will allow users of Python and R
to access the functionality within Pathling within their own language
environment. The ability to run operations using a Spark cluster would remain,
as this is an important capability when dealing with large datasets.

Instead of calling a FHIR API to perform an operation, it will be possible to
call a function and get a Spark DataFrame object returned. The Spark DataFrame
could then be "collected" into a Pandas or R DataFrame at the point at which it
makes sense to bring the data into the local language environment, in the same
way that this is currently done when using PySpark or SparkR.

It will be important for these libraries to be able to be installed into a local
environment using language-native package management utilities, i.e. pip and
CRAN.

Next: [SMART Authorisation](./smart.html)
