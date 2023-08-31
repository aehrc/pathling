R sparklyr API for Pathling
=======================

``pathlyr`` is the R [sparklyr](https://spark.rstudio.com/) API 
for [Pathling](https://pathling.csiro.au). It provides a set of functions that 
aid the use of FHIR terminology services and FHIR data within R code.

## Installation

* Install the `remotes` package: `install.packages('remotes')`
* Install the `pathlyr` package: `remotes::install_url('[package URL]', upgrade = FALSE)
* Install Spark: `pathlyr:::ptl_spark_install()`
