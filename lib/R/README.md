R sparklyr API for Pathling
=======================

``pathling`` is the R [sparklyr](https://spark.rstudio.com/) API 
for [Pathling](https://pathling.csiro.au). It provides a set of functions that 
aid the use of FHIR terminology services and FHIR data within R code.

## Installation

Prerequisites: `R >= 3.5.0` (has been tested with `R 4.3.1`)

* Install the `remotes` package: `install.packages('remotes')`
* Install the `pathling` package: `remotes::install_url('[package URL]', upgrade = FALSE)`
* Install Spark: `pathling::pathling_spark_install()`



# Running on Databricks

Setup the cluster using the instructions fron  [Pathling Databricks Installation](https://pathling.csiro.au/docs/libraries/installation/databricks), 
skipping the installation of the `python` package from `PyPI`.

In the notebook use the following code to install the R package and connect to the Databricks cluster:

```r
# install 'pathling' if not installed
# replace PACKAGE_URL with the URL of the package source distribution
if (!nzchar(system.file(package='pathling'))) {
    remotes::install_url(PACKAGE_URL, upgrade = FALSE)
}

library(pathling)
pc <- ptl_connect(sparklyr::spark_connect(method = "databricks"))

# code to use pathling here
# ...

ptl_disconnect(pc)
```
