R sparklyr API for Pathling
=======================

``pathling`` package is the R [sparklyr](https://spark.rstudio.com/) API
for [Pathling](https://pathling.csiro.au). It provides a set of functions that
aid the use of FHIR terminology services and FHIR data within R code.

# Local Installation

Prerequisites: `R >= 3.5.0` (has been tested with `R 4.3.1`)

* Install the `remotes` package: `install.packages('remotes')`
* Install the `pathling`
  package: `remotes::install_url('[package URL]', upgrade = FALSE)`
* Install Spark version required by
  Pathling: `pathling::pathling_spark_install()`

## Running on Windows

Additional steps are required to run Pathling on Windows, related to setting up
Hadoop on Windows. These are described in
the [Pathling Windows Installation](https://pathling.csiro.au/docs/libraries/installation/windows)
documentation.

In addition `%HADOOP_HOME%\bin` needs to be added to the `PATH` environment
variable.

Both env variables (`HADOOP_HOME` and `PATH`) need to be set
as `System variables`, not user variables. In order toset the `HADOOP_HOME`
variable use the `Browse Directory ...`  button, as typing the path in the text
box may not work.

To verify that the variables are set correctly open a new command prompt and
run:

```cmd
dir %HADOOP_HOME%
hadoop
```

Both commands should run without errors.

Also, please be aware that Windows does not allow deleting files that are in
use. This may lead to some problems in removing/upgrading 'pathling' package if
the current version is being uses by Spark/JVM process running in a background,
either for an exising pathling session or one that was not properly closed.

# Getting Started

The example below shows how to use `pathling` terminology functions to find
codes and names of viral diseases in an R data frame. The dataframe `conditions`
is an example dataset that comes with the `pathling` package.

```R
library(sparklyr)
library(pathling)

# create a default pathling context
pc <- ptl_connect()

# copy the R data frame to the spark data frame
conditions_sdf <- pc %>%
        ptl_spark() %>%
        copy_to(conditions, overwrite = TRUE)


# define an ECL expression for viral diseases
VIRAL_DISEASE_ECL <- '<< 64572001|Disease| : (
      << 370135005|Pathological process| = << 441862004|Infectious process|,
      << 246075003|Causative agent| = << 49872002|Virus|
    )'

# use pathling terminology functions and dplr verbs to find codes for viral diseases and obtain their display names
result <- conditions_sdf %>%
        filter(!!trm_member_of(!!trm_to_snomed_coding(CODE), !!trm_to_ecl_value_set(VIRAL_DISEASE_ECL))) %>%
        mutate(DISPLAY_NAME = !!trm_display(!!trm_to_snomed_coding(CODE))) %>%
        select(CODE, DISPLAY_NAME) %>%
        distinct() %>%
        collect()

# disconnect from the pathling context
pc %>% ptl_disconnect()

# as we used `collect()` `result` is also an R data frame
result %>% show()
```

This should produce the following output:

```
# A tibble: 2 × 2
       CODE DISPLAY_NAME           
      <int> <chr>                  
1 195662009 Acute viral pharyngitis
2 444814009 Viral sinusitis   
```

Please note that in this example both the input and output are R data frames,
even though internally they were processed as Spark/Sparklyr data frames.

To find out about other `pathling` capabilities please explore the examples in
the help topics for `pathling` functions. In particular these are some good
staring points:

- `?ptl_connect` for information about creating and configuring pathling
  contexts.
- `?trm_display` and `?trm_to_snomed_coding` for terminology functions.
- `?ds_aggregate` and `?ds_extract` for pathling queries.
- `?ptl_encode` for encoding of FHIR resources into data frames.
- `?ptl_read_ndjson` and `?ds_write_ndjson` for reading and writing FHIR
  resources in various formats.

# Running on Databricks

Setup the cluster using the instructions
from [Pathling Databricks Installation](https://pathling.csiro.au/docs/libraries/installation/databricks)
, skipping the installation of the `python` package from `PyPI`.

In the notebook use the following code to install the R package and connect to
the Databricks cluster:

```r
# install 'pathling' if not installed
# replace PACKAGE_URL with the URL of the package source distribution
if (!nzchar(system.file(package = 'pathling'))) {
    remotes::install_url(PACKAGE_URL, upgrade = FALSE)
}

library(pathling)
pc <- ptl_connect(sparklyr::spark_connect(method = "databricks"))

# code to use pathling here
# ...

ptl_disconnect(pc)
```
