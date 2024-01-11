R API for Pathling
==================

``pathling`` package is the R API for [Pathling](https://pathling.csiro.au),
based on [sparklyr](https://spark.rstudio.com/). It provides a set of functions
that aid the use of FHIR terminology services and FHIR data within R code.

## Local installation

Prerequisites:

- R >= 3.5.0 (tested with 4.3.1)

To install, run these commands:

```r
# Install the `pathling` package.
install.packages('pathling')

# Install the Spark version required by Pathling.
pathling::pathling_install_spark()
```

### Running on Windows

Additional steps are required to run Pathling on Windows, related to setting up
Hadoop on Windows. These are described in
the [Pathling Windows Installation](https://pathling.csiro.au/docs/libraries/installation/windows)
documentation.

### Running on Databricks

See [Pathling Databricks installation](https://pathling.csiro.au/docs/libraries/installation/databricks) for instructions on how to install
the Pathling R API on Databricks.

## Getting started

The example below shows how to use `pathling` terminology functions to find
codes and names of viral diseases in an R data frame. The dataframe `conditions`
is an example dataset that comes with the `pathling` package.

```R
library(sparklyr)
library(pathling)

# Create a default Pathling context.
pc <- pathling_connect()

# Copy the R data frame to a Spark data frame.
conditions_sdf <- pc %>%
        pathling_spark() %>%
        copy_to(conditions, overwrite = TRUE)


# Define an ECL expression for viral diseases.
VIRAL_DISEASE_ECL <- '<< 64572001|Disease| : (
      << 370135005|Pathological process| = << 441862004|Infectious process|,
      << 246075003|Causative agent| = << 49872002|Virus|
    )'

# Use Pathling terminology functions and dplyr verbs to find codes for viral 
# diseases and obtain their display names.
result <- conditions_sdf %>%
        filter(!!tx_member_of(!!tx_to_snomed_coding(CODE), !!tx_to_ecl_value_set(VIRAL_DISEASE_ECL))) %>%
        mutate(DISPLAY_NAME = !!tx_display(!!tx_to_snomed_coding(CODE))) %>%
        select(CODE, DISPLAY_NAME) %>%
        distinct() %>%
        collect()

# Disconnect from the Pathling context.
pc %>% pathling_disconnect()

# As we used collect(), result is also an R data frame.
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

To find out about other Pathling capabilities please explore the examples in
the help topics for `pathling` functions. In particular these are some good
starting points:

- `?pathling_connect` for information about creating and configuring Pathling
  contexts.
- `?tx_display` and `?tx_to_snomed_coding` for terminology functions.
- `?ds_aggregate` and `?ds_extract` for pathling queries.
- `?pathling_encode` for encoding of FHIR resources into data frames.
- `?pathling_read_ndjson` and `?ds_write_ndjson` for reading and writing FHIR
  resources in various formats.

## Developer notes (MacOS)

The following packages are needed to generate pdf manuals:

    brew install basictex
    brew install freetype

Then run the following to install the `inconsolata` fonts used in R manuals:

    # Install texlive packages needed to build R package vignettes
    sudo tlmgr update --self
    sudo tlmgr update --all
    sudo tlmgr install titling framed inconsolata
    sudo tlmgr install collection-fontsrecommended

The following packages may be needed to build the dependencies of `devtools`.

    brew install harfbuzz fribidi
    brew install libjpeg
    brew install libtiff
    brew install libgit2


To setup the dev environment, run the following commands:

Use miniconda to install R in the activated dev environment:

    # run 'conda activate your-dev-env-name' first
    conda install -y -c conda-forge r-base=4.1.3

Install the `devtools` R package:

    Rscript --vanilla -e 'install.packages("devtools", repos="https://cloud.r-project.org/")'

Pathling is copyright © 2018-2023, Commonwealth Scientific and Industrial
Research Organisation
(CSIRO) ABN 41 687 119 230. Licensed under
the [Apache License, version 2.0](https://www.apache.org/licenses/LICENSE-2.0).
