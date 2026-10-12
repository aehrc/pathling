# Scripts that generate package data

These scripts are excluded from the built package.

- `example_data.R` regenerates the Parquet and Delta example data in
  `inst/extdata` from the NDJSON example data. Run it from `lib/R` with
  `Rscript data-raw/example_data.R`, after installing the current build of
  `library-runtime` into the local Maven repository.
