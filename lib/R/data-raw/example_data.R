#  Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
#  Organisation (CSIRO) ABN 41 687 119 230.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

# Regenerates the Parquet and Delta example data in inst/extdata/parquet and inst/extdata/delta
# from the NDJSON example data in inst/extdata/ndjson, in the layout the current build writes.
#
# Run it from the package directory (lib/R) after installing the current build of
# library-runtime into the local Maven repository (mvn install -pl library-runtime -am):
#
#   Rscript data-raw/example_data.R
#
# The script is written in R, through sparklyr and the package's own API, so that the data is
# encoded and written exactly as a user of the package would encode and write it. The package is
# loaded from source, so it resolves the library-runtime version named in DESCRIPTION.
#
# Each resource type is written once, through the Parquet sink, as a single Parquet file. That file
# is placed in both example directories under the short name part-00000.snappy.parquet, because
# the names Spark gives its files make paths in the built package longer than the 100 bytes that
# R CMD build accepts without a warning. The Delta table is then created around its copy with
# Delta's own conversion, so that its log, checkpoint and checksums describe the file under the
# name it has. Hadoop's hidden checksum files are removed, as they are not part of either format.
#
# The conversion is made through a temporary catalog table rather than from the path alone. Given
# a path, Delta infers the schema itself, and Delta 4.0.0 does so with a constructor of Spark's
# Parquet schema converter that Spark 4.0.2 does not have (the versions DESCRIPTION named when
# this was written), so it fails. Given a catalog table, it takes the schema the catalog holds,
# which Spark inferred. The session uses an in-memory catalog and a temporary warehouse so that
# the table leaves nothing behind.

pkgload::load_all(".", quiet = TRUE)

resource_types <- c("Condition", "Patient")
extdata <- normalizePath(file.path("inst", "extdata"), mustWork = TRUE)
part_file_name <- "part-00000.snappy.parquet"

remove_hadoop_checksums <- function(directory) {
  checksums <- list.files(directory,
    pattern = "^\\..*\\.crc$", all.files = TRUE,
    recursive = TRUE, full.names = TRUE
  )
  unlink(checksums)
}

run_sql <- function(statement) invisible(sparklyr::invoke(spark, "sql", statement))

sc <- sparklyr::spark_connect(
  master = "local[1]",
  config = list(
    "sparklyr.connect.enablehivesupport" = FALSE,
    "sparklyr.shell.conf" = c(
      "spark.sql.catalogImplementation=in-memory",
      paste0("spark.sql.warehouse.dir=", file.path(tempdir(), "warehouse")),
      "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
      "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
    )
  )
)
pc <- pathling_connect(sc)
spark <- sparklyr::spark_session(sc)

staging <- tempfile("pathling-example-data-")
pc %>%
  pathling_read_ndjson(file.path(extdata, "ndjson")) %>%
  ds_write_parquet(staging)

for (resource_type in resource_types) {
  written <- list.files(staging, pattern = paste0("^", resource_type, "\\..*\\.parquet$"))
  if (length(written) != 1) {
    stop(sprintf(
      "Expected one Parquet file for %s, found %d: %s",
      resource_type, length(written), paste(written, collapse = ", ")
    ))
  }
  for (format in c("parquet", "delta")) {
    table <- file.path(extdata, format, paste0(resource_type, ".parquet"))
    unlink(table, recursive = TRUE)
    dir.create(table, recursive = TRUE)
    if (!file.copy(file.path(staging, written), file.path(table, part_file_name))) {
      stop(sprintf("Could not copy %s to %s", written, table))
    }
  }
  delta_table <- file.path(extdata, "delta", paste0(resource_type, ".parquet"))
  catalog_table <- paste0("example_", tolower(resource_type))
  run_sql(sprintf("CREATE TABLE %s USING parquet LOCATION '%s'", catalog_table, delta_table))
  run_sql(sprintf("CONVERT TO DELTA %s", catalog_table))
  # The table is external, so dropping it leaves its files in place.
  run_sql(sprintf("DROP TABLE %s", catalog_table))
  # Delta creates a directory for staged commits that it leaves empty.
  unlink(file.path(delta_table, "_delta_log", "_staged_commits"), recursive = TRUE)
}

pc %>% pathling_disconnect()
remove_hadoop_checksums(file.path(extdata, "parquet"))
remove_hadoop_checksums(file.path(extdata, "delta"))
unlink(staging, recursive = TRUE)
