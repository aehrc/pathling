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

#' @importFrom sparklyr j_invoke
#' @importFrom sparklyr spark_connection
#' @importFrom sparklyr j_invoke_new
#' @importFrom sparklyr invoke_static
#' @importFrom purrr map
data_sources <- function(pc) {
  j_invoke(pc, "read")
}

#' Invoke a data source
#'
#' Invokes a method of the Java data source builder, after giving it the schema set to read with
#' and setting whether the sources it creates classify the layout of the data they read.
#'
#' @param pc The PathlingContext object.
#' @param name The name of the builder method.
#' @param ... The arguments of the builder method.
#' @param schemas An optional named list of the schema to read each resource type with, each a
#'   string holding a struct type's JSON form.
#' @param layout_detection Whether the source classifies the layout of the data it reads.
#' @return A jobj referring to the data source.
#'
#' @importFrom sparklyr j_invoke
#' @noRd
invoke_datasource <- function(pc, name, ..., schemas = NULL, layout_detection = TRUE) {
  sources <- pc %>%
    data_sources() %>%
    j_invoke("withLayoutDetection", as.logical(layout_detection))
  if (length(schemas) > 0) {
    sources <- sources %>%
      j_invoke("withSchemas", to_java_schema_map(sparklyr::spark_connection(pc), schemas))
  }
  sources %>% j_invoke(name, ...)
}

to_java_list <- function(sc, elements) {
  list <- j_invoke_new(sc, "java.util.ArrayList")
  for (e in elements) {
    j_invoke(list, "add", e)
  }
  list
}

#' SaveMode
#'
#' The following save modes are supported:
#' \itemize{
#'   \item{\code{OVERWRITE}: Overwrite any existing data.}
#'   \item{\code{APPEND}: Append the new data to the existing data.}
#'   \item{\code{IGNORE}: Only save the data if the file does not already exist.}
#'   \item{\code{ERROR}: Raise an error if the file already exists.}
#'   \item{\code{MERGE}: Merge the new data with the existing data based on resource ID.}
#' }
#'
#' @export
SaveMode <- list(
  OVERWRITE = "overwrite",
  APPEND = "append",
  IGNORE = "ignore",
  ERROR = "error",
  MERGE = "merge"
)

#' Create a data source from NDJSON
#'
#' Creates a data source from a directory containing NDJSON files. The files must be named with the
#' resource type code and must have the ".ndjson" extension, e.g. "Patient.ndjson" or
#' "Observation.ndjson".
#'
#' @param pc The PathlingContext object.
#' @param path The URI of the directory containing the NDJSON files.
#' @param extension The file extension to use when searching for files. Defaults to "ndjson".
#' @param file_name_mapper An optional function that maps a filename to the set of resource types
#'   that it contains. Currently not implemented.
#' @param schemas An optional named list of the schema to read each resource type with, keyed by
#'   resource type code. Each schema is a string holding a struct type's JSON form, as Spark
#'   writes it. Each resource type the list names is read with its schema, in its order: elements
#'   the schema lacks are dropped, and elements the data lacks are null. Resource types it does
#'   not name are read as usual.
#' @return A DataSource object that can be used to run queries against the data.
#'
#' @seealso \href{https://pathling.csiro.au/docs/libraries/io#ndjson}{Pathling documentation - Reading NDJSON}
#'
#' @export
#'
#' @family data source functions
#'
#' @examples \dontrun{
#' data_source <- pc %>% pathling_read_ndjson(pathling_examples("ndjson"))
#' data_source %>%
#'   ds_read("Patient") %>%
#'   sparklyr::sdf_nrow()
#' }
pathling_read_ndjson <- function(pc, path, extension = "ndjson", file_name_mapper = NULL,
                                 schemas = NULL) {
  # See: issue #1601 (Implement file_name_mappers in R sparkly API)
  stopifnot(file_name_mapper == NULL)
  pc %>% invoke_datasource(
    "ndjson", as.character(path), as.character(extension),
    schemas = schemas
  )
}

#' Create a data source from FHIR bundles
#'
#' Creates a data source from a directory containing FHIR bundles.
#'
#' @param pc The PathlingContext object.
#' @param path The URI of the directory containing the bundles.
#' @param resource_types A sequence of resource type codes that should be extracted from the bundles.
#' @param mime_type The MIME type of the bundles. Defaults to "application/fhir+json".
#' @param schemas An optional named list of the schema to read each resource type with, keyed by
#'   resource type code. Each schema is a string holding a struct type's JSON form, as Spark
#'   writes it. Each resource type the list names is read with its schema, in its order: elements
#'   the schema lacks are dropped, and elements the data lacks are null. Resource types it does
#'   not name are read as usual.
#' @return A DataSource object that can be used to run queries against the data.
#'
#' @seealso \href{https://pathling.csiro.au/docs/libraries/io#fhir-bundles}{Pathling documentation - Reading Bundles}
#'
#' @export
#'
#' @family data source functions
#'
#' @examples \dontrun{
#' data_source <- pc %>% pathling_read_bundles(
#'   pathling_examples("bundle-xml"),
#'   c("Patient", "Observation"), MimeType$FHIR_XML
#' )
#' data_source %>%
#'   ds_read("Observation") %>%
#'   sparklyr::sdf_nrow()
#' }
pathling_read_bundles <- function(pc, path, resource_types, mime_type = MimeType$FHIR_JSON,
                                  schemas = NULL) {
  pc %>% invoke_datasource(
    "bundles", as.character(path),
    sparklyr::spark_connection(pc) %>% j_to_set(resource_types),
    as.character(mime_type),
    schemas = schemas
  )
}

#' Create a data source from datasets
#'
#' Creates an immutable, ad-hoc data source from a named list of Spark datasets indexed with
#' resource type codes.
#'
#' The datasets are expected in the Parquet on FHIR layout. Their layout is not checked. Datasets in
#' the layout written by earlier releases of Pathling are not supported, and the results of a query
#' over them are undefined.
#'
#' @param pc The PathlingContext object.
#' @param resources A name list of Spark datasets, where the keys are resource type codes
#'   and the values are the data frames containing the resource data.
#' @param schemas An optional named list of the schema to read each resource type with, keyed by
#'   resource type code. Each schema is a string holding a struct type's JSON form, as Spark
#'   writes it. Each resource type the list names is read with its schema, in its order: elements
#'   the schema lacks are dropped, and elements the data lacks are null. Resource types it does
#'   not name are read as usual.
#' @return A DataSource object that can be used to run queries against the data.
#'
#' @seealso \href{https://pathling.csiro.au/docs/libraries/io#datasets}{Pathling documentation - Reading datasets}
#'
#' @export
#'
#' @family data source functions
#'
#' @examples \dontrun{
#' patient_df <- pc %>% pathling_example_resource("Patient")
#' condition_df <- pc %>% pathling_example_resource("Condition")
#' data_source <- pc %>% pathling_read_datasets(list(Patient = patient_df, Condition = condition_df))
#' data_source %>%
#'   ds_read("Patient") %>%
#'   sparklyr::sdf_nrow()
#' }
pathling_read_datasets <- function(pc, resources, schemas = NULL) {
  resources <- as.list(resources)
  ds <- pc %>% invoke_datasource("datasets", schemas = schemas)
  for (resource_code in names(resources)) {
    resource_df <- resources[[resource_code]]
    ds %>% j_invoke("dataset", resource_code, spark_dataframe(resource_df))
  }
  ds
}

#' Create a data source from Parquet tables
#'
#' @description
#' \code{pathling_read_parquet()} creates a data source from a directory containing Parquet tables.
#' Each table must be named according to the name of the resource type that it stores.
#'
#' The files of a resource type may carry differing schemas, and are read as one dataset whose
#' schema is the union of theirs. A resource type named in \code{schemas} is instead read with the
#' schema given for it: the data is projected to that schema, keeping every row.
#'
#' The layout of each resource type's data is classified as it is read, and data in the layout
#' written by earlier releases of Pathling is refused with a message naming the remedy, which is to
#' re-import it from its source. Where there is no source, the legacy export converts it to NDJSON
#' for re-import, losing what that layout did not store. The legacy export is a separate tool,
#' described in the Pathling documentation on the storage layout.
#'
#' @param pc The PathlingContext object.
#' @param path The URI of the directory containing the Parquet tables.
#' @param schemas An optional named list of the schema to read each resource type with, keyed by
#'   resource type code. Each schema is a string holding a struct type's JSON form, as Spark
#'   writes it. Each resource type the list names is read with its schema, in its order: elements
#'   the schema lacks are dropped, and elements the data lacks are null. Resource types it does
#'   not name are read as usual.
#' @param layout_detection Whether to classify the layout of each resource type's data as it is
#'   read, refusing data in the layout written by earlier releases of Pathling. Defaults to
#'   \code{TRUE}. Set it to \code{FALSE} only for conforming data that the detector misclassifies;
#'   it applies to this data source alone. Data in the earlier layout read with it disabled is
#'   not supported, and the results of a query over it are undefined.
#' @return A DataSource object that can be used to run queries against the data.
#'
#' @seealso \href{https://pathling.csiro.au/docs/libraries/io#parquet}{Pathling documentation - Reading Parquet}
#'
#' @export
#'
#' @family data source functions
#'
#' @examples \dontrun{
#' data_source <- pc %>% pathling_read_parquet(pathling_examples("parquet"))
#' data_source %>%
#'   ds_read("Patient") %>%
#'   sparklyr::sdf_nrow()
#' }
pathling_read_parquet <- function(pc, path, schemas = NULL, layout_detection = TRUE) {
  pc %>% invoke_datasource(
    "parquet", as.character(path),
    schemas = schemas, layout_detection = layout_detection
  )
}

#' Build a Java map of resource types to schemas
#'
#' Converts a named list of schemas, each a string holding a struct type's JSON form, into a
#' \code{java.util.Map} from resource type codes to \code{StructType} objects, with no limit on the
#' number of entries. The strings are passed to the JVM unchanged, joined into the JSON object of a
#' schema set, which the JVM parses. Each string is first checked to be a single JSON value, so
#' that the joined object holds exactly the given entries and a malformed string is refused with an
#' error naming its key. The JVM refuses a value that is not a struct type, naming its key.
#'
#' @param sc The Spark connection.
#' @param schemas A named list of schemas.
#'
#' @return A jobj referring to a \code{java.util.Map}.
#'
#' @importFrom sparklyr j_invoke_static
#' @noRd
to_java_schema_map <- function(sc, schemas) {
  resource_types <- names(schemas)
  if (is.null(resource_types) || any(resource_types == "")) {
    stop("Every schema must be named with the resource type code it is for.")
  }
  entries <- vapply(resource_types, function(resource_type) {
    schema <- schemas[[resource_type]]
    if (!is.character(schema) || length(schema) != 1 || !jsonlite::validate(schema)) {
      stop("The schema of ", resource_type, " is not a struct type's JSON form.")
    }
    paste0(jsonlite::toJSON(resource_type, auto_unbox = TRUE), ":", schema)
  }, character(1))
  j_invoke_static(
    sc, "au.csiro.pathling.io.SchemaSet", "parse",
    paste0("{", paste(entries, collapse = ","), "}")
  ) %>%
    j_invoke("schemas")
}

#' Create a data source from Delta tables
#'
#' @description
#' \code{pathling_read_delta()} creates a data source from a directory containing Delta tables.
#' Each table must be named according to the name of the resource type that it stores.
#'
#' The layout of each resource type's data is classified as it is read, and data in the layout
#' written by earlier releases of Pathling is refused with a message naming the remedy, which is to
#' re-import it from its source. Where there is no source, the legacy export converts it to NDJSON
#' for re-import, losing what that layout did not store. The legacy export is a separate tool,
#' described in the Pathling documentation on the storage layout.
#'
#' @param pc The PathlingContext object.
#' @param path The URI of the directory containing the Delta tables.
#' @param schemas An optional named list of the schema to read each resource type with, keyed by
#'   resource type code. Each schema is a string holding a struct type's JSON form, as Spark
#'   writes it. Each resource type the list names is read with its schema, in its order: elements
#'   the schema lacks are dropped, and elements the data lacks are null. Resource types it does
#'   not name are read as usual.
#' @param layout_detection Whether to classify the layout of each resource type's data as it is
#'   read, refusing data in the layout written by earlier releases of Pathling. Defaults to
#'   \code{TRUE}. Set it to \code{FALSE} only for conforming data that the detector misclassifies;
#'   it applies to this data source alone. Data in the earlier layout read with it disabled is
#'   not supported, and the results of a query over it are undefined.
#' @return A DataSource object that can be used to run queries against the data.
#'
#' @seealso \href{https://pathling.csiro.au/docs/libraries/io#delta-lake}{Pathling documentation - Reading Delta}
#'
#' @export
#'
#' @family data source functions
#'
#' @examples \dontrun{
#' data_source <- pc %>% pathling_read_delta(pathling_examples("delta"))
#' data_source %>%
#'   ds_read("Patient") %>%
#'   sparklyr::sdf_nrow()
#' }
pathling_read_delta <- function(pc, path, schemas = NULL, layout_detection = TRUE) {
  pc %>% invoke_datasource(
    "delta", as.character(path),
    schemas = schemas, layout_detection = layout_detection
  )
}

#' Create a data source from managed tables
#'
#' \code{pathling_read_tables()} creates a data source from a set of Spark tables,
#' where the table names are the resource type codes.
#'
#' The layout of each resource type's data is classified as it is read, and data in the layout
#' written by earlier releases of Pathling is refused with a message naming the remedy, which is to
#' re-import it from its source. Where there is no source, the legacy export converts it to NDJSON
#' for re-import, losing what that layout did not store. The legacy export is a separate tool,
#' described in the Pathling documentation on the storage layout.
#'
#' @param pc The PathlingContext object.
#' @param schema An optional schema name that should be used to qualify the table names. This is
#'   the catalog database to read the tables from, not a schema set, which is given by
#'   \code{schemas}.
#' @param schemas An optional schema set: a named list of the schema to read each resource type
#'   with, keyed by resource type code. Each schema is a string holding a struct type's JSON form, as Spark
#'   writes it. Each resource type the list names is read with its schema, in its order: elements
#'   the schema lacks are dropped, and elements the data lacks are null. Resource types it does
#'   not name are read as usual.
#' @param layout_detection Whether to classify the layout of each resource type's data as it is
#'   read, refusing data in the layout written by earlier releases of Pathling. Defaults to
#'   \code{TRUE}. Set it to \code{FALSE} only for conforming data that the detector misclassifies;
#'   it applies to this data source alone. Data in the earlier layout read with it disabled is
#'   not supported, and the results of a query over it are undefined.
#' @return A DataSource object that can be used to run queries against the data.
#'
#' @seealso \href{https://pathling.csiro.au/docs/libraries/io#managed-tables}{Pathling documentation - Reading managed tables}
#'
#' @export
#'
#' @family data source functions
#'
#' @examples \dontrun{
#' data_source <- pc %>% pathling_read_tables()
#' data_source %>%
#'   ds_read("Patient") %>%
#'   sparklyr::sdf_nrow()
#' }
pathling_read_tables <- function(pc, schema = NULL, schemas = NULL, layout_detection = TRUE) {
  if (!is.null(schema)) {
    pc %>% invoke_datasource(
      "tables", as.character(schema),
      schemas = schemas, layout_detection = layout_detection
    )
  } else {
    pc %>% invoke_datasource("tables", schemas = schemas, layout_detection = layout_detection)
  }
}

#' Get data for a resource type from a data source
#'
#' @param ds The DataSource object.
#' @param resource_code A string representing the type of FHIR resource to read data from.
#'
#' @return A Spark DataFrame containing the data for the given resource type.
#'
#' @export
#'
#' @examples \dontrun{
#' data_source <- pc %>% pathling_read_ndjson(pathling_examples("ndjson"))
#' data_source %>%
#'   ds_read("Patient") %>%
#'   sparklyr::sdf_nrow()
#' data_source %>%
#'   ds_read("Condition") %>%
#'   sparklyr::sdf_nrow()
#' }
ds_read <- function(ds, resource_code) {
  jdf <- j_invoke(ds, "read", resource_code)
  sdf_register(jdf)
}

data_sinks <- function(ds) {
  j_invoke(ds, "write")
}

#' Convert a Java WriteDetails object to an R list.
#'
#' @param java_result The Java WriteDetails object from the library API.
#' @return A list with element `file_infos`, containing a list of files created.
#'   Each file has `fhir_resource_type` and `absolute_url`.
#' @importFrom sparklyr invoke
#' @noRd
convert_write_details <- function(java_result) {
  java_file_infos <- java_result %>% invoke("fileInfos")
  size <- java_file_infos %>% invoke("size")

  file_infos <- lapply(seq_len(size), function(i) {
    fi <- java_file_infos %>% invoke("get", as.integer(i - 1))
    list(
      fhir_resource_type = fi %>% invoke("fhirResourceType"),
      absolute_url = fi %>% invoke("absoluteUrl")
    )
  })
  list(file_infos = file_infos)
}

#' @importFrom sparklyr invoke
invoke_datasink <- function(ds, name, save_mode = NULL, ...) {
  sink_builder <- ds %>% data_sinks()

  if (!is.null(save_mode)) {
    sink_builder <- sink_builder %>% invoke("saveMode", save_mode)
  }

  result <- sink_builder %>% invoke(name, ...)
  convert_write_details(result)
}


#' Write FHIR data to NDJSON files
#'
#' Writes the data from a data source to a directory of NDJSON files. Each resource type is written
#' to one or more files named with the resource type, a partition number and an identifier of the
#' write, for example "Patient.00000-<write id>.ndjson".
#'
#' @param ds The DataSource object.
#' @param path The URI of the directory to write the files to.
#' @param save_mode The save mode to use when writing the data.
#' @param file_name_mapper An optional function that can be used to customise the mapping
#'  of the resource type to the file name. Currently not implemented.
#'
#' @return A list with element \code{file_infos}, containing a list of files created.
#'   Each file has \code{fhir_resource_type} and \code{absolute_url}.
#'
#' @seealso \href{https://pathling.csiro.au/docs/libraries/io#ndjson-1}{Pathling documentation - Writing NDJSON}
#'
#' @examples \dontrun{
#' data_source <- pc %>% pathling_read_ndjson(pathling_examples("ndjson"))
#'
#' # Write the data to a directory of NDJSON files.
#' data_source %>% ds_write_ndjson(file.path(tempdir(), "ndjson"))
#' }
#'
#' @family data sink functions
#'
#' @export
ds_write_ndjson <- function(ds, path, save_mode = SaveMode$ERROR, file_name_mapper = NULL) {
  # See: issue #1601 (Implement file_name_mappers in R sparkly API)
  stopifnot(file_name_mapper == NULL)
  invoke_datasink(ds, "ndjson", save_mode, path)
}

#' Write FHIR data to Parquet files
#'
#' Writes the data from a data source to a directory of Parquet files. Each resource type is written
#' to one or more files named with the resource type, a partition number and an identifier of the
#' write, for example "Patient.00000-<write id>.parquet".
#'
#' @param ds The DataSource object.
#' @param path The URI of the directory to write the files to.
#' @param save_mode The save mode to use when writing the data.
#'
#' @return A list with element \code{file_infos}, containing a list of files created.
#'   Each file has \code{fhir_resource_type} and \code{absolute_url}.
#'
#' @seealso \href{https://pathling.csiro.au/docs/libraries/io#parquet-1}{Pathling documentation - Writing Parquet}
#'
#' @examplesIf pathling_is_spark_installed()
#' pc <- pathling_connect()
#' data_source <- pc %>% pathling_read_ndjson(pathling_examples("ndjson"))
#'
#' # Write the data to a directory of Parquet files.
#' data_source %>% ds_write_parquet(file.path(tempdir(), "parquet"))
#'
#' pathling_disconnect(pc)
#'
#' @family data sink functions
#'
#' @export
ds_write_parquet <- function(ds, path, save_mode = SaveMode$ERROR) {
  invoke_datasink(ds, "parquet", save_mode, path)
}

#' Write FHIR data to Delta files
#'
#' Writes the data from a data source to a directory of Delta files.
#'
#' @param ds The DataSource object.
#' @param path The URI of the directory to write the files to.
#' @param save_mode The save mode to use when writing the data - "overwrite" will overwrite any
#'      existing data, "merge" will merge the new data with the existing data based on resource ID.
#'
#' @return A list with element \code{file_infos}, containing a list of files created.
#'   Each file has \code{fhir_resource_type} and \code{absolute_url}.
#'
#' @seealso \href{https://pathling.csiro.au/docs/libraries/io#delta-lake-1}{Pathling documentation - Writing Delta}
#'
#' @seealso \code{\link{SaveMode}}
#'
#' @examplesIf pathling_is_spark_installed()
#' pc <- pathling_connect()
#' data_source <- pc %>% pathling_read_ndjson(pathling_examples("ndjson"))
#'
#' # Write the data to a directory of Delta files.
#' data_source %>% ds_write_delta(file.path(tempdir(), "delta"), save_mode = SaveMode$OVERWRITE)
#'
#' pathling_disconnect(pc)
#'
#' @family data sink functions
#'
#' @export
ds_write_delta <- function(ds, path, save_mode = SaveMode$OVERWRITE) {
  invoke_datasink(ds, "delta", save_mode, path)
}

#' Write FHIR data to managed tables
#'
#' Writes the data from a data source to a set of tables in the Spark catalog.
#'
#' @param ds The DataSource object.
#' @param schema The name of the schema to write the tables to. If omitted, the current database
#'      of the Spark session is used.
#' @param save_mode The save mode to use when writing the data - "overwrite" will overwrite any
#'      existing data, "merge" will merge the new data with the existing data based on resource ID.
#' @param format The table format to create tables in, such as "delta" or "parquet". If omitted,
#'      Spark's default (the \code{spark.sql.sources.default} setting) is used. Use "delta" for
#'      tables that will later be appended to or merged into: only a Delta table can be widened to
#'      admit data whose schema differs from its own, and only a Delta table can be merged into.
#'
#' @return A list with element \code{file_infos}, containing a list of files created.
#'   Each file has \code{fhir_resource_type} and \code{absolute_url}.
#'
#' @seealso \href{https://pathling.csiro.au/docs/libraries/io#managed-tables-1}{Pathling documentation - Writing managed tables}
#'
#' @seealso \code{\link{SaveMode}}
#'
#' @examplesIf pathling_is_spark_installed()
#' # Create a temporary warehouse location, which will be used when we call ds_write_tables().
#' temp_dir_path <- tempfile()
#' dir.create(temp_dir_path)
#' sc <- sparklyr::spark_connect(master = "local[*]", config = list(
#'   "sparklyr.shell.conf" = c(
#'     paste0("spark.sql.warehouse.dir=", temp_dir_path),
#'     "spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension",
#'     "spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog"
#'   )
#' ), version = pathling_spark_info()$spark_version)
#'
#' pc <- pathling_connect(sc)
#' data_source <- pc %>% pathling_read_ndjson(pathling_examples("ndjson"))
#'
#' # Write the data to a set of Delta tables in the 'default' database, which later writes can
#' # merge into.
#' data_source %>% ds_write_tables("default", save_mode = SaveMode$MERGE, format = "delta")
#'
#' pathling_disconnect(pc)
#' unlink(temp_dir_path, recursive = TRUE)
#'
#' @family data sink functions
#'
#' @export
ds_write_tables <- function(ds, schema = NULL, save_mode = SaveMode$OVERWRITE, format = NULL) {
  if (!is.null(format)) {
    # The Java method that takes a format also requires a schema, so the current database stands
    # in for an omitted one, which is where the tables would go anyway.
    if (is.null(schema)) {
      schema <- sparklyr::spark_connection(ds) %>%
        sparklyr::spark_session() %>%
        invoke("catalog") %>%
        invoke("currentDatabase")
    }
    invoke_datasink(ds, "tables", save_mode, schema, format)
  } else if (!is.null(schema)) {
    invoke_datasink(ds, "tables", save_mode, schema)
  } else {
    invoke_datasink(ds, "tables", save_mode)
  }
}

#' Create a data source from a FHIR Bulk Data Access API endpoint
#'
#' Creates a data source by downloading data from a FHIR server that implements the FHIR Bulk Data
#' Access API.
#'
#' @param pc The PathlingContext object.
#' @param fhir_endpoint_url The URL of the FHIR server to export from.
#' @param output_dir The directory to write the output files to.
#' @param group_id Optional group ID for group-level export.
#' @param patients Optional list of patient IDs for patient-level export.
#' @param types List of FHIR resource types to include.
#' @param output_format The format of the output data. Defaults to "application/fhir+ndjson".
#' @param since Only include resources modified after this timestamp.
#' @param elements List of FHIR elements to include.
#' @param type_filters FHIR search queries to filter resources.
#' @param include_associated_data Pre-defined set of FHIR resources to include.
#' @param output_extension File extension for output files. Defaults to "ndjson".
#' @param timeout Optional timeout duration in seconds.
#' @param max_concurrent_downloads Maximum number of concurrent downloads. Defaults to 10.
#' @param auth_config Optional authentication configuration list with the following possible elements:
#'   \itemize{
#'     \item{enabled: Whether authentication is enabled (default: FALSE)}
#'     \item{client_id: The client ID to use for authentication}
#'     \item{private_key_jwk: The private key in JWK format}
#'     \item{client_secret: The client secret to use for authentication}
#'     \item{token_endpoint: The token endpoint URL}
#'     \item{use_smart: Whether to use SMART authentication (default: TRUE)}
#'     \item{use_form_for_basic_auth: Whether to use form-based basic auth (default: FALSE)}
#'     \item{scope: The scope to request}
#'     \item{token_expiry_tolerance: The token expiry tolerance in seconds (default: 120)}
#'   }
#' @param schemas An optional named list of the schema to read each resource type with, keyed by
#'   resource type code. Each schema is a string holding a struct type's JSON form, as Spark
#'   writes it. Each resource type the list names is read with its schema, in its order: elements
#'   the schema lacks are dropped, and elements the data lacks are null. Resource types it does
#'   not name are read as usual.
#' @return A DataSource object that can be used to run queries against the data.
#'
#' @seealso \href{https://pathling.csiro.au/docs/libraries/io#fhir-bulk-data-api}{Pathling documentation - Reading from Bulk Data API}
#'
#' @export
#'
#' @family data source functions
#'
#' @examples \dontrun{
#' pc <- pathling_connect()
#'
#' # Basic system-level export
#' data_source <- pc %>% pathling_read_bulk(
#'   fhir_endpoint_url = "https://bulk-data.smarthealthit.org/fhir",
#'   output_dir = "/tmp/bulk_export"
#' )
#'
#' # Group-level export with filters
#' data_source <- pc %>% pathling_read_bulk(
#'   fhir_endpoint_url = "https://bulk-data.smarthealthit.org/fhir",
#'   output_dir = "/tmp/bulk_export",
#'   group_id = "group-1",
#'   types = c("Patient", "Observation"),
#'   elements = c("id", "status"),
#'   since = as.POSIXct("2023-01-01")
#' )
#'
#' # Patient-level export with auth
#' data_source <- pc %>% pathling_read_bulk(
#'   fhir_endpoint_url = "https://bulk-data.smarthealthit.org/fhir",
#'   output_dir = "/tmp/bulk_export",
#'   patients = c(
#'     "123", # Just the ID portion
#'     "456"
#'   ),
#'   auth_config = list(
#'     enabled = TRUE,
#'     client_id = "my-client-id",
#'     private_key_jwk = '{ "kty":"RSA", ...}',
#'     scope = "system/*.read"
#'   )
#' )
#'
#' pathling_disconnect(pc)
#' }
pathling_read_bulk <- function(pc,
                               fhir_endpoint_url,
                               output_dir,
                               group_id = NULL,
                               patients = NULL,
                               types = NULL,
                               output_format = "application/fhir+ndjson",
                               since = NULL,
                               elements = NULL,
                               type_filters = NULL,
                               include_associated_data = NULL,
                               output_extension = "ndjson",
                               timeout = NULL,
                               max_concurrent_downloads = 10,
                               auth_config = NULL,
                               schemas = NULL) {
  # Validate required parameters.
  if (missing(fhir_endpoint_url)) {
    stop("argument \"fhir_endpoint_url\" is missing")
  }
  if (missing(output_dir)) {
    stop("argument \"output_dir\" is missing")
  }

  # Get the appropriate BulkExportClient builder based on export type
  sc <- spark_connection(pc)
  builder_class <- "au.csiro.fhir.export.BulkExportClient"

  if (!is.null(group_id)) {
    builder <- invoke_static(sc, builder_class, "groupBuilder", as.character(group_id))
  } else if (!is.null(patients)) {
    builder <- invoke_static(sc, builder_class, "patientBuilder")
  } else {
    builder <- invoke_static(sc, builder_class, "systemBuilder")
  }

  # Configure the basic parameters.
  builder <- builder %>%
    j_invoke("withFhirEndpointUrl", as.character(fhir_endpoint_url)) %>%
    j_invoke("withOutputDir", as.character(output_dir)) %>%
    j_invoke("withTypes", to_java_list(sc, types))

  # Configure optional parameters if provided.
  if (!is.null(output_format)) {
    builder <- builder %>% j_invoke("withOutputFormat", as.character(output_format))
  }
  if (!is.null(since)) {
    instant <- j_invoke_static(sc, "java.time.Instant", "ofEpochMilli", as.numeric(since) * 1000)
    builder <- builder %>% j_invoke("withSince", instant)
  }
  if (!is.null(patients)) {
    j_objects <- purrr::map(patients, function(r) {
      j_invoke_static(
        sc, "au.csiro.fhir.model.Reference",
        "of", r
      )
    })
    builder <- builder %>% j_invoke("withPatients", to_java_list(sc, j_objects))
  }
  if (!is.null(elements)) {
    builder <- builder %>% j_invoke("withElements", to_java_list(sc, elements))
  }
  if (!is.null(type_filters)) {
    builder <- builder %>% j_invoke("withTypeFilters", to_java_list(sc, type_filters))
  }
  if (!is.null(include_associated_data)) {
    builder <- builder %>% j_invoke(
      "withIncludeAssociatedData",
      to_java_list(sc, include_associated_data)
    )
  }
  if (!is.null(output_extension)) {
    builder <- builder %>% j_invoke("withOutputExtension", as.character(output_extension))
  }
  if (!is.null(timeout)) {
    j_object <- j_invoke_static(sc, "java.time.Duration", "ofSeconds", as.numeric(timeout))
    builder <- builder %>% j_invoke("withTimeout", j_object)
  }
  if (!is.null(max_concurrent_downloads)) {
    builder <- builder %>% j_invoke("withMaxConcurrentDownloads", as.integer(max_concurrent_downloads))
  }

  # Configure authentication if provided.
  if (!is.null(auth_config)) {
    auth_builder <- j_invoke_new(sc, "au.csiro.pathling.auth.AuthConfig$AuthConfigBuilder")

    # Set defaults to match Java class.
    auth_builder <- auth_builder %>%
      j_invoke("enabled", FALSE) %>%
      j_invoke("useSMART", TRUE) %>%
      j_invoke("useFormForBasicAuth", FALSE) %>%
      j_invoke("tokenExpiryTolerance", 120L)

    # Map R config to Java builder methods.
    if (!is.null(auth_config$enabled)) {
      auth_builder <- auth_builder %>% j_invoke("enabled", auth_config$enabled)
    }

    if (!is.null(auth_config$use_smart)) {
      auth_builder <- auth_builder %>% j_invoke("useSMART", auth_config$use_smart)
    }

    if (!is.null(auth_config$token_endpoint)) {
      auth_builder <- auth_builder %>% j_invoke("tokenEndpoint", auth_config$token_endpoint)
    }

    if (!is.null(auth_config$client_id)) {
      auth_builder <- auth_builder %>% j_invoke("clientId", auth_config$client_id)
    }

    if (!is.null(auth_config$client_secret)) {
      auth_builder <- auth_builder %>% j_invoke("clientSecret", auth_config$client_secret)
    }

    if (!is.null(auth_config$private_key_jwk)) {
      auth_builder <- auth_builder %>% j_invoke("privateKeyJWK", auth_config$private_key_jwk)
    }

    if (!is.null(auth_config$use_form_for_basic_auth)) {
      auth_builder <- auth_builder %>% j_invoke(
        "useFormForBasicAuth",
        auth_config$use_form_for_basic_auth
      )
    }

    if (!is.null(auth_config$scope)) {
      auth_builder <- auth_builder %>% j_invoke("scope", auth_config$scope)
    }

    if (!is.null(auth_config$token_expiry_tolerance)) {
      auth_builder <- auth_builder %>% j_invoke(
        "tokenExpiryTolerance",
        as.integer(auth_config$token_expiry_tolerance)
      )
    }

    # Build auth config and add to builder.
    auth_config_obj <- auth_builder %>% j_invoke("build")
    builder <- builder %>% j_invoke("withAuthConfig", auth_config_obj)
  }

  # Build the BulkExportClient.
  client <- builder %>% j_invoke("build")

  # Pass the client to the bulk method.
  pc %>% invoke_datasource("bulk", client, schemas = schemas)
}

#' Get the schemas of a data source
#'
#' Returns the schema of each resource type a data source holds, as a schema set: a named list
#' keyed by resource type code, each value a string holding a struct type's JSON form. The set can
#' be saved with \code{\link{pathling_write_schemas}} and given to any source to read the same
#' resource types with. Getting it runs no job of its own, but a resource type that has not been
#' read yet is read first, which for NDJSON, bundles or XML read without a schema set means fitting
#' its schema to the data.
#'
#' @param ds The DataSource object.
#'
#' @return A named list of schemas, keyed by resource type code.
#'
#' @family schema set functions
#'
#' @importFrom sparklyr j_invoke
#'
#' @export
#'
#' @examples \dontrun{
#' data_source <- pc %>% pathling_read_ndjson(pathling_examples("ndjson"))
#' schemas <- data_source %>% ds_schemas()
#' }
ds_schemas <- function(ds) {
  from_java_schema_map(j_invoke(ds, "schemas"))
}
