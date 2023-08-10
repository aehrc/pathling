#  Copyright 2023 Commonwealth Scientific and Industrial Research
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


 #'@importFrom sparklyr j_invoke
data_sources <- function(pc) {
  j_invoke(pc, "read")
}

 #'@importFrom sparklyr j_invoke
invoke_datasource <- function(pc, name, ...) {
  pc %>%
      data_sources() %>%
      j_invoke(name, ...)
}

#' Creates a data source from a directory containing NDJSON files.
#'
#' The files must be named with the resource type code and must have the ".ndjson" extension,
#' e.g. "Patient.ndjson" or "Observation.ndjson".
#'
#' @param pc The PathlingContext object.
#' @param path The URI of the directory containing the NDJSON files.
#' @param extension The file extension to use when searching for files. Defaults to "ndjson".
#' @param file_name_mapper An optional function that maps a filename to the set of resource types
#'   that it contains. Currently not implemented.
#' @return A DataSource object that can be used to run queries against the data.
#' @export
ptl_read_ndjson <- function(pc, path, extension = "ndjson", file_name_mapper = NULL) {
  #See: issue #1601 (Implement file_name_mappers in R sparkly API)
  stopifnot(file_name_mapper == NULL)
  pc %>% invoke_datasource("ndjson", as.character(path), as.character(extension))
}

#' Creates a data source from a directory containing FHIR bundles.
#'
#' @param pc The PathlingContext object.
#' @param path The URI of the directory containing the bundles.
#' @param resource_types A sequence of resource type codes that should be extracted from the bundles.
#' @param mime_type The MIME type of the bundles. Defaults to "application/fhir+json".
#' @return A DataSource object that can be used to run queries against the data.
#' @export
ptl_read_bundles <- function(pc, path, resource_types, mime_type = MimeType$FHIR_JSON) {

  pc %>% invoke_datasource("bundles", as.character(path),
                           spark_connection(pc) %>% j_to_set(resource_types),
                           as.character(mime_type))
}

#' Creates an immutable, ad-hoc data source from a dictionary of Spark DataFrames indexed with
#' resource type codes.
#'
#' @param pc The PathlingContext object.
#' @param resources A dictionary of Spark DataFrames, where the keys are resource type codes
#'   and the values are the data frames containing the resource data.
#' @return A DataSource object that can be used to run queries against the data.
#' @export
ptl_read_datasets <- function(pc, resources) {
  resources <- as.list(resources)
  ds <- pc %>% invoke_datasource("datasets")
  for (resource_code in names(resources)) {
    resource_df <- resources[[resource_code]]
    ds %>% j_invoke("dataset", resource_code, spark_dataframe(resource_df))
  }
  ds
}

#' Creates a data source from a directory containing Parquet tables.
#'
#' Each table must be named according to the name of the resource type that it stores.
#'
#' @param pc The PathlingContext object.
#' @param path The URI of the directory containing the Parquet tables.
#' @return A DataSource object that can be used to run queries against the data.
#' @export
ptl_read_parquet <- function(pc, path) {
  pc %>% invoke_datasource("parquet", as.character(path))
}

#' Creates a data source from a directory containing Delta tables.
#'
#' Each table must be named according to the name of the resource type that it stores.
#'
#' @param pc The PathlingContext object.
#' @param path The URI of the directory containing the Delta tables.
#' @return A DataSource object that can be used to run queries against the data.
#' @export
ptl_read_delta <- function(pc, path) {
  pc %>% invoke_datasource("delta", as.character(path))
}

#' Creates a data source from a set of Spark tables, where the table names are the resource type codes.
#'
#' @param pc The PathlingContext object.
#' @param schema An optional schema name that should be used to qualify the table names.
#' @return A DataSource object that can be used to run queries against the data.
#' @export
ptl_read_tables <- function(pc, schema = NULL) {
  if (!is.null(schema)) {
    pc %>% invoke_datasource("tables", as.character(schema))
  } else {
    pc %>% invoke_datasource("tables")
  }
}


#' Reads the data for the given resource type from the data source.
#'
#' @param ds The DataSource object.
#' @param resource_code A string representing the type of FHIR resource to read data from.
#'
#' @return A Spark DataFrame containing the data for the given resource type.
#' @export
ds_read <- function(ds, resource_code) {
  jdf <- j_invoke(ds, "read", resource_code)
  sdf_register(jdf)
}

data_sinks <- function(ds) {
  j_invoke(ds, "write")
}

 #'@importFrom sparklyr invoke
invoke_datasink <- function(ds, name, ...) {
  ds %>%
      data_sinks() %>%
      invoke(name, ...)
  return(invisible(NULL))
}


#' Import modes.
#'
#' @export
ImportMode <- list(
    OVERWRITE = "overwrite",
    MERGE = "merge"
)

#' Writes the data to a directory of NDJSON files. The files will be named using the resource type and the ".ndjson" extension.
#'
#' @param ds The DataSource object.
#' @param path The URI of the directory to write the files to.
#' @param file_name_mapper An optional function that can be used to customise the mapping 
#'  of the resource type to the file name. Currently not implemented.
#'
#' @return NULL
#' @export
ds_write_ndjson <- function(ds, path, file_name_mapper = NULL) {
  #See: issue #1601 (Implement file_name_mappers in R sparkly API)
  stopifnot(file_name_mapper == NULL)
  invoke_datasink(ds, "ndjson", path)
}

#' Writes the data to a directory of Parquet files.
#'
#' @param ds The DataSource object.
#' @param path The URI of the directory to write the files to.
#'
#' @return NULL
#' @export
ds_write_parquet <- function(ds, path) {
  invoke_datasink(ds, "parquet", path)
}

#' Writes the data to a directory of Delta files.
#'
#' @param ds The DataSource object.
#' @param path The URI of the directory to write the files to.
#' @param import_mode The import mode to use when writing the data - "overwrite" will overwrite any existing data, "merge" will merge the new data with the existing data based on resource ID.
#'
#' @return NULL
#' @export
ds_write_delta <- function(ds, path, import_mode = ImportMode$OVERWRITE) {
  invoke_datasink(ds, "delta", path, import_mode)
}

#' Writes the data to a set of tables in the Spark catalog.
#'
#' @param ds The DataSource object.
#' @param schema The name of the schema to write the tables to.
#' @param import_mode The import mode to use when writing the data - "overwrite" will overwrite any existing data, "merge" will merge the new data with the existing data based on resource ID.
#'
#' @return NULL
#' @export
ds_write_tables <- function(ds, schema = NULL, import_mode = ImportMode$OVERWRITE) {
  if (!is.null(schema)) {
    invoke_datasink(ds, "tables", import_mode, schema)
  } else {
    invoke_datasink(ds, "tables", import_mode)
  }
}
