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


read_dcf <- function(path)
{
  fields <- colnames(read.dcf(path))
  as.list(read.dcf(path, keep.white = fields)[1,])
}

package_info <- function(pkgname) {
  path <- system.file("DESCRIPTION", package = pkgname)
  read_dcf(path)
}

pathling_spark_info <- function() {
  metadata <- package_info("pathling")
  list(
      spark_version = metadata[["Config/pathling/SparkVersion"]],
      hadoop_version = metadata[["Config/pathling/HadoopVersion"]]
  )
}

#' Returns the version of the Pathling R library.
#' 
#' @return The version of the Pathling R library.
#' @export
pathling_version <- function() {
  metadata[["Config/pathling/Version"]]
}

#' Installs the version of Spark/Hadoop required by pathling.
#' 
#' @description
#' Installs the version of Spark/Hadoop defined in the package metadata 
#' using the sparklyr package  \code{spark_install} function.
#' 
#' @return List with information about the installed version.
#' @export
pathling_install_spark <- function() {
  spark_info <- pathling_spark_info()
  sparklyr::spark_install(version = spark_info$spark_version, hadoop_version = spark_info$hadoop_version)
}

#' Checks if the version of Spark/Hadoop reuired by pathling is installed.
#' @return TRUE if the required version of Spark/Hadoop is installed, FALSE otherwise.
#' 
#' @importFrom rlang .data
#' @export
pathling_is_spark_installed <- function() {
  spark_info <- pathling_spark_info()
  sparklyr::spark_installed_versions() %>%
      sparklyr::filter(.data$spark == spark_info$spark_version, .data$hadoop == spark_info$hadoop_version) %>%
      nrow() > 0
}


#' Constructs the path to the package example data.
#' 
#' Construct the path to the package example data from components in a platform-independent way.
#' 
#' @param ... character vector of the path components.
#' @return The path to the examples data.
#' 
#' @family pathling examples
#' 
#' @export
#' 
#' @examples
#' pathling_examples('ndjson', 'Condition.ndjson')
pathling_examples <- function(...) {
  system.file("extdata", ..., package = "pathling")
}

#' Reads example FHIR resource data frame.
#' 
#' @description
#' \code{pathling_example_resource()} reads a FHIR resource dataframe from the package example data.
#' 
#' @param pc The PathlingContext object.
#' @param resource_name The name of the resource to read.
#' 
#' @details
#' The resorces are read from the package example data in the \code{extdata/parquet} directory. 
#' Currently the following resources are available: 'Patient' and 'Condition'.
#' 
#' @return A Spark DataFrame containing the resource data.
#' 
#' @family pathling examples
#'
#' @export
#' 
#' @examplesIf pathling_is_spark_installed()
#' pc <- pathling_connect()
#' pathling_example_resource(pc, 'Condition')
#' pathling_disconnect(pc)
pathling_example_resource <- function(pc, resource_name) {
  pc %>% pathling_spark() %>% 
      sparklyr::spark_read_parquet(pathling_examples("parquet" , paste0(resource_name, ".parquet")))
}
