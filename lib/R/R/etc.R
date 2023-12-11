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

#' Get versions of Spark and other dependencies
#'
#' Returns the versions of Spark and Spark packages used by the Pathling R library.
#' 
#' @return A list containing the following keys:
#' \itemize{
#'   \item{\code{spark_version}: The version of Spark used by Pathling.}
#'   \item{\code{scala_version}: The version of Scala used by Pathling.}
#'   \item{\code{hadoop_version}: The version of Hadoop used by Pathling.}
#'   \item{\code{hadoop_major_version}: The major version of Hadoop used by Pathling.}
#'   \item{\code{delta_version}: The version of Delta used by Pathling.}
#' }
#' 
#' @family installation functions
#' 
#' @export
pathling_spark_info <- function() {
  metadata <- package_info("pathling")
  list(
      spark_version = metadata[["Config/pathling/SparkVersion"]],
      scala_version = metadata[["Config/pathling/ScalaVersion"]],
      hadoop_version = metadata[["Config/pathling/HadoopVersion"]],
      hadoop_major_version = metadata[["Config/pathling/HadoopVersion"]],
      delta_version = metadata[["Config/pathling/DeltaVersion"]]
  )
}

#' Get version of Pathling
#' 
#' @return The version of the Pathling R library.
#' 
#' @family installation functions
#' 
#' @export
pathling_version <- function() {
  metadata <- package_info("pathling")
  metadata[["Config/pathling/Version"]]
}

#' Install Spark
#' 
#' Installs the version of Spark/Hadoop defined in the package metadata using the 
#' \code{sparklyr::spark_install} function.
#' 
#' @return List with information about the installed version.
#' 
#' @family installation functions
#' 
#' @export
pathling_install_spark <- function() {
  spark_info <- pathling_spark_info()
  sparklyr::spark_install(version = spark_info$spark_version, hadoop_version = spark_info$hadoop_major_version)
}

#' Check if Spark is installed
#' 
#' Checks if the version of Spark/Hadoop required by Pathling is installed.
#' 
#' @return \code{TRUE} if the required version of Spark/Hadoop is installed, \code{FALSE} otherwise.
#' 
#' @importFrom rlang .data
#' 
#' @family installation functions
#' 
#' @export
pathling_is_spark_installed <- function() {
  spark_info <- pathling_spark_info()
  sparklyr::spark_installed_versions() %>%
      sparklyr::filter(.data$spark == spark_info$spark_version, .data$hadoop == spark_info$hadoop_major_version) %>%
      nrow() > 0
}


#' Get path to Pathling example data
#' 
#' Construct the path to the package example data in a platform-independent way.
#' 
#' @param ... character vector of the path components.
#' @return The path to the examples data.
#' 
#' @family example functions
#' 
#' @export
#' 
#' @examples
#' pathling_examples('ndjson', 'Condition.ndjson')
pathling_examples <- function(...) {
  system.file("extdata", ..., package = "pathling")
}

#' Read resource from Pathling example data
#' 
#' Reads a FHIR resource dataframe from the package example data.
#' 
#' The resources are read from the package example data in the \code{extdata/parquet} directory. 
#' Currently the following resources are available: 'Patient' and 'Condition'.
#' 
#' @param pc The PathlingContext object.
#' @param resource_name The name of the resource to read.
#' 
#' @return A Spark DataFrame containing the resource data.
#' 
#' @family example functions
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
