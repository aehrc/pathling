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

for_each_with_name <-function(sequence, FUN, ...) {
  sequence_names <- names(sequence)
  char_sequence <- as.character(sequence)
  for (i in seq_along(char_sequence)) {
    name <- if (is.null(sequence_names)) "" else sequence_names[i]
    value <- char_sequence[i]
    FUN(value, name, ...)
  }
}

#' Execute an aggregate query
#'
#' Executes an aggregate query over FHIR data. The query calculates summary values based on 
#' aggregations and groupings of FHIR resources.
#'
#' @param ds The DataSource object containing the data to be queried.
#' @param subject_resource A string representing the type of FHIR resource to aggregate data from.
#' @param aggregations A named list of FHIRPath expressions that calculate a summary value from each
#'   grouping. The expressions must be singular.
#' @param groupings An optional named list of FHIRPath expressions that determine which groupings
#'   the resources should be counted within.
#' @param filters An optional sequence of FHIRPath expressions that can be evaluated against each resource
#'   in the data set to determine whether it is included within the result. The expression must evaluate to a
#'   Boolean value. Multiple filters are combined using logical AND operation.
#' @return A Spark DataFrame containing the aggregated data.
#' 
#' @family FHIRPath queries
#' 
#' @importFrom sparklyr j_invoke sdf_register
#' 
#' @seealso \href{https://pathling.csiro.au/docs/libraries/fhirpath-query#aggregate}{Pathling documentation - Aggregate}
#'
#' @export
#' @examplesIf pathling_is_spark_installed()
#' pc <- pathling_connect()
#' data_source <- pc %>% pathling_read_ndjson(pathling_examples('ndjson'))
#' data_source %>% ds_aggregate('Patient',
#'      aggregations = c(patientCount='count()', 'id.count()'),
#'      groupings = c('gender', givenName='name.given'),
#'      filters = c('birthDate > @1950-01-01')
#' )
#' pathling_disconnect(pc)
ds_aggregate <- function(ds, subject_resource, aggregations, groupings = NULL, filters = NULL) {
  q <- j_invoke(ds, "aggregate", as.character(subject_resource))
  
  for_each_with_name(aggregations, function(expression, name) {
    if (nchar(name) == 0) {
      j_invoke(q, "aggregation", expression)
    } else {
      j_invoke(q, "aggregation", expression, name)
    }
  })

  for_each_with_name(groupings, function(expression, name) {
    if (nchar(name) == 0) {
      j_invoke(q, "grouping", expression)
    } else {
      j_invoke(q, "grouping", expression, name)
    }
  })
  
  if (!is.null(filters)) {
    for (f in as.character(filters)) {
      j_invoke(q, "filter", f)
    }
  }
  
  sdf_register(j_invoke(q, "execute"))
}

#' Execute an extract query
#' 
#' Executes an extract query over FHIR data. This type of query extracts specified columns from 
#' FHIR resources in a tabular format.
#'
#' @param ds The DataSource object containing the data to be queried.
#' @param subject_resource A string representing the type of FHIR resource to extract data from.
#' @param columns A named list of FHIRPath expressions that define the columns to include in the extract.
#' @param filters An optional sequence of FHIRPath expressions that can be evaluated against each resource
#'   in the data set to determine whether it is included within the result. The expression must evaluate to a
#'   Boolean value. Multiple filters are combined using AND logic.
#' @return A Spark DataFrame containing the extracted data.
#' 
#' @family FHIRPath queries
#' 
#' @importFrom sparklyr j_invoke sdf_register
#' 
#' @seealso \href{https://pathling.csiro.au/docs/libraries/fhirpath-query#extract}{Pathling documentation - Extract}
#' 
#' @export 
#' @examplesIf pathling_is_spark_installed()
#' pc <- pathling_connect()
#' data_source <- pc %>% pathling_read_ndjson(pathling_examples('ndjson'))
#' data_source %>% ds_extract('Patient',
#'      columns = c('gender', givenName='name.given'),
#'      filters = c('birthDate > @1950-01-01')
#' )
#' pathling_disconnect(pc)
ds_extract <- function(ds, subject_resource, columns, filters = NULL) {
  q <- j_invoke(ds, "extract", as.character(subject_resource))

  for_each_with_name(columns, function(expression, name) {
    if (nchar(name) == 0) {
      j_invoke(q, "column", expression)
    } else {
      j_invoke(q, "column", expression, name)
    }
  })

  if (!is.null(filters)) {
    for (f in as.character(filters)) {
      j_invoke(q, "filter", f)
    }
  }

  sdf_register(j_invoke(q, "execute"))
}
