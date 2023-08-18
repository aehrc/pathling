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


#' Checks if the version of Spark/Hadoop reuired by PathLyr is installed.
#' @return TRUE if the required version of Spark/Hadoop is installed, FALSE otherwise.
#' 
#' @importFrom rlang .data
#' @export
ptl_is_spark_installed <- function() {
  sparklyr::spark_installed_versions() %>% 
      sparklyr::filter(.data$spark=='3.3.2', .data$hadoop=='3') %>% nrow() > 0 
}
