#  Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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


#' @importFrom sparklyr j_invoke_static
j_to_set <- function(spark, values) {
  spark %>% j_invoke_static("com.google.common.collect.ImmutableSet", "copyOf", as.list(values))
}

# Returns the first argument unless it is NULL, in which case it returns the second.
#
# This is defined here rather than imported from rlang, which as of version 1.3.0 only re-exports the
# operator that base R provides from version 4.4.0. The package supports R 3.5.0 and above, so on an
# older R there is nothing for rlang to export.
`%||%` <- function(x, y) {
  if (is.null(x)) y else x
}
