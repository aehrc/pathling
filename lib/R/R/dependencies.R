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

spark_dependencies <- function(spark_version, scala_version, ...) {
  spark_info <- pathling_spark_info()
  sparklyr::spark_dependency(
      packages = c(
          paste0("au.csiro.pathling:library-runtime:", pathling_version()),
          paste0("io.delta:delta-spark_", spark_info$scala_version, ":", spark_info$delta_version)
      )
  )
}

.onLoad <- function(libname, pkgname) {
  sparklyr::register_extension(pkgname)
}
