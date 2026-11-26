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

# The schema equality of spec SC-002, used by the tests of a schema that is the same. Schemas are
# the JSON text of a Spark struct type, as StructType.json() writes it.
#
# Two schemas are the same when their base schemas are equal field for field, in order at every
# depth, ignoring nullability (of fields, array elements and map values) and field metadata.
# Annotation fields are left out, since they are compared against the configuration, and so is a
# group left with only annotation fields, at any depth. The prefix that marks an annotation is read
# from Pathling's AnnotationNames, so a Spark connection must be open.

same_schema <- function(left, right) {
  prefix <- annotation_prefix()
  parse <- function(schema) jsonlite::fromJSON(schema, simplifyVector = FALSE)
  identical(schema_base(parse(left), prefix), schema_base(parse(right), prefix))
}

# Two schema sets, lists of schemas named by resource type, are the same when they name the same
# types and the schemas of each type are the same. An empty list is an empty set. A non-empty list
# that does not name every schema, or names a type twice, is refused.
same_schema_set <- function(left, right) {
  named <- function(set) {
    length(set) == 0 || (!is.null(names(set)) && all(nzchar(names(set))))
  }
  if (!named(left) || !named(right)) {
    stop("A schema set must name the resource type of every schema.")
  }
  if (anyDuplicated(names(left)) || anyDuplicated(names(right))) {
    stop("A schema set must not name a resource type twice.")
  }
  setequal(names(left), names(right)) &&
    length(left) == length(right) &&
    all(vapply(names(left), function(type) same_schema(left[[type]], right[[type]]), logical(1)))
}

# The prefix that marks an annotation, from the JVM of the open Spark connection.
annotation_prefix <- function() {
  connections <- sparklyr::spark_connection_find()
  if (length(connections) == 0) {
    stop("Schema equality needs an open Spark connection.")
  }
  sparklyr::invoke_static(
    connections[[1]], "au.csiro.pathling.utilities.AnnotationNames", "PREFIX"
  )
}

# The base of a parsed type: nullable throughout, without metadata, annotation fields or groups left
# with only annotation fields at any depth. A primitive type is a string, and the rest are lists
# with a type member.
schema_base <- function(type, prefix) {
  if (is.character(type)) {
    return(type)
  }
  switch(type$type,
    struct = {
      fields <- lapply(
        Filter(function(field) !startsWith(field$name, prefix), type$fields),
        function(field) {
          base <- schema_base(field$type, prefix)
          if (emptied(field$type, base)) NULL else list(name = field$name, type = base)
        }
      )
      list(type = "struct", fields = Filter(Negate(is.null), fields))
    },
    array = list(type = "array", elementType = schema_base(type$elementType, prefix)),
    map = list(
      type = "map",
      keyType = schema_base(type$keyType, prefix),
      valueType = schema_base(type$valueType, prefix)
    ),
    type
  )
}

# Whether a parsed group, or array of groups, that held fields has none left in its base.
emptied <- function(original, base) {
  if (is.character(original) || is.character(base)) {
    return(FALSE)
  }
  if (original$type == "array" && base$type == "array") {
    return(emptied(original$elementType, base$elementType))
  }
  original$type == "struct" && length(original$fields) > 0 &&
    base$type == "struct" && length(base$fields) == 0
}
