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
# group that holds only annotation fields.

same_schema <- function(left, right) {
  parse <- function(schema) jsonlite::fromJSON(schema, simplifyVector = FALSE)
  identical(schema_base(parse(left)), schema_base(parse(right)))
}

# Two schema sets, named lists of schemas keyed by resource type, are the same when they name the
# same types and the schemas of each type are the same.
same_schema_set <- function(left, right) {
  setequal(names(left), names(right)) &&
    length(left) == length(right) &&
    all(vapply(names(left), function(type) same_schema(left[[type]], right[[type]]), logical(1)))
}

is_annotation <- function(name) startsWith(name, "__")

# The base of a parsed type: nullable throughout, without metadata or annotation fields. A primitive
# type is a string, and the rest are lists with a type member.
schema_base <- function(type) {
  if (is.character(type)) {
    return(type)
  }
  switch(type$type,
    struct = {
      kept <- Filter(
        function(field) !is_annotation(field$name) && !only_annotations(field$type),
        type$fields
      )
      list(type = "struct", fields = lapply(kept, function(field) {
        list(name = field$name, type = schema_base(field$type))
      }))
    },
    array = list(type = "array", elementType = schema_base(type$elementType)),
    map = list(
      type = "map", keyType = schema_base(type$keyType), valueType = schema_base(type$valueType)
    ),
    type
  )
}

# Whether a parsed type is a group, or an array of groups, that holds only annotations.
only_annotations <- function(type) {
  if (is.character(type)) {
    return(FALSE)
  }
  if (type$type == "array") {
    return(only_annotations(type$elementType))
  }
  type$type == "struct" && length(type$fields) > 0 &&
    all(vapply(type$fields, function(field) is_annotation(field$name), logical(1)))
}
