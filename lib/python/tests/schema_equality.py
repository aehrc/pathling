#
# Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
# Organisation (CSIRO) ABN 41 687 119 230.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""The schema equality of spec SC-002, used by the tests of a schema that is the same.

Two schemas are the same when their base schemas are equal field for field, in order at every
depth, ignoring nullability (of fields, array elements and map values) and field metadata.
Annotation fields are left out, since they are compared against the configuration, and so is a
group left with only annotation fields, at any depth. The prefix that marks an annotation is read
from Pathling's `AnnotationNames`, so a Spark session must be running.
"""

from typing import List, Mapping

from pyspark import SparkContext
from pyspark.sql.types import ArrayType, DataType, MapType, StructField, StructType


def same_schema(left: StructType, right: StructType) -> bool:
    """Return whether two schemas are the same."""
    prefix = _annotation_prefix()
    return _base(left, prefix) == _base(right, prefix)


def same_schema_set(
    left: Mapping[str, StructType], right: Mapping[str, StructType]
) -> bool:
    """Return whether two schema sets name the same types, with the same schema for each."""
    return left.keys() == right.keys() and all(
        same_schema(left[type_], right[type_]) for type_ in left
    )


def _annotation_prefix() -> str:
    """Return the prefix that marks an annotation, from the running Spark session's JVM."""
    jvm = SparkContext._jvm
    if jvm is None:
        raise RuntimeError("Schema equality needs a running Spark session.")
    return jvm.au.csiro.pathling.utilities.AnnotationNames.PREFIX


def _base(type_: DataType, prefix: str) -> DataType:
    """Return the base of a type.

    The base is nullable throughout, without metadata, annotation fields or groups left with only
    annotation fields at any depth.
    """
    if isinstance(type_, StructType):
        fields: List[StructField] = []
        for field in type_.fields:
            if field.name.startswith(prefix):
                continue
            base = _base(field.dataType, prefix)
            if not _emptied(field.dataType, base):
                fields.append(StructField(field.name, base, True))
        return StructType(fields)
    if isinstance(type_, ArrayType):
        return ArrayType(_base(type_.elementType, prefix), True)
    if isinstance(type_, MapType):
        return MapType(
            _base(type_.keyType, prefix), _base(type_.valueType, prefix), True
        )
    return type_


def _emptied(original: DataType, base: DataType) -> bool:
    """Return whether a group, or an array of groups, that held fields has none left in its base."""
    if isinstance(original, ArrayType) and isinstance(base, ArrayType):
        return _emptied(original.elementType, base.elementType)
    return (
        isinstance(original, StructType)
        and len(original.fields) > 0
        and isinstance(base, StructType)
        and len(base.fields) == 0
    )
