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
group that holds only annotation fields.
"""

from typing import Mapping

from pyspark.sql.types import ArrayType, DataType, MapType, StructField, StructType

ANNOTATION_PREFIX = "__"


def same_schema(left: StructType, right: StructType) -> bool:
    """Return whether two schemas are the same."""
    return _base(left) == _base(right)


def same_schema_set(
    left: Mapping[str, StructType], right: Mapping[str, StructType]
) -> bool:
    """Return whether two schema sets name the same types, with the same schema for each."""
    return left.keys() == right.keys() and all(
        same_schema(left[type_], right[type_]) for type_ in left
    )


def _base(type_: DataType) -> DataType:
    """Return the base of a type: nullable throughout, without metadata or annotation fields."""
    if isinstance(type_, StructType):
        return StructType(
            [
                StructField(field.name, _base(field.dataType), True)
                for field in type_.fields
                if not field.name.startswith(ANNOTATION_PREFIX)
                and not _only_annotations(field.dataType)
            ]
        )
    if isinstance(type_, ArrayType):
        return ArrayType(_base(type_.elementType), True)
    if isinstance(type_, MapType):
        return MapType(_base(type_.keyType), _base(type_.valueType), True)
    return type_


def _only_annotations(type_: DataType) -> bool:
    """Return whether a type is a group, or an array of groups, that holds only annotations."""
    if isinstance(type_, ArrayType):
        return _only_annotations(type_.elementType)
    return (
        isinstance(type_, StructType)
        and len(type_.fields) > 0
        and all(name.startswith(ANNOTATION_PREFIX) for name in type_.names)
    )
