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

"""Schema set files: a schema set saved to, and read back from, a JSON file (FR-028).

The file holds one JSON object mapping each resource type code to the JSON form of its struct
type, as Spark writes it. Both functions are pure Python and need no Spark session.
"""

import json
from typing import Any, Dict, List, Tuple

from pyspark.sql.types import StructType


def write_schemas(schemas: Dict[str, StructType], path: str) -> None:
    """Write a schema set to a schema set file.

    :param schemas: the schema set, keyed by resource type code.
    :param path: the path of the file to write.
    """
    content = {
        resource_type: schema.jsonValue() for resource_type, schema in schemas.items()
    }
    with open(path, "w", encoding="utf-8") as file:
        json.dump(content, file)


def read_schemas(path: str) -> Dict[str, StructType]:
    """Read a schema set from a schema set file.

    :param path: the path of the file to read.
    :return: the schema set, keyed by resource type code, in the order of the file.
    :raises ValueError: if the file is malformed, if it names a resource type more than once, or if
        a value in it is not a struct type.
    """
    # The hook is called as each object closes, so the pairs of the outermost object come last.
    # Only its keys are resource types, which must not repeat.
    objects: List[List[Tuple[str, Any]]] = []

    def build_object(pairs: List[Tuple[str, Any]]) -> Dict[str, Any]:
        objects.append(pairs)
        return dict(pairs)

    with open(path, encoding="utf-8") as file:
        try:
            content = json.load(file, object_pairs_hook=build_object)
        except json.JSONDecodeError as error:
            raise ValueError(
                f"The schema set file {path} is not valid JSON: {error}"
            ) from error
    if not isinstance(content, dict):
        raise ValueError(f"The schema set file {path} does not hold a JSON object")
    _refuse_repeated_resource_types(objects[-1])
    return {
        resource_type: _struct_type(resource_type, value)
        for resource_type, value in content.items()
    }


def _refuse_repeated_resource_types(pairs: List[Tuple[str, Any]]) -> None:
    """Refuse a schema set that names a resource type more than once."""
    seen = set()
    for resource_type, _ in pairs:
        if resource_type in seen:
            raise ValueError(f"The schema set names {resource_type} more than once")
        seen.add(resource_type)


def _struct_type(resource_type: str, value: Any) -> StructType:
    """Parse the JSON form of the schema of one resource type, which must be a struct type."""
    if not isinstance(value, dict) or value.get("type") != "struct":
        raise ValueError(f"The schema of {resource_type} is not a struct type")
    try:
        return StructType.fromJson(value)
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError(
            f"The schema of {resource_type} is malformed: {error}"
        ) from error
