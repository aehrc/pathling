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

from typing import Dict

from pyspark.sql.types import StructType


def write_schemas(schemas: Dict[str, StructType], path: str) -> None:
    """Write a schema set to a schema set file.

    :param schemas: the schema set, keyed by resource type code.
    :param path: the path of the file to write.
    """
    raise NotImplementedError("write_schemas is not implemented yet.")


def read_schemas(path: str) -> Dict[str, StructType]:
    """Read a schema set from a schema set file.

    :param path: the path of the file to read.
    :return: the schema set, keyed by resource type code.
    :raises ValueError: if the file is malformed, or a value in it is not a struct type.
    """
    raise NotImplementedError("read_schemas is not implemented yet.")
