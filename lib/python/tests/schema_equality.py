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
Annotation fields are left out, since they are compared against the configuration.
"""

from typing import Mapping

from pyspark.sql.types import StructType


def same_schema(left: StructType, right: StructType) -> bool:
    """Return whether two schemas are the same."""
    raise NotImplementedError("Schema equality is not yet implemented.")


def same_schema_set(
    left: Mapping[str, StructType], right: Mapping[str, StructType]
) -> bool:
    """Return whether two schema sets name the same types, with the same schema for each."""
    raise NotImplementedError("Schema equality is not yet implemented.")
