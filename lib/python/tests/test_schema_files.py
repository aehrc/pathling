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

"""Tests the schema set file functions of the Python API (T061, FR-028)."""

import json
import re
import subprocess
import sys

from pyspark.sql.types import (
    ArrayType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)
from pytest import fixture, mark, raises

from pathling import read_schemas, write_schemas

NESTED_METADATA = {"comment": "the given name", "source": {"version": 2}}


@fixture
def schema_set():
    """A set out of canonical order, with non-nullable fields and empty and nested metadata."""
    patient = StructType(
        [
            StructField("id", StringType(), False, {}),
            StructField("resourceType", StringType(), True),
            StructField(
                "name",
                ArrayType(
                    StructType(
                        [
                            StructField(
                                "given",
                                ArrayType(StringType(), False),
                                True,
                                NESTED_METADATA,
                            ),
                            StructField("family", StringType(), False),
                        ]
                    ),
                    False,
                ),
            ),
            StructField("multipleBirthInteger", LongType()),
        ]
    )
    condition = StructType(
        [
            StructField("resourceType", StringType()),
            StructField("id", StringType()),
            StructField(
                "_recordedDate",
                StructType(
                    [
                        StructField("__start", TimestampType()),
                        StructField("__end", TimestampType()),
                    ]
                ),
            ),
        ]
    )
    return {"Patient": patient, "Condition": condition}


def java_to_json(pathling_ctx, schemas) -> str:
    """Return the content the Java SchemaSets.toJson gives for a schema set."""
    jvm = pathling_ctx.spark._jvm
    java_set = jvm.java.util.LinkedHashMap()
    for type_, schema in schemas.items():
        java_set.put(
            type_, jvm.org.apache.spark.sql.types.DataType.fromJson(schema.json())
        )
    return jvm.au.csiro.pathling.library.SchemaSets.toJson(java_set)


def test_written_and_read_back_equals_the_set(schema_set, tmp_path):
    """A set written and read back is equal to it, field order, nullability and metadata included."""
    path = str(tmp_path / "schemas.json")

    write_schemas(schema_set, path)

    assert read_schemas(path) == schema_set


def test_file_matches_the_java_content(pathling_ctx, schema_set, tmp_path):
    """The file written parses to the same JSON object as SchemaSets.toJson of the same set."""
    path = tmp_path / "schemas.json"

    write_schemas(schema_set, str(path))

    assert json.loads(path.read_text()) == json.loads(
        java_to_json(pathling_ctx, schema_set)
    )


def test_file_from_java_reads_to_the_same_set(pathling_ctx, schema_set, tmp_path):
    """A file written from SchemaSets.toJson is read to the same set, so files cross bindings."""
    path = tmp_path / "schemas.json"
    path.write_text(java_to_json(pathling_ctx, schema_set))

    assert read_schemas(str(path)) == schema_set


@mark.parametrize("content", ["", "not json", "[]", '{"Patient": '])
def test_malformed_file_is_refused_naming_the_file(content, tmp_path):
    """A malformed file is refused with an error naming the file."""
    path = tmp_path / "malformed.json"
    path.write_text(content)

    with raises(ValueError, match=re.escape(str(path))):
        read_schemas(str(path))


@mark.parametrize(
    "value",
    [
        '"string"',
        "1",
        '{"type": "array", "elementType": "string", "containsNull": true}',
    ],
)
def test_value_that_is_not_a_struct_is_refused_naming_its_key(value, tmp_path):
    """A value that is not a struct type is refused with an error naming its key."""
    path = tmp_path / "schemas.json"
    path.write_text('{"Condition": ' + value + "}")

    with raises(ValueError, match="Condition"):
        read_schemas(str(path))


@mark.parametrize(
    "schemas, key",
    [
        ({"Condition": "not a schema"}, "Condition"),
        ({"Condition": StringType()}, "Condition"),
        ({7: StructType([])}, "7"),
    ],
)
def test_writing_a_key_or_schema_of_the_wrong_type_is_refused_naming_the_key(
    schemas, key, tmp_path
):
    """A key that is not a string or a schema that is not a struct type is refused, naming the
    key, and no file is written."""
    path = tmp_path / "schemas.json"

    with raises(TypeError, match=key):
        write_schemas(schemas, str(path))
    assert not path.exists()


def test_repeated_resource_type_is_refused_naming_it(tmp_path):
    """A file that names a resource type twice is refused with an error naming the type."""
    struct = '{"type": "struct", "fields": []}'
    path = tmp_path / "schemas.json"
    path.write_text('{"Patient": ' + struct + ', "Patient": ' + struct + "}")

    with raises(ValueError, match="Patient"):
        read_schemas(str(path))


def test_empty_object_gives_an_empty_set(tmp_path):
    """A file holding an empty object is read to an empty set."""
    path = tmp_path / "schemas.json"
    path.write_text("{}")

    assert read_schemas(str(path)) == {}


def test_both_run_without_a_spark_session(tmp_path):
    """Writing and reading a schema set file needs no Spark session, and starts none."""
    path = str(tmp_path / "schemas.json")
    code = f"""
from pyspark import SparkContext
from pyspark.sql.types import StringType, StructField, StructType
from pathling import read_schemas, write_schemas

schemas = {{"Patient": StructType([StructField("id", StringType())])}}
write_schemas(schemas, {path!r})
assert read_schemas({path!r}) == schemas
assert SparkContext._active_spark_context is None
assert SparkContext._jvm is None
print("ok")
"""
    result = subprocess.run(
        [sys.executable, "-c", code], capture_output=True, text=True
    )

    assert result.returncode == 0, result.stderr
    assert "ok" in result.stdout
