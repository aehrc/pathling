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

"""Tests the schema equality of spec SC-002 (T011)."""

from typing import List, Optional

from pytest import fixture
from pyspark.sql.types import (
    ArrayType,
    BooleanType,
    DataType,
    DecimalType,
    IntegerType,
    MapType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from tests.schema_equality import same_schema, same_schema_set


@fixture(autouse=True)
def _spark_session(pathling_ctx) -> None:
    """Start the Spark session that the helpers read the annotation prefix from."""


def _schema(
    nullable: bool = True,
    given_type: Optional[DataType] = None,
    name_fields: Optional[List[StructField]] = None,
    code_fields: Optional[List[StructField]] = None,
    top_fields: Optional[List[StructField]] = None,
) -> StructType:
    """Build the test schema, letting a test change one part of it."""
    metadata = {} if nullable else {"comment": "x"}
    name = StructType(
        name_fields
        or [
            StructField("family", StringType(), nullable, metadata),
            StructField(
                "given", given_type or ArrayType(StringType(), nullable), nullable
            ),
        ]
    )
    code = StructType(
        code_fields or [StructField("code", StringType(), nullable, metadata)]
    )
    fields = top_fields or [
        StructField("id", StringType(), nullable, metadata),
        StructField("name", ArrayType(name, nullable), nullable),
        StructField("tags", MapType(StringType(), code, nullable), nullable),
    ]
    return StructType(fields)


SCHEMA = _schema()


def test_ignores_nullability_and_metadata_at_every_depth() -> None:
    assert same_schema(SCHEMA, SCHEMA)
    assert same_schema(SCHEMA, _schema(nullable=False))
    assert same_schema(_schema(nullable=False), SCHEMA)


def test_ignores_annotation_fields() -> None:
    annotated = _schema(
        name_fields=[
            StructField("family", StringType()),
            StructField("given", ArrayType(StringType())),
            StructField("__flag", BooleanType()),
        ],
        code_fields=[
            StructField("code", StringType()),
            StructField("__canonical_code", StringType()),
        ],
    )
    annotated = StructType(
        [annotated["id"], StructField("__first", DecimalType(38, 6))]
        + annotated.fields[1:]
        + [StructField("__custom", StringType())]
    )
    assert same_schema(SCHEMA, annotated)
    assert same_schema(annotated, SCHEMA)


def test_drops_group_left_with_only_annotation_fields() -> None:
    grouped = _schema(
        name_fields=[
            StructField("family", StringType()),
            StructField("given", ArrayType(StringType())),
            StructField(
                "_given",
                ArrayType(StructType([StructField("__flag", BooleanType())])),
            ),
        ]
    )
    grouped = StructType(
        [
            grouped["id"],
            StructField(
                "_id",
                StructType(
                    [
                        StructField("__start", TimestampType()),
                        StructField("__end", TimestampType()),
                    ]
                ),
            ),
        ]
        + grouped.fields[1:]
    )
    with_base_field = StructType(
        [
            SCHEMA["id"],
            StructField(
                "_id",
                StructType(
                    [
                        StructField("id", StringType()),
                        StructField("__start", TimestampType()),
                    ]
                ),
            ),
        ]
        + SCHEMA.fields[1:]
    )
    assert same_schema(SCHEMA, grouped)
    assert same_schema(grouped, SCHEMA)
    assert not same_schema(SCHEMA, with_base_field)
    assert not same_schema(with_base_field, SCHEMA)


def test_drops_group_left_with_only_annotation_fields_at_any_depth() -> None:
    nested = StructType(
        [
            SCHEMA["id"],
            StructField(
                "_id",
                StructType(
                    [
                        StructField(
                            "_inner",
                            ArrayType(
                                StructType([StructField("__flag", BooleanType())])
                            ),
                        ),
                        StructField("__start", TimestampType()),
                    ]
                ),
            ),
        ]
        + SCHEMA.fields[1:]
    )
    nested_with_base_field = StructType(
        [
            SCHEMA["id"],
            StructField(
                "_id",
                StructType(
                    [
                        StructField(
                            "_inner",
                            StructType(
                                [
                                    StructField("id", StringType()),
                                    StructField("__flag", BooleanType()),
                                ]
                            ),
                        )
                    ]
                ),
            ),
        ]
        + SCHEMA.fields[1:]
    )
    assert same_schema(SCHEMA, nested)
    assert same_schema(nested, SCHEMA)
    assert not same_schema(SCHEMA, nested_with_base_field)
    assert not same_schema(nested_with_base_field, SCHEMA)


def test_differs_on_missing_or_extra_field() -> None:
    missing = _schema(name_fields=[StructField("family", StringType())])
    extra = _schema(
        code_fields=[
            StructField("code", StringType()),
            StructField("system", StringType()),
        ]
    )
    assert not same_schema(SCHEMA, missing)
    assert not same_schema(missing, SCHEMA)
    assert not same_schema(SCHEMA, extra)
    assert not same_schema(extra, SCHEMA)


def test_differs_on_renamed_or_retyped_field() -> None:
    renamed = _schema(
        name_fields=[
            StructField("family", StringType()),
            StructField("givenName", ArrayType(StringType())),
        ]
    )
    retyped = _schema(given_type=StringType())
    retyped_leaf = _schema(code_fields=[StructField("code", IntegerType())])
    assert not same_schema(SCHEMA, renamed)
    assert not same_schema(SCHEMA, retyped)
    assert not same_schema(SCHEMA, retyped_leaf)


def test_differs_on_field_order() -> None:
    top_level = StructType([SCHEMA["name"], SCHEMA["id"], SCHEMA["tags"]])
    nested = _schema(
        name_fields=[
            StructField("given", ArrayType(StringType())),
            StructField("family", StringType()),
        ]
    )
    assert not same_schema(SCHEMA, top_level)
    assert not same_schema(SCHEMA, nested)


def test_schema_sets_are_the_same_when_types_and_schemas_are() -> None:
    observation = StructType(
        [StructField("id", StringType()), StructField("status", StringType())]
    )
    assert same_schema_set(
        {"Patient": SCHEMA, "Observation": observation},
        {"Patient": _schema(nullable=False), "Observation": observation},
    )
    assert not same_schema_set(
        {"Patient": SCHEMA, "Observation": observation}, {"Patient": SCHEMA}
    )
    assert not same_schema_set({"Patient": SCHEMA}, {"Patient": observation})
    assert not same_schema_set({"Patient": SCHEMA}, {"Condition": SCHEMA})
