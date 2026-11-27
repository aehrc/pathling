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

"""Tests the sources of the Python API read with a schema set (T021, FR-001, FR-004).

Also tests the schemas a source reports and their canonicalisation (T038, T066, FR-019, FR-027).
"""

import os
from typing import Dict, List
from uuid import uuid4

from flask import Response
from pyspark.sql.types import ArrayType, StructField, StructType
from pytest import fixture, mark, raises

from pathling import read_schemas, write_schemas
from pathling.datasource import DataSource
from tests.schema_equality import _annotation_prefix, same_schema_set

# Eleven resource types of the bundles test data, one more than the ten that an earlier binding
# could pass.
ELEVEN_TYPES = [
    "CarePlan",
    "CareTeam",
    "Claim",
    "Condition",
    "DiagnosticReport",
    "Encounter",
    "Immunization",
    "MedicationRequest",
    "Observation",
    "Patient",
    "Procedure",
]


@fixture(scope="module")
def ndjson_dir(test_data_dir):
    return os.path.join(test_data_dir, "ndjson")


@fixture(scope="module")
def bundles_dir(test_data_dir):
    return os.path.join(test_data_dir, "bundles")


@fixture(scope="module")
def inferred(pathling_ctx, ndjson_dir) -> DataSource:
    """The NDJSON test data read without a schema set."""
    return pathling_ctx.read.ndjson(ndjson_dir)


@fixture(scope="module")
def defined(inferred) -> Dict[str, StructType]:
    """A schema set for Patient and Condition narrower than the data and in another order."""
    return {
        type_: narrow(inferred.read(type_).schema) for type_ in ["Patient", "Condition"]
    }


@fixture(scope="module")
def tables_database(pathling_ctx, inferred) -> str:
    """A database of its own holding the NDJSON test data as tables."""
    database = "defined_" + uuid4().hex
    pathling_ctx.spark.sql(f"CREATE DATABASE {database}")
    inferred.write.tables(schema=database)
    return database


@fixture(scope="function")
def bulk_server(mock_server, ndjson_dir):
    """A bulk export server serving the Patient and Condition NDJSON test data."""

    @mock_server.route("/fhir/$export", methods=["GET"])
    def export():
        resp = Response(status=202)
        resp.headers["content-location"] = mock_server.url("/pool")
        return resp

    @mock_server.route("/pool", methods=["GET"])
    def pool():
        return dict(
            transactionTime="1970-01-01T00:00:00.000Z",
            output=[
                dict(
                    type=resource, url=mock_server.url(f"/download/{resource}"), count=1
                )
                for resource in ["Patient", "Condition"]
            ],
        )

    @mock_server.route("/download/<resource>", methods=["GET"])
    def download(resource):
        with open(os.path.join(ndjson_dir, f"{resource}.ndjson"), "r") as f:
            return f.read()

    return mock_server


def narrow(schema: StructType) -> StructType:
    """Return a schema of the first three base fields of a schema, in reverse order.

    A group, or an array of groups, among them keeps only its first base field, so that the
    projection is exercised below the top level too.
    """
    prefix = _annotation_prefix()

    def base_fields(struct: StructType) -> List[StructField]:
        return [field for field in struct.fields if not field.name.startswith(prefix)]

    def first_only(field: StructField) -> StructField:
        type_ = field.dataType
        if isinstance(type_, StructType):
            return StructField(field.name, StructType(base_fields(type_)[:1]))
        if isinstance(type_, ArrayType) and isinstance(type_.elementType, StructType):
            element = StructType(base_fields(type_.elementType)[:1])
            return StructField(field.name, ArrayType(element))
        return field

    return StructType(
        [first_only(field) for field in reversed(base_fields(schema)[:3])]
    )


def schemas_of(data_source: DataSource, types) -> Dict[str, StructType]:
    """Return the schema of each of the given types, as the data source reads it."""
    return {type_: data_source.read(type_).schema for type_ in types}


def primitive_rows(data_source: DataSource, schemas: Dict[str, StructType]):
    """Return the sorted rows of each type's top-level primitive columns in a schema set."""

    def rows(type_: str, schema: StructType):
        columns = [
            field.name
            for field in schema.fields
            if not isinstance(field.dataType, (StructType, ArrayType))
        ]
        return sorted(data_source.read(type_).select(columns).collect(), key=repr)

    return {type_: rows(type_, schema) for type_, schema in schemas.items()}


def open_source(kind: str, pathling_ctx, inferred, test_data_dir, database, schemas):
    """Open a source of the given kind over the Patient and Condition test data."""
    read = pathling_ctx.read
    if kind == "ndjson":
        return read.ndjson(os.path.join(test_data_dir, "ndjson"), schemas=schemas)
    if kind == "bundles":
        return read.bundles(
            os.path.join(test_data_dir, "bundles"),
            ["Patient", "Condition"],
            schemas=schemas,
        )
    if kind == "datasets":
        return read.datasets(
            {type_: inferred.read(type_) for type_ in ["Patient", "Condition"]},
            schemas=schemas,
        )
    if kind == "parquet":
        return read.parquet(os.path.join(test_data_dir, "parquet"), schemas=schemas)
    if kind == "delta":
        return read.delta(os.path.join(test_data_dir, "delta"), schemas=schemas)
    if kind == "tables":
        return read.tables(schema=database(), schemas=schemas)
    raise ValueError(f"Unknown source kind: {kind}")


SOURCES = ["ndjson", "bundles", "datasets", "parquet", "delta", "tables"]


@mark.parametrize("kind", SOURCES)
def test_source_reads_with_the_given_schemas(
    kind, pathling_ctx, inferred, defined, test_data_dir, request
):
    """Every source reads each type with the schema the set gives it (FR-001, FR-004).

    The schemas are narrower than the data and in another field order, so the output has the
    given schemas only if the source projects to them. The projection keeps every row and its
    values, as the same source read without a schema set has them.
    """

    def open_with(schemas):
        return open_source(
            kind,
            pathling_ctx,
            inferred,
            test_data_dir,
            lambda: request.getfixturevalue("tables_database"),
            schemas,
        )

    data_source = open_with(defined)

    assert same_schema_set(schemas_of(data_source, defined), defined)
    expected = primitive_rows(open_with(None), defined)
    assert all(expected.values())
    assert primitive_rows(data_source, defined) == expected


def test_bulk_reads_with_the_given_schemas(
    pathling_ctx, defined, bulk_server, ndjson_dir
):
    """The bulk source reads each type with the schema the set gives it (FR-001, FR-004)."""
    with bulk_server.run():
        data_source = pathling_ctx.read.bulk(
            fhir_endpoint_url=bulk_server.url("/fhir"), schemas=defined
        )
        assert same_schema_set(schemas_of(data_source, defined), defined)
        expected = primitive_rows(pathling_ctx.read.ndjson(ndjson_dir), defined)
        assert all(expected.values())
        assert primitive_rows(data_source, defined) == expected


def test_bulk_refuses_a_schema_set_before_it_exports(
    pathling_ctx, defined, mock_server
):
    """The bulk source refuses a schema set before it starts the export (FR-004)."""
    exports = []

    @mock_server.route("/fhir/$export", methods=["GET"])
    def export():
        exports.append(1)
        return Response(status=500)

    schemas = {
        "Patient": defined["Patient"],
        "Condition": '{"type":"struct","fields":[]}',
    }

    with mock_server.run():
        with raises(TypeError, match="Condition"):
            pathling_ctx.read.bulk(
                fhir_endpoint_url=mock_server.url("/fhir"), schemas=schemas
            )

    assert exports == []


def test_more_than_ten_types_are_accepted(pathling_ctx, bundles_dir):
    """A set of more than ten types is accepted, with no limit on its size (FR-004)."""
    bundles = pathling_ctx.read.bundles(bundles_dir, ELEVEN_TYPES)
    schemas = {
        type_: narrow(schema)
        for type_, schema in schemas_of(bundles, ELEVEN_TYPES).items()
    }

    data_source = pathling_ctx.read.bundles(bundles_dir, ELEVEN_TYPES, schemas=schemas)

    assert same_schema_set(schemas_of(data_source, ELEVEN_TYPES), schemas)
    expected = primitive_rows(bundles, schemas)
    assert all(expected.values())
    assert primitive_rows(data_source, schemas) == expected


@mark.parametrize("kind", SOURCES)
@mark.parametrize(
    "value",
    [
        {"type": "struct", "fields": []},
        '{"type":"struct","fields":[]}',
    ],
    ids=["dict", "json-string"],
)
def test_value_that_is_not_a_struct_type_is_refused(
    kind, value, pathling_ctx, inferred, defined, test_data_dir
):
    """A schema that is not a StructType is refused with an error naming its key (FR-004)."""
    schemas = {"Patient": defined["Patient"], "Condition": value}

    with raises((TypeError, ValueError), match="Condition"):
        open_source(
            kind,
            pathling_ctx,
            inferred,
            test_data_dir,
            lambda: "default",
            schemas,
        )


def reversed_at_every_depth(type_):
    """Return a type with the fields of every struct within it in reverse order."""
    if isinstance(type_, StructType):
        return StructType(
            [
                StructField(
                    field.name,
                    reversed_at_every_depth(field.dataType),
                    field.nullable,
                    field.metadata,
                )
                for field in reversed(type_.fields)
            ]
        )
    if isinstance(type_, ArrayType):
        return ArrayType(reversed_at_every_depth(type_.elementType), type_.containsNull)
    return type_


def test_canonical_schemas_gives_the_order_of_an_inferred_load(pathling_ctx, inferred):
    """A set out of order canonicalises to the order an inferred load gives (FR-027, SC-009)."""
    canonical = schemas_of(inferred, ["Patient", "Condition"])
    out_of_order = {
        type_: reversed_at_every_depth(schema) for type_, schema in canonical.items()
    }
    assert out_of_order != canonical

    assert pathling_ctx.canonical_schemas(out_of_order) == canonical


ROUND_TRIP_TYPES = ["Patient", "Condition"]


@mark.parametrize("kind", SOURCES + ["bulk"])
def test_schemas_round_trip_through_a_file_into_every_source(
    kind, pathling_ctx, inferred, test_data_dir, bulk_server, tmp_path, request
):
    """The schemas of every source, saved to a file and read back, are accepted by every source.

    Each source then reports the same set by schema equality (FR-019, SC-007).
    """

    def open_any(source_kind, schemas):
        if source_kind == "bulk":
            return pathling_ctx.read.bulk(
                fhir_endpoint_url=bulk_server.url("/fhir"), schemas=schemas
            )
        return open_source(
            source_kind,
            pathling_ctx,
            inferred,
            test_data_dir,
            lambda: request.getfixturevalue("tables_database"),
            schemas,
        )

    def held(data_source):
        schemas = data_source.schemas()
        assert set(ROUND_TRIP_TYPES) <= set(schemas)
        return {type_: schemas[type_] for type_ in ROUND_TRIP_TYPES}

    path = str(tmp_path / "schemas.json")
    with bulk_server.run():
        write_schemas(held(open_any(kind, None)), path)
        read_back = read_schemas(path)

        for target in SOURCES + ["bulk"]:
            reported = held(open_any(target, read_back))
            assert same_schema_set(reported, read_back), f"{kind} into {target}"
