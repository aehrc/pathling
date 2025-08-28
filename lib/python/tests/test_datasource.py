#  Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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

import os
from tempfile import TemporaryDirectory

from flask import Response
from pathling.datasource import DataSource
from pyspark.sql import DataFrame, Row
from pytest import fixture


@fixture(scope="function", autouse=True)
def func_temp_dir(temp_dir):
    """
    Fixture to create a temporary directory for each test function.
    :param temp_dir: 
    :return: existing temporary directory for each test function.
    """
    temp_ndjson_dir = TemporaryDirectory(dir=temp_dir, prefix="function")
    yield temp_ndjson_dir.name
    temp_ndjson_dir.cleanup()


@fixture(scope="module")
def ndjson_test_data_dir(test_data_dir):
    return os.path.join(test_data_dir, "ndjson")


@fixture(scope="module", autouse=True)
def temp_ndjson_dir(temp_dir):
    temp_ndjson_dir = TemporaryDirectory(dir=temp_dir, prefix="ndjson")
    yield temp_ndjson_dir.name
    temp_ndjson_dir.cleanup()


@fixture(scope="module")
def bundles_test_data_dir(test_data_dir):
    return os.path.join(test_data_dir, "bundles")


@fixture(scope="module")
def parquet_test_data_dir(test_data_dir):
    return os.path.join(test_data_dir, "parquet")


@fixture(scope="module", autouse=True)
def temp_parquet_dir(temp_dir):
    temp_parquet_dir = TemporaryDirectory(dir=temp_dir, prefix="parquet")
    yield temp_parquet_dir.name
    temp_parquet_dir.cleanup()


@fixture(scope="module")
def delta_test_data_dir(test_data_dir):
    return os.path.join(test_data_dir, "delta")


@fixture(scope="module", autouse=True)
def temp_delta_dir(temp_dir):
    temp_delta_dir = TemporaryDirectory(dir=temp_dir, prefix="delta")
    yield temp_delta_dir.name
    temp_delta_dir.cleanup()


@fixture(scope="function")
def bulk_server(mock_server, ndjson_test_data_dir):
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
                dict(type=resource, url=mock_server.url(f"/download/{resource}"), count=1) for
                resource in ["Patient", "Condition"]
            ],
        )

    @mock_server.route("/download/<resource>", methods=["GET"])
    def download(resource):
        with open(os.path.join(ndjson_test_data_dir, f"{resource}.ndjson"), "r") as f:
            return f.read()

    return mock_server


ResultRow = Row("count")


def test_datasource_read(ndjson_test_data_dir, pathling_ctx):
    patients = pathling_ctx.read.ndjson(ndjson_test_data_dir).read("Patient")
    assert patients.count() == 9


def test_datasource_ndjson(ndjson_test_data_dir, temp_ndjson_dir, pathling_ctx):
    pathling_ctx.read.ndjson(ndjson_test_data_dir).write.ndjson(temp_ndjson_dir)
    data_source = pathling_ctx.read.ndjson(temp_ndjson_dir)

    result = ndjson_query(data_source)
    assert result.columns == list(ResultRow)
    assert result.collect() == [
        ResultRow(71),
    ]


def test_datasource_ndjson_mapper(ndjson_test_data_dir, temp_ndjson_dir, pathling_ctx):
    pathling_ctx.read.ndjson(ndjson_test_data_dir).write.ndjson(
        temp_ndjson_dir, file_name_mapper=lambda x: f"Custom{x}"
    )
    data_source = pathling_ctx.read.ndjson(
        temp_ndjson_dir,
        file_name_mapper=lambda x: {x.replace("^Custom", "")},
    )

    result = ndjson_query(data_source)
    assert result.columns == list(ResultRow)
    assert result.collect() == [
        ResultRow(71),
    ]


def test_datasource_bundles(bundles_test_data_dir, pathling_ctx):
    data_source = pathling_ctx.read.bundles(
        bundles_test_data_dir, ["Patient", "Condition"]
    )

    result = bundles_query(data_source)
    assert result.columns == list(ResultRow)
    assert result.collect() == [
        ResultRow(10),
    ]


def test_datasource_datasets(ndjson_test_data_dir, pathling_ctx):
    data_source = pathling_ctx.read.datasets(
        resources={
            "Patient": pathling_ctx.encode(
                pathling_ctx.spark.read.text(
                    os.path.join(ndjson_test_data_dir, "Patient.ndjson")
                ),
                "Patient",
            ),
            "Condition": pathling_ctx.encode(
                pathling_ctx.spark.read.text(
                    os.path.join(ndjson_test_data_dir, "Condition.ndjson")
                ),
                "Condition",
            ),
        }
    )
    result = ndjson_query(data_source)
    assert result.columns == list(ResultRow)
    assert result.collect() == [
        ResultRow(71),
    ]


def test_datasource_parquet(parquet_test_data_dir, temp_parquet_dir, pathling_ctx):
    pathling_ctx.read.parquet(parquet_test_data_dir).write.parquet(temp_parquet_dir)
    data_source = pathling_ctx.read.parquet(temp_parquet_dir)

    result = parquet_query(data_source)
    assert result.columns == list(ResultRow)
    assert result.collect() == [
        ResultRow(71),
    ]


def test_datasource_delta(delta_test_data_dir, temp_delta_dir, pathling_ctx):
    pathling_ctx.read.delta(delta_test_data_dir).write.delta(temp_delta_dir)
    data_source = pathling_ctx.read.delta(temp_delta_dir)

    result = delta_query(data_source)
    assert result.columns == list(ResultRow)
    assert result.collect() == [
        ResultRow(71),
    ]


def test_datasource_delta_merge(delta_test_data_dir, temp_delta_dir, pathling_ctx):
    pathling_ctx.read.delta(delta_test_data_dir).write.delta(
        temp_delta_dir, save_mode="merge"
    )
    data_source = pathling_ctx.read.delta(temp_delta_dir)

    result = delta_query(data_source)
    assert result.columns == list(ResultRow)
    assert result.collect() == [
        ResultRow(71),
    ]


def test_datasource_tables(ndjson_test_data_dir, pathling_ctx):
    pathling_ctx.read.ndjson(ndjson_test_data_dir).write.tables()

    data_source = pathling_ctx.read.tables()
    result = ndjson_query(data_source)
    assert result.columns == list(ResultRow)
    assert result.collect() == [
        ResultRow(71),
    ]


def test_datasource_tables_schema(ndjson_test_data_dir, pathling_ctx):
    pathling_ctx.read.ndjson(ndjson_test_data_dir).write.tables(schema="test")

    data_source = pathling_ctx.read.tables(schema="test")
    result = ndjson_query(data_source)
    assert result.columns == list(ResultRow)
    assert result.collect() == [
        ResultRow(71),
    ]


def test_datasource_bulk_with_temp_dir(pathling_ctx, bulk_server):
    # !!! this directory cannot exist for the datasource to work
    with bulk_server.run():
        data_source = pathling_ctx.read.bulk(
            fhir_endpoint_url=bulk_server.url("/fhir")
        )
        result = ndjson_query(data_source)
        assert result.columns == list(ResultRow)
        assert result.collect() == [
            ResultRow(71),
        ]


def test_datasource_bulk_with_existing_dir(pathling_ctx, bulk_server, func_temp_dir):
    assert os.path.exists(func_temp_dir)
    with bulk_server.run():
        data_source = pathling_ctx.read.bulk(
            fhir_endpoint_url=bulk_server.url("/fhir"),
            output_dir=func_temp_dir,
            overwrite=True  # default anyway, but explicit for clarity
        )
        result = ndjson_query(data_source)
        assert result.columns == list(ResultRow)
        assert result.collect() == [
            ResultRow(71),
        ]


def ndjson_query(data_source: DataSource) -> DataFrame:
    return data_source.view(
        resource='Condition',
        select=[
            {
                'column': [
                    {'path': 'id', 'name': 'id'}
                ]
            }
        ]
    ).groupby().count()


def bundles_query(data_source: DataSource) -> DataFrame:
    return data_source.view(
        resource='Patient',
        select=[
            {
                'column': [
                    {'path': 'id', 'name': 'id'}
                ]
            }
        ]
    ).groupby().count()


def parquet_query(data_source: DataSource) -> DataFrame:
    return ndjson_query(data_source)


def delta_query(data_source: DataSource) -> DataFrame:
    return ndjson_query(data_source)


def test_datasource_resource_types(ndjson_test_data_dir, pathling_ctx):
    """Test that resource_types() returns a list of strings."""
    data_source = pathling_ctx.read.ndjson(ndjson_test_data_dir)
    
    # Call the resource_types method.
    resource_types = data_source.resource_types()
    
    # Verify it returns a list.
    assert isinstance(resource_types, list)
    
    # Verify the list contains strings.
    assert all(isinstance(rt, str) for rt in resource_types)
    
    # Verify expected resource types are present.
    assert "Patient" in resource_types
    assert "Condition" in resource_types
