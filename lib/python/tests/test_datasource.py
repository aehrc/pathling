#  Copyright 2023 Commonwealth Scientific and Industrial Research
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

from pyspark.sql import Row, DataFrame
from pytest import fixture

from pathling import Expression as fpe
from pathling.datasource import DataSource


@fixture(scope="module")
def ndjson_test_data_dir(test_data_dir):
    return os.path.join(test_data_dir, "ndjson")


@fixture(scope="module")
def ndjson_custom_test_data_dir(test_data_dir):
    return os.path.join(test_data_dir, "ndjson")


@fixture(scope="module")
def bundles_test_data_dir(test_data_dir):
    return os.path.join(test_data_dir, "bundles")


@fixture(scope="module")
def parquet_test_data_dir(test_data_dir):
    return os.path.join(test_data_dir, "parquet")


@fixture(scope="module")
def delta_test_data_dir(test_data_dir):
    return os.path.join(test_data_dir, "delta")


ResultRow = Row("count")


def test_datasource_read(ndjson_test_data_dir, pathling_ctx):
    patients = pathling_ctx.read.ndjson(ndjson_test_data_dir).read("Patient")
    assert patients.count() == 9


def test_datasource_ndjson(ndjson_test_data_dir, pathling_ctx):
    data_source = pathling_ctx.read.ndjson(ndjson_test_data_dir)

    result = ndjson_query(data_source)
    assert result.columns == list(ResultRow)
    assert result.collect() == [
        ResultRow(71),
    ]


def test_datasource_ndjson_mapper(ndjson_custom_test_data_dir, pathling_ctx):
    data_source = pathling_ctx.read.ndjson(
        ndjson_custom_test_data_dir,
        filename_mapper=lambda x: {x.replace("^Custom", "")},
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


def test_datasource_parquet(parquet_test_data_dir, pathling_ctx):
    data_source = pathling_ctx.read.parquet(parquet_test_data_dir)

    result = parquet_query(data_source)
    assert result.columns == list(ResultRow)
    assert result.collect() == [
        ResultRow(71),
    ]


def test_datasource_delta(delta_test_data_dir, pathling_ctx):
    data_source = pathling_ctx.read.delta(delta_test_data_dir)

    result = delta_query(data_source)
    assert result.columns == list(ResultRow)
    assert result.collect() == [
        ResultRow(71),
    ]


def ndjson_query(data_source: DataSource) -> DataFrame:
    return data_source.aggregate(
        "Patient",
        aggregations=[
            fpe("reverseResolve(Condition.subject).count()").alias("count"),
        ],
    )


def bundles_query(data_source: DataSource) -> DataFrame:
    return data_source.aggregate(
        "Patient",
        aggregations=[
            fpe("count()").alias("count"),
        ],
    )


def parquet_query(data_source: DataSource) -> DataFrame:
    return ndjson_query(data_source)


def delta_query(data_source: DataSource) -> DataFrame:
    return ndjson_query(data_source)
