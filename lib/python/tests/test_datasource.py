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

from pyspark.sql import Row
from pytest import fixture

from pathling import Expression as fpe
from pathling.query import AggregateQuery


@fixture(scope="module")
def ndjson_test_data_dir(test_data_dir):
    return os.path.join(test_data_dir, "ndjson")


@fixture(scope="module")
def test_query():
    return AggregateQuery(
        "Patient",
        aggregations=[
            fpe("reverseResolve(Condition.subject).count()").alias("conditionCount"),
        ],
    )


ResultRow = Row("conditionCount")


def test_datasource_from_resources(test_query, ndjson_test_data_dir, pathling_ctx):
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
    result = test_query.execute(data_source)
    assert result.columns == list(ResultRow)
    assert result.collect() == [
        ResultRow(71),
    ]


def test_datasource_from_ndjson_dir(test_query, ndjson_test_data_dir, pathling_ctx):
    data_source = pathling_ctx.read.ndjson(ndjson_test_data_dir)

    result = test_query.execute(data_source)
    assert result.columns == list(ResultRow)
    assert result.collect() == [
        ResultRow(71),
    ]


def test_datasource_from_delta_warehouse(test_query, test_data_dir, pathling_ctx):
    data_source = pathling_ctx.read.delta("file://" + test_data_dir + "/delta")

    result = test_query.execute(data_source)
    assert result.columns == list(ResultRow)
    assert result.collect() == [
        ResultRow(71),
    ]
