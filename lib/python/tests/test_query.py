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

from pathling import Expression, Expression as fpe
from pathling.query import AggregateQuery


@fixture(scope="module")
def test_data_source(pathling_ctx, test_data_dir):
    return pathling_ctx.read.ndjson(os.path.join(test_data_dir, "ndjson"))


def test_extract(test_data_source):
    result = test_data_source.extract(
        "Patient",
        columns=[
            "id",
            "gender",
            Expression(
                "reverseResolve(Condition.subject).code.coding.code", "condition_code"
            ),
        ],
        filters=["gender = 'male'"],
    )

    # noinspection PyPep8Naming
    ExtractRow = Row("id", "gender", "condition_code")
    assert result.columns == list(ExtractRow)

    assert result.limit(5).collect() == [
        ExtractRow("beff242e-580b-47c0-9844-c1a68c36c5bf", "male", "444814009"),
        ExtractRow("beff242e-580b-47c0-9844-c1a68c36c5bf", "male", "444814009"),
        ExtractRow("beff242e-580b-47c0-9844-c1a68c36c5bf", "male", "444814009"),
        ExtractRow("a7eb2ce7-1075-426c-addd-957b861b0e55", "male", "367498001"),
        ExtractRow("a7eb2ce7-1075-426c-addd-957b861b0e55", "male", "162864005"),
    ]


def test_extract_no_filters(test_data_source):
    result = test_data_source.extract(
        "Patient",
        columns=[
            "id",
            "gender",
            fpe("reverseResolve(Condition.subject).code.coding.code").alias(
                "condition_code"
            ),
        ],
    )

    # noinspection PyPep8Naming
    ExtractRow = Row("id", "gender", "condition_code")
    assert result.columns == list(ExtractRow)

    assert result.limit(5).collect() == [
        ExtractRow("beff242e-580b-47c0-9844-c1a68c36c5bf", "male", "444814009"),
        ExtractRow("beff242e-580b-47c0-9844-c1a68c36c5bf", "male", "444814009"),
        ExtractRow("beff242e-580b-47c0-9844-c1a68c36c5bf", "male", "444814009"),
        ExtractRow("121503c8-9564-4b48-9086-a22df717948e", "female", "15777000"),
        ExtractRow("121503c8-9564-4b48-9086-a22df717948e", "female", "271737000"),
    ]


def test_aggregate(test_data_source):
    agg_result = test_data_source.aggregate(
        "Patient",
        aggregations=[fpe("count()").alias("patient_count")],
        groupings=["gender", "maritalStatus.coding.code"],
        filters=["birthDate > @1957-06-06"],
    )

    # noinspection PyPep8Naming
    AggregateRow = Row("gender", "maritalStatus.coding.code", "patient_count")
    assert agg_result.columns == list(AggregateRow)

    assert agg_result.collect() == [
        AggregateRow("male", "S", 1),
        AggregateRow("male", "M", 2),
        AggregateRow("female", "S", 3),
        AggregateRow("female", "M", 1),
    ]


def test_aggregate_no_filter(test_data_source):
    agg_result = test_data_source.aggregate(
        "Patient",
        aggregations=[fpe("count()").alias("patient_count")],
        groupings=[
            fpe("gender"),
            fpe("maritalStatus.coding.code").alias("marital_status_code"),
        ],
    )

    # noinspection PyPep8Naming
    AggregateRow = Row("gender", "marital_status_code", "patient_count")
    assert agg_result.columns == list(AggregateRow)

    assert agg_result.collect() == [
        AggregateRow("male", "S", 3),
        AggregateRow("male", "M", 2),
        AggregateRow("female", "S", 3),
        AggregateRow("female", "M", 1),
    ]


def test_many_aggregate_no_grouping(test_data_source):
    # noinspection PyPep8Naming
    ResultRow = Row("patient_count", "id.count()")

    agg_result = test_data_source.aggregate(
        "Patient",
        aggregations=[fpe("count()").alias("patient_count"), "id.count()"],
    )
    assert agg_result.columns == list(ResultRow)
    assert agg_result.collect() == [ResultRow(9, 9)]

    agg_result = AggregateQuery(
        "Patient",
        aggregations=[fpe("count()").alias("patient_count"), "id.count()"],
    ).execute(test_data_source)
    assert agg_result.columns == list(ResultRow)
    assert agg_result.collect() == [ResultRow(9, 9)]
