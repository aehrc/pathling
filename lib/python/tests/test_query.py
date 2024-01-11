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
from typing import Sequence

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

    assert_result(
        [
            ExtractRow("2b36c1e2-bbe1-45ae-8124-4adad2677702", "male", "10509002"),
            ExtractRow("2b36c1e2-bbe1-45ae-8124-4adad2677702", "male", "38341003"),
            ExtractRow("2b36c1e2-bbe1-45ae-8124-4adad2677702", "male", "65363002"),
            ExtractRow("8ee183e2-b3c0-4151-be94-b945d6aa8c6d", "male", "195662009"),
            ExtractRow("8ee183e2-b3c0-4151-be94-b945d6aa8c6d", "male", "237602007"),
        ],
        result.orderBy("id", "gender", "condition_code").limit(5).collect(),
    )


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

    assert_result(
        [
            ExtractRow("121503c8-9564-4b48-9086-a22df717948e", "female", "10509002"),
            ExtractRow("121503c8-9564-4b48-9086-a22df717948e", "female", "15777000"),
            ExtractRow("121503c8-9564-4b48-9086-a22df717948e", "female", "195662009"),
            ExtractRow("121503c8-9564-4b48-9086-a22df717948e", "female", "271737000"),
            ExtractRow("121503c8-9564-4b48-9086-a22df717948e", "female", "363406005"),
        ],
        result.orderBy("id", "gender", "condition_code").limit(5).collect(),
    )


def test_aggregate(test_data_source):
    agg_result = test_data_source.aggregate(
        "Patient",
        aggregations=[fpe("count()").alias("patient_count")],
        groupings=[
            "gender",
            fpe("maritalStatus.coding.code").alias("marital_status_code"),
        ],
        filters=["birthDate > @1957-06-06"],
    )

    # noinspection PyPep8Naming
    AggregateRow = Row("gender", "marital_status_code", "patient_count")
    assert agg_result.columns == list(AggregateRow)

    assert_result(
        [
            AggregateRow("male", "M", 2),
            AggregateRow("male", "S", 1),
            AggregateRow("female", "M", 1),
            AggregateRow("female", "S", 3),
        ],
        agg_result.orderBy("gender", "marital_status_code", "patient_count").collect(),
    )


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

    assert_result(
        [
            AggregateRow("male", "M", 2),
            AggregateRow("male", "S", 3),
            AggregateRow("female", "M", 1),
            AggregateRow("female", "S", 3),
        ],
        agg_result.orderBy("gender", "marital_status_code", "patient_count").collect(),
    )


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
    assert_result([ResultRow(9, 9)], agg_result.collect())


def assert_result(expected: Sequence[Row], actual: Sequence[Row]):
    assert len(expected) == len(actual)
    assert set(expected).issubset(set(actual))
