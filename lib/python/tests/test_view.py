#  Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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


@fixture(scope="module")
def ndjson_test_data_dir(test_data_dir):
    return os.path.join(test_data_dir, "ndjson")


ResultRow = Row("id", "family_name")


def test_view_on_ndjson(ndjson_test_data_dir, pathling_ctx):
    data_source = pathling_ctx.read.ndjson(ndjson_test_data_dir)
    result = data_source.view(
        resource="Patient",
        select=[
            {
                "column": [
                    {"path": "id", "name": "id"},
                    {"path": "name.first().family", "name": "family_name"},
                ]
            }
        ],
    )
    assert result.columns == list(ResultRow)
    assert result.limit(2).collect() == [
        ResultRow("8ee183e2-b3c0-4151-be94-b945d6aa8c6d", "Krajcik437"),
        ResultRow("beff242e-580b-47c0-9844-c1a68c36c5bf", "Towne435"),
    ]

def test_view_with_constants(ndjson_test_data_dir, pathling_ctx):
    """Reproduces #2574: view with constants fails to resolve constant references in where clause."""

    data_source = pathling_ctx.read.ndjson(ndjson_test_data_dir)
    result = data_source.view(
        resource="Patient",
        constant = [{"name": "filter_id", "valueUuid": "8ee183e2-b3c0-4151-be94-b945d6aa8c6d"}],
        select=[
            {
                "column": [
                    {"path": "id", "name": "id"},
                    {"value": "name.first().family", "name": "family_name"},
                ]
            }
        ],
        where=[{"path": "id = $filter_id"}],
    )
    assert result.collect() == [
        Row("8ee183e2-b3c0-4151-be94-b945d6aa8c6d", "Krajcik437"),
    ]

ConditionCodingRow = Row("id", "code_text", "system", "code", "display")


def test_foreach_coding_with_sibling_text_topandas(ndjson_test_data_dir, pathling_ctx):
    """Reproduces #2568: toPandas() corrupts data when combining code.text
    with forEach code.coding."""
    data_source = pathling_ctx.read.ndjson(ndjson_test_data_dir)
    result = data_source.view(
        resource="Condition",
        select=[
            {
                "column": [
                    {"path": "id", "name": "id"},
                    {"path": "code.text", "name": "code_text"},
                ],
                "select": [
                    {
                        "forEach": "code.coding",
                        "column": [
                            {"path": "system", "name": "system"},
                            {"path": "code", "name": "code"},
                            {"path": "display", "name": "display"},
                        ],
                    }
                ],
            }
        ],
    )

    # toPandas() exercises the physical collection path where the bug manifested.
    pdf = result.toPandas()
    assert list(pdf.columns) == list(ConditionCodingRow)

    # Verify the first few rows have correct column alignment.
    first_rows = result.limit(3).collect()
    assert first_rows == [
        ConditionCodingRow(
            "c879c300-7fdf-4b53-aa6a-a2b4a266b30c",
            "Viral sinusitis (disorder)",
            "http://snomed.info/sct",
            "444814009",
            "Viral sinusitis (disorder)",
        ),
        ConditionCodingRow(
            "bb9c4fc1-795a-4492-b065-1f497fe18bb2",
            "Diabetes",
            "http://snomed.info/sct",
            "44054006",
            "Diabetes",
        ),
        ConditionCodingRow(
            "e620d7ee-6cfe-4f04-ba06-1d0b39f7624d",
            "Anemia (disorder)",
            "http://snomed.info/sct",
            "271737000",
            "Anemia (disorder)",
        ),
    ]
