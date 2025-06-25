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
from unittest.mock import Mock, patch
from pyspark.sql import Row, DataFrame
from pytest import fixture


@fixture(scope="module")
def ndjson_test_data_dir(test_data_dir):
    return os.path.join(test_data_dir, "ndjson")


ResultRow = Row("id", "family_name")


def test_view_on_ndjson(ndjson_test_data_dir, pathling_ctx):
    data_source = pathling_ctx.read.ndjson(ndjson_test_data_dir)
    result = data_source.view(
        resource='Patient',
        select=[
            {
                'column': [
                    {'path': 'id', 'name': 'id'},
                    {'path': "name.first().family", 'name': 'family_name'}
                ]
            }
        ]
    )
    assert result.columns == list(ResultRow)
    assert result.limit(2).collect() == [
        ResultRow('8ee183e2-b3c0-4151-be94-b945d6aa8c6d', 'Krajcik437'),
        ResultRow('beff242e-580b-47c0-9844-c1a68c36c5bf', 'Towne435'),
    ]
