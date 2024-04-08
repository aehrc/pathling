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

from datetime import datetime, timezone
from pathling.fhir import as_fhir_instant, from_fhir_instant


def test_utc_fhir_instant_to_datetime():
    assert "2023-01-02T03:04:05.123Z" == as_fhir_instant(
        datetime(2023, 1, 2, 3, 4, 5, 123_000, tzinfo=timezone.utc)
    )


def test_utc_datetime_to_fhir_instant():
    assert datetime(
        2023, 1, 2, 3, 4, 5, 123_000, tzinfo=timezone.utc
    ) == from_fhir_instant("2023-01-02T03:04:05.123Z")
