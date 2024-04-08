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

from datetime import datetime, timezone, timedelta
from pathling.jvm import (
    datetime_to_instant,
    jvm,
    instant_to_datetime,
    timedelta_to_duration,
)


def test_datetime_to_instant(pathling_ctx):
    dt = datetime(2023, 1, 2, 3, 4, 5, 123_999, tzinfo=timezone.utc)
    inst = datetime_to_instant(dt)
    assert jvm().java.time.Instant.parse("2023-01-02T03:04:05.123Z") == inst


def test_instant_to_datetime(pathling_ctx):
    inst = jvm().java.time.Instant.parse("2023-01-02T03:04:05.123999Z")
    dt = datetime(2023, 1, 2, 3, 4, 5, 123_000, tzinfo=timezone.utc)
    assert dt == instant_to_datetime(inst)


def test_datetime_to_instant_round_trip(pathling_ctx):
    dt = datetime(2023, 1, 2, 3, 4, 5, 123_000, tzinfo=timezone.utc)
    inst = datetime_to_instant(dt)
    assert dt == instant_to_datetime(inst)


def test_timedelta_to_duration(pathling_ctx):
    td = timedelta(minutes=1, seconds=2, microseconds=3_999)
    dur = jvm().java.time.Duration.parse("PT62.003S")
    assert dur == timedelta_to_duration(td)
