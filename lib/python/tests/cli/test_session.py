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

"""Unit tests for the Spark session helpers that do not require Spark.

Author: John Grimes.
"""

from pathlib import Path

from pathling.cli.session import _write_quiet_log4j2


def test_quiet_log4j2_file_is_written():
    """The quiet log4j2 helper writes a readable properties file."""
    path = Path(_write_quiet_log4j2())

    assert path.exists()
    contents = path.read_text(encoding="utf-8")
    assert "rootLogger.level = off" in contents
    assert "SYSTEM_ERR" in contents
