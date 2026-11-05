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

"""Subprocess probe for the CLI verbose/quiet logging tests.

Starts a real local Spark session in a fresh JVM, applies the CLI log level
for the requested mode, and emits a single INFO record from a
Pathling-namespace logger. The tests run this as a subprocess so they can
assert on the process's real stderr: log4j2's console appender binds its own
handle to stderr when the session starts, so in-process file-descriptor
redirection (e.g. pytest's capfd) cannot capture its output.

Author: Rakesh Pai.
"""

from __future__ import annotations

import sys


def main(verbose: bool, marker: str) -> None:
    """Runs the probe: real local session, CLI log level, one INFO record.

    :param verbose: whether to apply the ``--verbose`` log level.
    :param marker: the marker string emitted in the INFO record.
    """
    from pyspark.sql import SparkSession

    from pathling.cli.session import _apply_log_level

    spark = (
        SparkSession.builder.appName("pathling-cli-logging-probe")
        .master("local[1]")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )
    try:
        # The exact code path --verbose (or the default) takes in
        # _create_pathling_context.
        _apply_log_level(spark, verbose)
        logger = spark._jvm.org.apache.logging.log4j.LogManager.getLogger(
            "au.csiro.pathling.cli.session.test"
        )
        logger.info(marker)
    finally:
        spark.stop()


if __name__ == "__main__":
    main(verbose=sys.argv[1] == "verbose", marker=sys.argv[2])
