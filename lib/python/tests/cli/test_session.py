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

The session-build tests mock :func:`pathling.context._build_spark_session` so
that the configuration assembled by ``_build_quiet_spark`` can be captured
without starting Spark.

Author: John Grimes.
"""

from pathlib import Path

import pathling.context as context_module
from pathling.cli.config import CliConfig
from pathling.cli.session import _build_quiet_spark, _write_quiet_log4j2


def _capture_build(monkeypatch) -> dict:
    """Replaces the session builder with a stub that captures its configuration.

    :param monkeypatch: the pytest monkeypatch fixture.
    :return: a dict populated with the ``extra_configs`` passed to the builder.
    """
    captured = {}

    def fake_build(extra_configs=None):
        captured.update(extra_configs or {})
        return "session"

    monkeypatch.setattr(context_module, "_build_spark_session", fake_build)
    return captured


def test_quiet_log4j2_file_is_written():
    """The quiet log4j2 helper writes a readable properties file."""
    path = Path(_write_quiet_log4j2())

    assert path.exists()
    contents = path.read_text(encoding="utf-8")
    assert "rootLogger.level = off" in contents
    assert "SYSTEM_ERR" in contents


def test_build_quiet_spark_passes_spark_conf(monkeypatch):
    """User Spark settings reach the session builder."""
    captured = _capture_build(monkeypatch)
    config = CliConfig(verbose=True, spark_conf={"spark.sql.shuffle.partitions": "16"})

    result = _build_quiet_spark(config)

    assert result == "session"
    assert captured["spark.sql.shuffle.partitions"] == "16"


def test_build_quiet_spark_user_wins_over_quiet_java_options(monkeypatch):
    """A user-set spark.driver.extraJavaOptions overrides the quiet default."""
    captured = _capture_build(monkeypatch)
    config = CliConfig(
        verbose=False,
        spark_conf={"spark.driver.extraJavaOptions": "-Dcustom=1"},
    )

    _build_quiet_spark(config)

    # The user value wins over the CLI's quiet-logging option for the same key.
    assert captured["spark.driver.extraJavaOptions"] == "-Dcustom=1"
    # The quiet console-progress option the user did not set still applies.
    assert captured["spark.ui.showConsoleProgress"] == "false"


def test_build_quiet_spark_empty_conf_keeps_quiet_defaults(monkeypatch):
    """An empty spark_conf leaves the quiet-mode behaviour unchanged."""
    captured = _capture_build(monkeypatch)
    config = CliConfig(verbose=False, spark_conf={})

    _build_quiet_spark(config)

    assert captured["spark.ui.showConsoleProgress"] == "false"
    assert "log4j2.configurationFile" in captured["spark.driver.extraJavaOptions"]
    # Only the two quiet-logging options are present.
    assert set(captured) == {
        "spark.driver.extraJavaOptions",
        "spark.ui.showConsoleProgress",
    }
