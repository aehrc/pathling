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

import glob
import os
import tempfile
from pathlib import Path

import pathling
import pathling.context as context_module
from pathling.cli.config import CliConfig, TxAuth, TxStore
from pathling.cli.session import (
    _build_quiet_spark,
    _create_pathling_context,
    public_namespace,
    quiet_log4j2_path,
)

# The temp-file prefix the pre-fix implementation used; no file with this prefix
# may be created any more (FR-017).
_LEAKED_PREFIX = "pathling-cli-log4j2-*"


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


def test_quiet_log4j2_resolves_packaged_resource_without_temp_file():
    """The quiet log4j2 path resolves to the packaged resource and leaves no
    per-run temporary file behind (FR-017)."""
    pattern = os.path.join(tempfile.gettempdir(), _LEAKED_PREFIX)
    before = set(glob.glob(pattern))

    path = quiet_log4j2_path()

    after = set(glob.glob(pattern))
    assert after == before, "a per-run temporary log4j2 file was created"
    resolved = Path(path)
    assert resolved.exists()
    assert os.access(resolved, os.R_OK)
    contents = resolved.read_text(encoding="utf-8")
    assert "rootLogger.level = off" in contents
    assert "SYSTEM_ERR" in contents


def test_build_quiet_spark_points_driver_at_readable_quiet_config(monkeypatch):
    """The driver Java options point at a readable quiet log4j2 configuration."""
    captured = _capture_build(monkeypatch)
    config = CliConfig(verbose=False, spark_conf={})

    _build_quiet_spark(config)

    java_options = captured["spark.driver.extraJavaOptions"]
    prefix = "-Dlog4j2.configurationFile=file:"
    assert prefix in java_options
    path = Path(java_options.split(prefix, 1)[1])
    assert path.exists()
    assert "rootLogger.level = off" in path.read_text(encoding="utf-8")


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
    # Only the two quiet-logging options and the Arrow transfer option are
    # present.
    assert set(captured) == {
        "spark.driver.extraJavaOptions",
        "spark.ui.showConsoleProgress",
        "spark.sql.execution.arrow.pyspark.enabled",
    }


def test_build_quiet_spark_enables_arrow_transfer(monkeypatch):
    """Arrow-based columnar transfer is enabled on the CLI session."""
    captured = _capture_build(monkeypatch)
    config = CliConfig(verbose=True, spark_conf={})

    _build_quiet_spark(config)

    assert captured["spark.sql.execution.arrow.pyspark.enabled"] == "true"


def test_build_quiet_spark_user_can_disable_arrow(monkeypatch):
    """A user --spark-conf value overrides the Arrow transfer default."""
    captured = _capture_build(monkeypatch)
    config = CliConfig(
        verbose=True,
        spark_conf={"spark.sql.execution.arrow.pyspark.enabled": "false"},
    )

    _build_quiet_spark(config)

    assert captured["spark.sql.execution.arrow.pyspark.enabled"] == "false"


# ========== Local-mode session parameter threading (US1, T004) ==========


class _FakeSparkContext:
    """A stand-in Spark context that records the requested log level."""

    def setLogLevel(self, level):  # noqa: N802 - mirrors the Spark method name.
        self.level = level


class _FakeSpark:
    """A stand-in Spark session exposing a sparkContext."""

    def __init__(self):
        self.sparkContext = _FakeSparkContext()


def _capture_create(monkeypatch) -> dict:
    """Replaces the session builder and PathlingContext.create with capturers.

    :param monkeypatch: the pytest monkeypatch fixture.
    :return: a dict populated with the keyword arguments passed to
             ``PathlingContext.create``.
    """
    import pathling

    monkeypatch.setattr(
        "pathling.cli.session._build_quiet_spark", lambda config: _FakeSpark()
    )
    captured = {}

    def fake_create(spark, **kwargs):
        captured.update(kwargs)
        captured["_spark"] = spark
        return "context"

    monkeypatch.setattr(pathling.PathlingContext, "create", staticmethod(fake_create))
    return captured


def test_local_mode_threads_store_parameters(monkeypatch):
    """A configured store threads local-mode parameters into the context.

    The store path, edition, and cache size reach ``PathlingContext.create`` in
    local terminology mode, and no authentication parameters are enabled.
    """
    captured = _capture_create(monkeypatch)
    config = CliConfig(
        verbose=True,
        tx_store=TxStore(
            path="/data/tx-store",
            default_snomed_edition="32506021000036107",
            expansion_cache_size=200,
        ),
    )

    _create_pathling_context(config)

    assert captured["terminology_mode"] == "local"
    assert captured["terminology_storage_path"] == "/data/tx-store"
    assert captured["default_snomed_edition"] == "32506021000036107"
    assert captured["expansion_cache_size"] == 200
    # Authentication is not enabled in local mode.
    assert captured["enable_auth"] is False


def test_local_mode_omits_cache_size_when_unset(monkeypatch):
    """An unset expansion-cache-size is not passed, leaving the library default."""
    captured = _capture_create(monkeypatch)
    config = CliConfig(verbose=True, tx_store=TxStore(path="/data/tx-store"))

    _create_pathling_context(config)

    assert captured["terminology_mode"] == "local"
    # The library default (100) must apply, so the key is not forwarded at all.
    assert "expansion_cache_size" not in captured
    assert "default_snomed_edition" not in captured


def test_local_mode_ignores_configured_auth(monkeypatch):
    """A store wins over configured auth: no auth reaches the local session."""
    captured = _capture_create(monkeypatch)
    config = CliConfig(
        verbose=True,
        tx_store=TxStore(path="/data/tx-store"),
        tx_auth=TxAuth(
            client_id="c", token_endpoint="https://auth/token", client_secret="s"
        ),
    )

    _create_pathling_context(config)

    assert captured["enable_auth"] is False
    assert "terminology_server_url" not in captured


def test_remote_mode_passes_existing_parameters_unchanged(monkeypatch):
    """With no store, the existing remote parameters are passed unchanged."""
    captured = _capture_create(monkeypatch)
    config = CliConfig(
        verbose=True,
        tx_server="https://tx.example/fhir",
        tx_auth=TxAuth(
            client_id="c", token_endpoint="https://auth/token", client_secret="s"
        ),
    )

    _create_pathling_context(config)

    assert captured["terminology_server_url"] == "https://tx.example/fhir"
    assert captured["enable_auth"] is True
    assert captured["client_id"] == "c"
    # Local-mode parameters are absent in remote mode.
    assert "terminology_mode" not in captured or captured["terminology_mode"] != "local"


# ========== Public namespace helper ==========


def test_public_namespace_keys_equal_all():
    """The helper's keys are exactly the package's public API list (FR-003)."""
    namespace = public_namespace()

    assert set(namespace) == set(pathling.__all__)


def test_public_namespace_values_are_the_resolved_objects():
    """Each key maps to the object resolved from the package (INV-5)."""
    namespace = public_namespace()

    # A representative function, argument type, and API type all resolve to the
    # same objects the package exports.
    assert namespace["member_of"] is pathling.member_of
    assert namespace["Coding"] is pathling.Coding
    assert namespace["PathlingContext"] is pathling.PathlingContext


def test_public_namespace_covers_every_public_name():
    """Every name in __all__ resolves to its package attribute (INV-5)."""
    namespace = public_namespace()

    for name in pathling.__all__:
        assert namespace[name] is getattr(pathling, name)


# ========== Namespace sync invariant (INV-5) ==========


def test_run_namespace_is_superset_of_public_api():
    """The run namespace is derived from __all__ plus the run-only extras (INV-1)."""
    expected = set(pathling.__all__) | {"spark", "pathling", "tx_display"}

    namespace = dict(public_namespace())
    namespace["tx_display"] = namespace["display"]
    namespace["spark"] = object()
    namespace["pathling"] = object()

    assert set(namespace) >= expected


def test_console_namespace_equals_public_api_minus_display():
    """The console namespace is __all__ minus display plus the console extras
    (INV-2)."""
    expected = (set(pathling.__all__) - {"display"}) | {
        "spark",
        "pathling",
        "tx_display",
    }

    namespace = dict(public_namespace())
    namespace["tx_display"] = namespace.pop("display")
    namespace["spark"] = object()
    namespace["pathling"] = object()

    assert set(namespace) == expected
