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

"""Tests for the root command: version, help, and deferred heavy imports.

Author: John Grimes.
"""

import subprocess
import sys

from pathling._version import __version__
from pathling.cli.main import cli

ALL_COMMANDS = [
    "convert",
    "view",
    "fhirpath",
    "export",
    "member-of",
    "translate",
    "subsumes",
    "subsumed-by",
    "display",
    "property-of",
    "designation",
    "run",
    "console",
]


# ========== Version ==========


def test_version_prints_package_version(runner):
    """`pathling --version` prints the package version and exits 0."""
    result = runner.invoke(cli, ["--version"])

    assert result.exit_code == 0
    assert __version__ in result.output


# ========== Help ==========


def test_help_lists_all_commands(runner):
    """`pathling --help` documents every command."""
    result = runner.invoke(cli, ["--help"])

    assert result.exit_code == 0
    for command in ALL_COMMANDS:
        assert command in result.output


def test_help_mentions_config_file(runner):
    """The root help describes the config file location."""
    result = runner.invoke(cli, ["--help"])

    assert "config" in result.output.lower()


def test_help_lists_spark_conf_option(runner):
    """The root group exposes a repeatable --spark-conf option in its help."""
    result = runner.invoke(cli, ["--help"])

    assert result.exit_code == 0
    assert "--spark-conf" in result.output


# ========== Deferred imports ==========


def test_importing_main_does_not_import_heavy_modules():
    """Importing pathling.cli.main must import neither pyspark nor IPython.

    This is checked in a fresh subprocess because the test process has already
    imported the heavy modules via the shared fixtures.
    """
    code = (
        "import sys\n"
        "import pathling.cli.main\n"
        "assert 'pyspark' not in sys.modules, "
        "'pyspark was imported by pathling.cli.main'\n"
        "assert 'IPython' not in sys.modules, "
        "'IPython was imported by pathling.cli.main'\n"
        "print('ok')\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert "ok" in result.stdout


# ========== Central error handler threads the server URL (US4) ==========


def test_central_handler_passes_tx_server_as_server_url(runner, monkeypatch, tmp_path):
    """The central handler passes the configured tx_server to friendly_message
    so connection failures from convert/view/fhirpath name the server (FR-011).
    """
    captured = {}

    def fake_friendly(exc, verbose=False, server_url=None, store_path=None):
        captured["server_url"] = server_url
        captured["store_path"] = store_path
        return "rendered"

    monkeypatch.setattr("pathling.cli.main.friendly_message", fake_friendly)

    # Fail inside the command, past configuration resolution, with a generic
    # exception so the central ``except Exception`` branch is exercised.
    def boom(config, console=None):
        raise RuntimeError("Connection refused")

    monkeypatch.setattr("pathling.cli.session.create_context", boom)

    source = tmp_path / "data"
    source.mkdir()
    (source / "Patient.ndjson").write_text(
        '{"resourceType":"Patient"}\n', encoding="utf-8"
    )
    out = tmp_path / "out"

    result = runner.invoke(
        cli,
        [
            "--tx-server",
            "https://tx.example/fhir",
            "convert",
            str(source),
            "--to",
            "ndjson",
            "-o",
            str(out),
        ],
    )

    assert result.exit_code == 1
    assert captured["server_url"] == "https://tx.example/fhir"


# ========== Local-mode error threading (US1, T005) ==========


def test_server_url_from_is_none_when_store_configured():
    """_server_url_from returns None in local mode so no server is ever named."""
    import click

    from pathling.cli.config import CliConfig, TxStore
    from pathling.cli.main import CliContext, _server_url_from, _store_path_from
    from pathling.cli.render import stderr_console

    ctx = click.Context(cli)
    ctx.obj = CliContext(
        config=CliConfig(tx_store=TxStore(path="/data/tx-store")),
        console=stderr_console(),
    )

    assert _server_url_from(ctx) is None
    assert _store_path_from(ctx) == "/data/tx-store"


def test_server_url_from_returns_url_in_remote_mode():
    """In remote mode the configured server URL is returned and no store path."""
    import click

    from pathling.cli.config import CliConfig
    from pathling.cli.main import CliContext, _server_url_from, _store_path_from
    from pathling.cli.render import stderr_console

    ctx = click.Context(cli)
    ctx.obj = CliContext(
        config=CliConfig(tx_server="https://tx.example/fhir"),
        console=stderr_console(),
    )

    assert _server_url_from(ctx) == "https://tx.example/fhir"
    assert _store_path_from(ctx) is None
