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


# ========== Deferred imports ==========


def test_importing_main_does_not_import_pyspark():
    """Importing pathling.cli.main must not import pyspark.

    This is checked in a fresh subprocess because the test process has already
    imported pyspark via the shared Spark fixture.
    """
    code = (
        "import sys\n"
        "import pathling.cli.main\n"
        "assert 'pyspark' not in sys.modules, "
        "'pyspark was imported by pathling.cli.main'\n"
        "print('ok')\n"
    )
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
    )

    assert result.returncode == 0, result.stderr
    assert "ok" in result.stdout
