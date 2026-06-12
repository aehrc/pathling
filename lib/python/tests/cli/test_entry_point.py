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

"""Subprocess smoke test of the installed ``pathling`` console script.

This proves the packaging contract: the entry point declared in pyproject.toml
resolves to a working command that runs quickly without starting Spark.

Author: John Grimes.
"""

import json
import subprocess
import sys
from pathlib import Path

import pytest

from pathling._version import __version__

# The console script is installed alongside the active interpreter.
ENTRY_POINT = Path(sys.executable).parent / "pathling"


@pytest.mark.skipif(
    not ENTRY_POINT.exists(), reason="pathling console script is not installed"
)
def test_entry_point_version_is_fast():
    """`pathling --version` runs via the real console script, fast and green."""
    result = subprocess.run(
        [str(ENTRY_POINT), "--version"],
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert result.returncode == 0, result.stderr
    assert __version__ in result.stdout


@pytest.mark.skipif(
    not ENTRY_POINT.exists(), reason="pathling console script is not installed"
)
def test_entry_point_help_lists_commands():
    """`pathling --help` runs via the console script and lists the commands."""
    result = subprocess.run(
        [str(ENTRY_POINT), "--help"],
        capture_output=True,
        text=True,
        timeout=30,
    )

    assert result.returncode == 0, result.stderr
    for command in ("convert", "view", "fhirpath", "export", "member-of"):
        assert command in result.stdout


@pytest.mark.skipif(
    not ENTRY_POINT.exists(), reason="pathling console script is not installed"
)
def test_real_invocation_output_is_clean():
    """A real (non-mocked) command produces clean stdout and silent stderr.

    This exercises the actual Spark session creation path and asserts the
    end-to-end guarantees: data only on stdout (SC-007), and no Spark/JVM/Ivy
    log noise or stack traces on stderr by default (FR-004).
    """
    import tempfile

    with tempfile.TemporaryDirectory() as tmp:
        resource = Path(tmp) / "patient.json"
        resource.write_text(
            json.dumps(
                {
                    "resourceType": "Patient",
                    "id": "x",
                    "name": [{"given": ["John"], "family": "Smith"}],
                }
            )
        )
        result = subprocess.run(
            [
                str(ENTRY_POINT),
                "fhirpath",
                str(resource),
                "-e",
                "name.given.first()",
                "--format",
                "csv",
            ],
            capture_output=True,
            text=True,
            timeout=300,
        )

    assert result.returncode == 0, result.stderr
    # Data is on stdout, and only the data.
    assert "John" in result.stdout
    assert "INFO" not in result.stdout and "WARN" not in result.stdout
    # No Spark/JVM/Ivy log noise or stack traces on stderr by default.
    for noise in (
        "Setting default log level",
        "resolving dependencies",
        "Using incubator modules",
        "\tat org.apache",
        "Traceback",
    ):
        assert noise not in result.stderr, result.stderr
