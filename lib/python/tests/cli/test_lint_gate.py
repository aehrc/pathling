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

"""Tests for the scope of the missing-annotation lint gate.

Every signature in ``pathling/cli/`` must carry type annotations, enforced by a
nested ``ruff.toml`` that adds the ``ANN`` rules for that directory only. These
tests prove the gate both bites inside the CLI package and stays out of the rest
of the library and the test suite - a gate that fired everywhere would have to be
suppressed everywhere, and one that fired nowhere would not be a gate at all.

Ruff is run over standard input with a substituted filename, so the scoping is
exercised without creating files that a failed run could leave behind.

Author: John Grimes.
"""

import subprocess
import sys
from pathlib import Path

import pytest

# An unannotated parameter and return type: two ANN findings wherever the gate
# applies, and none where it does not.
UNANNOTATED_SOURCE = "def scratch(value):\n    return value\n"

# The library root, which is the working directory the project's own lint check
# runs from and the directory holding the root configuration.
LIBRARY_ROOT = Path(__file__).resolve().parents[2]


def _check(relative_path: str) -> subprocess.CompletedProcess:
    """Runs ruff over the unannotated source, attributed to the given path.

    :param relative_path: the path, relative to the library root, that the
           source is attributed to; this is what ruff matches configuration
           against.
    :return: the completed ruff process, with its output captured.
    """
    return subprocess.run(
        [
            sys.executable,
            "-m",
            "ruff",
            "check",
            "--no-cache",
            "--stdin-filename",
            relative_path,
            "-",
        ],
        cwd=LIBRARY_ROOT,
        input=UNANNOTATED_SOURCE,
        capture_output=True,
        text=True,
    )


def test_gate_rejects_an_unannotated_signature_in_the_cli_package():
    """An unannotated function inside the CLI package fails the check (FR-020)."""
    result = _check("pathling/cli/scratch.py")

    assert result.returncode != 0, result.stdout
    # Both the parameter and the return type are named.
    assert "ANN001" in result.stdout
    assert "ANN201" in result.stdout
    assert "value" in result.stdout


def test_gate_covers_a_nested_module_in_the_cli_package():
    """The gate applies throughout the package, not only at its top level."""
    result = _check("pathling/cli/nested/scratch.py")

    assert result.returncode != 0, result.stdout
    assert "ANN001" in result.stdout


@pytest.mark.parametrize(
    "relative_path",
    [
        # The rest of the library, outside the CLI package.
        "pathling/scratch.py",
        # The test suite, which is deliberately left unannotated.
        "tests/scratch.py",
        "tests/cli/scratch.py",
    ],
)
def test_gate_does_not_apply_outside_the_cli_package(relative_path):
    """The same unannotated source passes everywhere else (FR-020)."""
    result = _check(relative_path)

    assert result.returncode == 0, result.stdout + result.stderr
    assert "ANN" not in result.stdout
