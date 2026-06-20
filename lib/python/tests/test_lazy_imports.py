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

"""Tests for the package's lazy import shim and submodule attribute access.

Each scenario runs in a fresh subprocess so that the assertions reflect a clean
import of ``pathling`` rather than the state left by the shared test fixtures
(which have already imported PySpark and the submodules). This is essential for
the assertion that a plain ``import pathling`` does not import PySpark (FR-008).

Author: John Grimes.
"""

import subprocess
import sys


def _run(code: str) -> str:
    """Runs a snippet in a fresh interpreter and returns its stdout.

    :param code: the Python source to execute.
    :return: the captured stdout.
    """
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
    )
    assert result.returncode == 0, result.stderr
    return result.stdout


# ========== Submodule attribute access (FR-006) ==========


def test_submodule_attribute_access_after_bare_import():
    """`pathling.udfs.member_of` resolves after a bare `import pathling`."""
    out = _run(
        "import pathling\nassert callable(pathling.udfs.member_of)\nprint('ok')\n"
    )
    assert "ok" in out


def test_functions_submodule_attribute_access():
    """`pathling.functions.to_coding` resolves after a bare `import pathling`."""
    out = _run(
        "import pathling\nassert callable(pathling.functions.to_coding)\nprint('ok')\n"
    )
    assert "ok" in out


# ========== dir() advertises submodules (FR-007) ==========


def test_dir_lists_submodules_and_top_level_names():
    """`dir(pathling)` advertises submodules alongside top-level public names."""
    out = _run(
        "import pathling\n"
        "names = dir(pathling)\n"
        "assert 'udfs' in names, names\n"
        "assert 'functions' in names, names\n"
        "assert 'member_of' in names, names\n"
        "print('ok')\n"
    )
    assert "ok" in out


# ========== Top-level export unchanged (FR-009) ==========


def test_top_level_import_still_works():
    """The documented `from pathling import member_of` continues to work."""
    out = _run(
        "from pathling import member_of\nassert callable(member_of)\nprint('ok')\n"
    )
    assert "ok" in out


# ========== Unknown attribute still raises (edge case) ==========


def test_unknown_attribute_raises_attribute_error():
    """An unknown attribute still raises AttributeError (no false fallback)."""
    out = _run(
        "import pathling\n"
        "try:\n"
        "    pathling.not_a_thing\n"
        "    raise SystemExit('expected AttributeError')\n"
        "except AttributeError:\n"
        "    print('ok')\n"
    )
    assert "ok" in out


# ========== Plain import does not import PySpark (FR-008) ==========


def test_bare_import_does_not_import_pyspark():
    """A plain `import pathling` does not import PySpark, keeping startup fast."""
    out = _run(
        "import sys\n"
        "import pathling\n"
        "assert 'pyspark' not in sys.modules, 'pyspark was imported by import pathling'\n"
        "print('ok')\n"
    )
    assert "ok" in out
