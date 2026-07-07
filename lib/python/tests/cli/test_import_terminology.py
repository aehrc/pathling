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

"""Tests for the ``import-snomed`` and ``import-fhir-terminology`` CLI commands.

The commands are driven through Click's ``CliRunner`` against the shared mock-backed context, so
they import the checked-in fixtures into a temporary store and report a completion summary. Error
cases return a non-zero exit code.
"""

import os

from pathling.cli.main import cli

PROJECT_ROOT = os.path.abspath(
    os.path.join(os.path.dirname(__file__), os.pardir, os.pardir, os.pardir, os.pardir)
)
RF2_MINI = os.path.join(
    PROJECT_ROOT,
    "terminology",
    "src",
    "test",
    "resources",
    "rf2-mini",
    "international-20230601",
)
FHIR_FIXTURES = os.path.join(
    PROJECT_ROOT, "terminology", "src", "test", "resources", "fhir-fixtures", "json"
)


def test_import_snomed_reports_completion(runner, patched_context, tmp_path):
    """The import-snomed command imports the release and prints a completion summary."""
    store = str(tmp_path / "store")
    result = runner.invoke(cli, ["import-snomed", RF2_MINI, store])
    assert result.exit_code == 0, result.output
    assert "Imported SNOMED CT" in result.stdout


def test_import_fhir_terminology_reports_completion(runner, patched_context, tmp_path):
    """The import-fhir-terminology command imports the resources and prints a summary."""
    store = str(tmp_path / "store")
    result = runner.invoke(cli, ["import-fhir-terminology", FHIR_FIXTURES, store])
    assert result.exit_code == 0, result.output
    assert "Imported FHIR terminology" in result.stdout


def test_import_snomed_missing_source_fails(runner, patched_context, tmp_path):
    """A non-existent source produces a non-zero exit code."""
    store = str(tmp_path / "store")
    result = runner.invoke(
        cli, ["import-snomed", str(tmp_path / "does-not-exist"), store]
    )
    assert result.exit_code != 0
