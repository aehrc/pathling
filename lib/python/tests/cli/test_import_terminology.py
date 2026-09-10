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


# ========== The default dialect option (043) ==========


def test_import_snomed_passes_default_dialect(
    runner, patched_context, tmp_path, monkeypatch
):
    """The --default-dialect flag reaches the library import call."""
    captured = {}

    def fake_import_snomed(*args):
        captured["args"] = args

    monkeypatch.setattr(patched_context, "import_snomed", fake_import_snomed)
    store = str(tmp_path / "store")
    result = runner.invoke(
        cli, ["import-snomed", "--default-dialect", "en-GB", RF2_MINI, store]
    )

    assert result.exit_code == 0, result.output
    assert captured["args"] == (RF2_MINI, store, None, "code-order", "en-GB")


def test_import_snomed_omits_an_unnamed_default_dialect(
    runner, patched_context, tmp_path, monkeypatch
):
    """Without the flag, no dialect is named and the release decides."""
    captured = {}

    def fake_import_snomed(*args):
        captured["args"] = args

    monkeypatch.setattr(patched_context, "import_snomed", fake_import_snomed)
    store = str(tmp_path / "store")
    result = runner.invoke(cli, ["import-snomed", RF2_MINI, store])

    assert result.exit_code == 0, result.output
    assert captured["args"][4] is None


def test_import_snomed_rejects_a_dialect_the_release_does_not_hold(
    runner, patched_context, tmp_path
):
    """A dialect the release holds no reference set for fails the import."""
    store = str(tmp_path / "store")
    result = runner.invoke(
        cli, ["import-snomed", "--default-dialect", "es", RF2_MINI, store]
    )
    assert result.exit_code != 0


def test_import_snomed_help_documents_the_default_dialect_flag(runner):
    """The flag is discoverable from the command's help."""
    result = runner.invoke(cli, ["import-snomed", "--help"])
    assert result.exit_code == 0
    assert "--default-dialect" in result.stdout


def test_import_snomed_reads_default_dialect_from_config(
    runner, patched_context, tmp_path, monkeypatch
):
    """Without the flag, the configured tx-store.default-dialect is used.

    The store path is also taken from the config, so this is the flagless
    invocation a configured project uses: `pathling import-snomed SOURCE`.
    """
    captured = {}

    def fake_import_snomed(*args):
        captured["args"] = args

    monkeypatch.setattr(patched_context, "import_snomed", fake_import_snomed)
    store = str(tmp_path / "store")
    config = tmp_path / "config.toml"
    config.write_text(
        f'[tx-store]\npath = "{store}"\ndefault-dialect = "en-AU"\n',
        encoding="utf-8",
    )

    result = runner.invoke(cli, ["--config", str(config), "import-snomed", RF2_MINI])

    assert result.exit_code == 0, result.output
    assert captured["args"] == (RF2_MINI, store, None, "code-order", "en-AU")


def test_import_snomed_flag_overrides_configured_default_dialect(
    runner, patched_context, tmp_path, monkeypatch
):
    """The --default-dialect flag wins over the configured value."""
    captured = {}

    def fake_import_snomed(*args):
        captured["args"] = args

    monkeypatch.setattr(patched_context, "import_snomed", fake_import_snomed)
    store = str(tmp_path / "store")
    config = tmp_path / "config.toml"
    config.write_text(
        f'[tx-store]\npath = "{store}"\ndefault-dialect = "en-AU"\n',
        encoding="utf-8",
    )

    result = runner.invoke(
        cli,
        [
            "--config",
            str(config),
            "import-snomed",
            "--default-dialect",
            "en-GB",
            RF2_MINI,
        ],
    )

    assert result.exit_code == 0, result.output
    assert captured["args"][4] == "en-GB"


def test_import_snomed_missing_source_fails(runner, patched_context, tmp_path):
    """A non-existent source produces a non-zero exit code."""
    store = str(tmp_path / "store")
    result = runner.invoke(
        cli, ["import-snomed", str(tmp_path / "does-not-exist"), store]
    )
    assert result.exit_code != 0


# ========== Storage-path fallback to tx-store.path (US3, T013) ==========


def test_import_snomed_falls_back_to_configured_store(
    runner, patched_context, tmp_path
):
    """With tx-store.path configured, an omitted STORAGE_PATH uses the store."""
    store = str(tmp_path / "configured-store")
    result = runner.invoke(cli, ["--tx-store", store, "import-snomed", RF2_MINI])
    assert result.exit_code == 0, result.output
    assert "Imported SNOMED CT" in result.stdout
    # The completion summary names the resolved store.
    assert store in result.stdout


def test_import_snomed_explicit_positional_wins(runner, patched_context, tmp_path):
    """An explicit STORAGE_PATH wins over the configured tx-store.path."""
    configured = str(tmp_path / "configured-store")
    explicit = str(tmp_path / "explicit-store")
    result = runner.invoke(
        cli, ["--tx-store", configured, "import-snomed", RF2_MINI, explicit]
    )
    assert result.exit_code == 0, result.output
    assert explicit in result.stdout
    assert configured not in result.stdout


def test_import_snomed_no_path_anywhere_is_usage_error(
    runner, patched_context, tmp_path
):
    """With neither a positional nor a configured store, exit 2 naming both."""
    result = runner.invoke(cli, ["import-snomed", RF2_MINI])
    assert result.exit_code == 2
    assert "STORAGE_PATH" in result.stderr or "storage path" in result.stderr.lower()
    assert "tx-store.path" in result.stderr


def test_import_fhir_falls_back_to_configured_store(runner, patched_context, tmp_path):
    """import-fhir-terminology also falls back to the configured store."""
    store = str(tmp_path / "configured-store")
    result = runner.invoke(
        cli, ["--tx-store", store, "import-fhir-terminology", FHIR_FIXTURES]
    )
    assert result.exit_code == 0, result.output
    assert store in result.stdout


def test_import_fhir_no_path_anywhere_is_usage_error(runner, patched_context, tmp_path):
    """import-fhir-terminology with no path anywhere exits 2 naming both."""
    result = runner.invoke(cli, ["import-fhir-terminology", FHIR_FIXTURES])
    assert result.exit_code == 2
    assert "tx-store.path" in result.stderr
