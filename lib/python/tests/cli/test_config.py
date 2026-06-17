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

"""Unit tests for configuration loading and precedence.

These tests exercise the pure configuration logic without starting Spark.

Author: John Grimes.
"""

from pathlib import Path

import pytest

from pathling.cli.config import (
    DEFAULT_FHIR_VERSION,
    DEFAULT_TX_SERVER,
    default_config_path,
    load_config_file,
    resolve_config,
    resolve_config_source,
    resolve_secret,
)
from pathling.cli.errors import CliError


def _write_config(tmp_path: Path, contents: str) -> Path:
    """Writes a config file and returns its path."""
    path = tmp_path / "config.toml"
    path.write_text(contents, encoding="utf-8")
    return path


def _write_named(directory: Path, name: str, contents: str) -> Path:
    """Writes a named config file into a directory and returns its path."""
    path = directory / name
    path.write_text(contents, encoding="utf-8")
    return path


def _user_config(monkeypatch, tmp_path: Path) -> Path:
    """Isolates user-level config discovery and returns the user config path.

    Points ``XDG_CONFIG_HOME`` at a directory under ``tmp_path`` so that
    ``default_config_path()`` never resolves to the real user file. The parent
    directory is created; the file itself is not, so callers control whether a
    user-level config exists by writing to the returned path.
    """
    xdg = tmp_path / "xdg"
    monkeypatch.setenv("XDG_CONFIG_HOME", str(xdg))
    user_path = xdg / "pathling" / "config.toml"
    user_path.parent.mkdir(parents=True, exist_ok=True)
    return user_path


# ========== Defaults ==========


def test_defaults_when_no_flags_or_file(tmp_path):
    """With no flags and no file, built-in defaults are used."""
    config = resolve_config(config_path=tmp_path / "missing.toml")

    assert config.tx_server == DEFAULT_TX_SERVER
    assert config.fhir_version == DEFAULT_FHIR_VERSION
    assert config.tx_auth is None
    assert config.verbose is False
    assert config.config_path is None


# ========== Precedence ==========


def test_file_overrides_default(tmp_path):
    """A value in the config file overrides the built-in default."""
    path = _write_config(tmp_path, 'tx-server = "https://file.example/fhir"\n')

    config = resolve_config(config_path=path)

    assert config.tx_server == "https://file.example/fhir"
    assert config.config_path == path


def test_flag_overrides_file(tmp_path):
    """A command-line flag overrides the config file."""
    path = _write_config(tmp_path, 'tx-server = "https://file.example/fhir"\n')

    config = resolve_config(tx_server="https://flag.example/fhir", config_path=path)

    assert config.tx_server == "https://flag.example/fhir"


def test_fhir_version_precedence(tmp_path):
    """The FHIR version follows flag > file > default."""
    path = _write_config(tmp_path, 'fhir-version = "R4"\n')
    assert resolve_config(config_path=path).fhir_version == "R4"
    assert resolve_config(fhir_version="R4", config_path=path).fhir_version == "R4"


def test_unsupported_fhir_version_is_usage_error(tmp_path):
    """An unsupported FHIR version is rejected as a usage error."""
    with pytest.raises(CliError) as exc_info:
        resolve_config(fhir_version="R3", config_path=tmp_path / "missing.toml")

    assert exc_info.value.exit_code == 2
    assert "R3" in exc_info.value.message


# ========== XDG path resolution ==========


def test_default_config_path_honours_xdg(monkeypatch):
    """The default config path honours XDG_CONFIG_HOME when set."""
    monkeypatch.setenv("XDG_CONFIG_HOME", "/custom/xdg")

    assert default_config_path() == Path("/custom/xdg/pathling/config.toml")


def test_default_config_path_falls_back_to_home(monkeypatch):
    """The default config path falls back to ~/.config when XDG is unset."""
    monkeypatch.delenv("XDG_CONFIG_HOME", raising=False)
    monkeypatch.setattr(Path, "home", classmethod(lambda cls: Path("/home/tester")))

    assert default_config_path() == Path("/home/tester/.config/pathling/config.toml")


# ========== Unknown key warnings ==========


def test_unknown_top_level_key_warns(tmp_path):
    """An unknown top-level config key produces a warning naming the key."""
    path = _write_config(tmp_path, 'unknown-key = "x"\ntx-server = "https://a/fhir"\n')
    warnings = []

    load_config_file(path, on_warning=warnings.append)

    assert any("unknown-key" in message for message in warnings)
    assert any("Valid keys" in message for message in warnings)


def test_unknown_auth_key_warns(tmp_path):
    """An unknown key within an auth table produces a qualified warning."""
    path = _write_config(tmp_path, '[terminology-auth]\nbogus = 1\nclient-id = "abc"\n')
    warnings = []

    load_config_file(path, on_warning=warnings.append)

    assert any("terminology-auth.bogus" in message for message in warnings)


# ========== Terminology auth table ==========


def test_terminology_auth_table(tmp_path):
    """The [terminology-auth] table is resolved into TxAuth."""
    path = _write_config(
        tmp_path,
        'tx-server = "https://tx/fhir"\n\n'
        "[terminology-auth]\n"
        'client-id = "my-client"\n'
        'client-secret = "shh"\n'
        'token-endpoint = "https://auth/token"\n'
        'scope = "system/*.read"\n',
    )

    config = resolve_config(config_path=path)

    assert config.tx_auth is not None
    assert config.tx_auth.client_id == "my-client"
    assert config.tx_auth.client_secret == "shh"
    assert config.tx_auth.token_endpoint == "https://auth/token"
    assert config.tx_auth.scope == "system/*.read"
    assert config.tx_auth.enabled is True


def test_auth_flag_overrides_file_secret(tmp_path):
    """An auth flag overrides the corresponding config file value."""
    path = _write_config(
        tmp_path,
        "[terminology-auth]\n"
        'client-id = "file-client"\n'
        'token-endpoint = "https://auth/token"\n',
    )

    config = resolve_config(tx_client_id="flag-client", config_path=path)

    assert config.tx_auth.client_id == "flag-client"


# ========== Secret resolution ==========


def test_resolve_secret_literal():
    """A literal secret is returned unchanged."""
    assert resolve_secret("literal-secret") == "literal-secret"


def test_resolve_secret_from_file(tmp_path):
    """A @file reference reads and strips the file contents."""
    secret_file = tmp_path / "secret.txt"
    secret_file.write_text("  file-secret\n", encoding="utf-8")

    assert resolve_secret(f"@{secret_file}") == "file-secret"


def test_resolve_secret_missing_file_errors(tmp_path):
    """A @file reference to a missing file is an error."""
    with pytest.raises(CliError):
        resolve_secret(f"@{tmp_path / 'nope.txt'}")


def test_resolve_secret_from_env():
    """A None value falls back to the named environment variable."""
    assert (
        resolve_secret(None, "MY_SECRET", env={"MY_SECRET": "env-secret"})
        == "env-secret"
    )


def test_resolve_secret_none_without_env():
    """A None value with no env var yields None."""
    assert resolve_secret(None, "MISSING", env={}) is None


# ========== Project-local config: override (US1) ==========


def test_project_config_overrides_user_config(tmp_path, monkeypatch):
    """A project-local pathling.toml overrides the user-level config value."""
    # Arrange: a user-level config sets server A, a project file sets server B.
    user_path = _user_config(monkeypatch, tmp_path)
    user_path.write_text('tx-server = "https://user.example/fhir"\n', encoding="utf-8")
    project = tmp_path / "project"
    project.mkdir()
    project_file = _write_named(
        project, "pathling.toml", 'tx-server = "https://project.example/fhir"\n'
    )

    # Act.
    config = resolve_config(cwd=project)

    # Assert: the project value wins and the chosen path is the project file.
    assert config.tx_server == "https://project.example/fhir"
    assert config.config_path == project_file


def test_user_config_used_when_no_project_file(tmp_path, monkeypatch):
    """With no project pathling.toml, the user-level config is used."""
    user_path = _user_config(monkeypatch, tmp_path)
    user_path.write_text('tx-server = "https://user.example/fhir"\n', encoding="utf-8")
    empty = tmp_path / "empty"
    empty.mkdir()

    config = resolve_config(cwd=empty)

    assert config.tx_server == "https://user.example/fhir"
    assert config.config_path == user_path


def test_flag_overrides_project_config(tmp_path, monkeypatch):
    """A --tx-server flag overrides a project-local pathling.toml value."""
    _user_config(monkeypatch, tmp_path)
    project = tmp_path / "project"
    project.mkdir()
    _write_named(
        project, "pathling.toml", 'tx-server = "https://project.example/fhir"\n'
    )

    config = resolve_config(tx_server="https://flag.example/fhir", cwd=project)

    assert config.tx_server == "https://flag.example/fhir"


def test_explicit_config_ignores_project_file(tmp_path, monkeypatch):
    """An explicit config_path is used and the project pathling.toml ignored."""
    _user_config(monkeypatch, tmp_path)
    project = tmp_path / "project"
    project.mkdir()
    _write_named(
        project, "pathling.toml", 'tx-server = "https://project.example/fhir"\n'
    )
    explicit = _write_named(
        project, "explicit.toml", 'tx-server = "https://explicit.example/fhir"\n'
    )

    config = resolve_config(config_path=explicit, cwd=project)

    assert config.tx_server == "https://explicit.example/fhir"
    assert config.config_path == explicit


def test_resolve_config_source_origins(tmp_path, monkeypatch):
    """resolve_config_source returns the right path and origin per precedence."""
    user_path = _user_config(monkeypatch, tmp_path)
    project = tmp_path / "project"
    project.mkdir()
    empty = tmp_path / "empty"
    empty.mkdir()

    # Explicit wins regardless of any project or user file.
    explicit = tmp_path / "explicit.toml"
    assert resolve_config_source(explicit, project) == (explicit, "explicit")

    # The project-local file is chosen when present and no explicit path is given.
    project_file = _write_named(project, "pathling.toml", "")
    assert resolve_config_source(None, project) == (project_file, "project")

    # The user-level file is chosen when it exists and no project file is present.
    user_path.write_text("", encoding="utf-8")
    assert resolve_config_source(None, empty) == (user_path, "user")

    # Origin "none" returns the (missing) user path so defaults apply.
    user_path.unlink()
    assert resolve_config_source(None, empty) == (user_path, "none")


def test_project_config_parse_error_names_file(tmp_path, monkeypatch):
    """A malformed project pathling.toml raises CliError naming that file."""
    _user_config(monkeypatch, tmp_path)
    project = tmp_path / "project"
    project.mkdir()
    bad = _write_named(project, "pathling.toml", "this is not = = valid toml\n")

    with pytest.raises(CliError) as exc_info:
        resolve_config(cwd=project)

    assert str(bad) in exc_info.value.message


def test_project_config_unknown_key_warns_naming_file(tmp_path, monkeypatch):
    """An unknown key in the project file warns, naming that file."""
    _user_config(monkeypatch, tmp_path)
    project = tmp_path / "project"
    project.mkdir()
    bad = _write_named(project, "pathling.toml", 'unknown-key = "x"\n')
    warnings = []

    resolve_config(cwd=project, on_warning=warnings.append)

    assert any("unknown-key" in message and str(bad) in message for message in warnings)


# ========== Project-local config: no merge (US2) ==========


def test_omitted_project_key_falls_back_to_default_not_user(tmp_path, monkeypatch):
    """A key omitted from the project file resolves to the built-in default,
    never to the user-level value (FR-003, no silent merge).

    The terminology server is used as the distinguishing key because its
    built-in default differs from a value the user can set. The FHIR version
    cannot demonstrate this on its own, as the only supported version equals
    its default.
    """
    # Arrange: the user-level file sets tx-server (server A) and fhir-version.
    user_path = _user_config(monkeypatch, tmp_path)
    user_path.write_text(
        'tx-server = "https://user.example/fhir"\nfhir-version = "R4"\n',
        encoding="utf-8",
    )
    project = tmp_path / "project"
    project.mkdir()
    # The project file sets only fhir-version, deliberately omitting tx-server.
    _write_named(project, "pathling.toml", 'fhir-version = "R4"\n')

    config = resolve_config(cwd=project)

    # The omitted tx-server falls back to the default, not the user value A.
    assert config.tx_server == DEFAULT_TX_SERVER
    assert config.fhir_version == DEFAULT_FHIR_VERSION


def test_user_auth_table_does_not_leak_into_project(tmp_path, monkeypatch):
    """A user-level [terminology-auth] table does not apply when the project
    file defines no auth table."""
    user_path = _user_config(monkeypatch, tmp_path)
    user_path.write_text(
        "[terminology-auth]\n"
        'client-id = "user-client"\n'
        'token-endpoint = "https://user-auth/token"\n',
        encoding="utf-8",
    )
    project = tmp_path / "project"
    project.mkdir()
    _write_named(
        project, "pathling.toml", 'tx-server = "https://project.example/fhir"\n'
    )

    config = resolve_config(cwd=project)

    assert config.tx_auth is None


# ========== Project-local config: notice (US3) ==========


def test_notice_for_project_file_with_user_file(tmp_path, monkeypatch):
    """A discovered project file emits one notice naming it and the overridden
    user-level file."""
    user_path = _user_config(monkeypatch, tmp_path)
    user_path.write_text('tx-server = "https://user.example/fhir"\n', encoding="utf-8")
    project = tmp_path / "project"
    project.mkdir()
    project_file = _write_named(
        project, "pathling.toml", 'tx-server = "https://project.example/fhir"\n'
    )
    notices = []

    resolve_config(cwd=project, on_notice=notices.append)

    assert notices == [f"Using project config {project_file} (overrides {user_path})."]


def test_notice_omits_overrides_without_user_file(tmp_path, monkeypatch):
    """The notice omits the overrides clause when no user-level file exists."""
    # Isolate XDG but write no user-level file.
    _user_config(monkeypatch, tmp_path)
    project = tmp_path / "project"
    project.mkdir()
    project_file = _write_named(
        project, "pathling.toml", 'tx-server = "https://project.example/fhir"\n'
    )
    notices = []

    resolve_config(cwd=project, on_notice=notices.append)

    assert notices == [f"Using project config {project_file}."]


def test_no_notice_for_user_none_and_explicit_origins(tmp_path, monkeypatch):
    """No notice is emitted for the user, none, and explicit origins."""
    user_path = _user_config(monkeypatch, tmp_path)
    empty = tmp_path / "empty"
    empty.mkdir()

    # User origin: a user-level file exists, no project file present.
    user_path.write_text('tx-server = "https://user.example/fhir"\n', encoding="utf-8")
    user_notices = []
    resolve_config(cwd=empty, on_notice=user_notices.append)
    assert user_notices == []

    # None origin: neither a user-level nor a project file exists.
    user_path.unlink()
    none_notices = []
    resolve_config(cwd=empty, on_notice=none_notices.append)
    assert none_notices == []

    # Explicit origin: an explicit path is given, the project file is ignored.
    project = tmp_path / "project"
    project.mkdir()
    explicit = _write_named(
        project, "explicit.toml", 'tx-server = "https://explicit.example/fhir"\n'
    )
    _write_named(
        project, "pathling.toml", 'tx-server = "https://project.example/fhir"\n'
    )
    explicit_notices = []
    resolve_config(config_path=explicit, cwd=project, on_notice=explicit_notices.append)
    assert explicit_notices == []
