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
    resolve_secret,
)
from pathling.cli.errors import CliError


def _write_config(tmp_path: Path, contents: str) -> Path:
    """Writes a config file and returns its path."""
    path = tmp_path / "config.toml"
    path.write_text(contents, encoding="utf-8")
    return path


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
