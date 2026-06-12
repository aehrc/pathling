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

"""Configuration resolution for the Pathling command line interface.

Configuration is resolved with the precedence flag > config file > built-in
default. The config file is TOML located at
``${XDG_CONFIG_HOME:-~/.config}/pathling/config.toml`` unless overridden with
``--config``. Secret values for authentication may be supplied as a literal, a
``@/path/to/file`` reference, or via an environment variable.

Author: John Grimes.
"""

import os
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Callable, Optional

# The library's built-in default terminology server, mirrored here so the CLI
# can report it in error messages without starting a Spark session.
DEFAULT_TX_SERVER = "https://tx.ontoserver.csiro.au/fhir"

# The default FHIR version used when none is configured.
DEFAULT_FHIR_VERSION = "R4"

# The FHIR versions the CLI accepts.
SUPPORTED_FHIR_VERSIONS = ("R4",)

# Valid top-level keys in the config file.
VALID_CONFIG_KEYS = frozenset(
    {"tx-server", "fhir-version", "terminology-auth", "bulk-auth"}
)

# Valid keys within the [terminology-auth] and [bulk-auth] tables.
VALID_AUTH_KEYS = frozenset(
    {"client-id", "client-secret", "private-key-jwk", "token-endpoint", "scope"}
)


@dataclass
class TxAuth:
    """Terminology server authentication settings.

    :param client_id: the OAuth2 client identifier.
    :param client_secret: the resolved client secret, or None.
    :param token_endpoint: the OAuth2 token endpoint.
    :param scope: an optional OAuth2 scope.
    """

    client_id: Optional[str] = None
    client_secret: Optional[str] = None
    token_endpoint: Optional[str] = None
    scope: Optional[str] = None

    @property
    def enabled(self) -> bool:
        """Whether enough has been supplied to attempt authentication.

        :return: True when a client identifier and token endpoint are present.
        """
        return bool(self.client_id and self.token_endpoint)


@dataclass
class BulkAuth:
    """SMART backend services authentication settings for bulk export.

    :param client_id: the OAuth2 client identifier.
    :param private_key_jwk: the resolved private key JWK, or None.
    :param client_secret: the resolved client secret, or None.
    :param token_endpoint: the OAuth2 token endpoint.
    :param scope: an optional OAuth2 scope.
    """

    client_id: str
    token_endpoint: Optional[str] = None
    private_key_jwk: Optional[str] = None
    client_secret: Optional[str] = None
    scope: Optional[str] = None

    @property
    def mechanism(self) -> str:
        """A human-readable name for the credential mechanism in use.

        :return: a description of the credential type for error messages.
        """
        if self.private_key_jwk:
            return "a private key JWK"
        if self.client_secret:
            return "a client secret"
        return "no credential"


@dataclass
class CliConfig:
    """Resolved global configuration for a single invocation.

    :param tx_server: the terminology server URL.
    :param tx_auth: terminology authentication settings, or None.
    :param fhir_version: the FHIR version code.
    :param verbose: whether verbose logging and stack traces are enabled.
    :param config_path: the path the config file was read from, or None.
    """

    tx_server: str = DEFAULT_TX_SERVER
    tx_auth: Optional[TxAuth] = None
    fhir_version: str = DEFAULT_FHIR_VERSION
    verbose: bool = False
    config_path: Optional[Path] = None


def _load_toml(path: Path) -> dict:
    """Reads and parses a TOML file using the available TOML library.

    :param path: the path to the TOML file.
    :return: the parsed contents as a dict.
    :raises CliError: if the file cannot be parsed as TOML.
    """
    # Prefer the standard library on Python 3.11+, fall back to tomli.
    try:
        import tomllib as toml_lib
    except ModuleNotFoundError:  # pragma: no cover - exercised on Python < 3.11.
        import tomli as toml_lib

    from pathling.cli.errors import CliError

    try:
        with open(path, "rb") as handle:
            return toml_lib.load(handle)
    except toml_lib.TOMLDecodeError as exc:
        raise CliError(
            f"Could not parse the config file at {path}: {exc}. "
            "Check that it is valid TOML."
        ) from exc


def default_config_path() -> Path:
    """Computes the default config file path, honouring ``XDG_CONFIG_HOME``.

    :return: the path to the default config file location.
    """
    xdg = os.environ.get("XDG_CONFIG_HOME")
    base = Path(xdg) if xdg else Path.home() / ".config"
    return base / "pathling" / "config.toml"


def resolve_secret(
    value: Optional[str],
    env_var: Optional[str] = None,
    env: Optional[dict] = None,
) -> Optional[str]:
    """Resolves a secret value from a literal, a ``@file`` reference, or an
    environment variable.

    A value beginning with ``@`` is treated as a path to a file whose stripped
    contents are returned. When ``value`` is None and ``env_var`` is given, the
    environment variable is consulted.

    :param value: the literal value, ``@path`` reference, or None.
    :param env_var: the name of a fallback environment variable, or None.
    :param env: the environment mapping to read from; defaults to ``os.environ``.
    :return: the resolved secret, or None when nothing is available.
    :raises CliError: if a ``@file`` reference cannot be read.
    """
    environment = env if env is not None else os.environ
    if value is None:
        if env_var is not None:
            return environment.get(env_var)
        return None
    if value.startswith("@"):
        file_path = Path(value[1:])
        from pathling.cli.errors import CliError

        try:
            return file_path.read_text(encoding="utf-8").strip()
        except OSError as exc:
            raise CliError(
                f"Could not read the secret file at {file_path}: {exc}. "
                "Check the path and permissions."
            ) from exc
    return value


def load_config_file(
    path: Path,
    on_warning: Optional[Callable[[str], None]] = None,
) -> dict:
    """Loads a config file, warning about unknown keys.

    :param path: the config file path.
    :param on_warning: an optional callback invoked with each warning message;
           defaults to writing to stderr.
    :return: the parsed config as a dict, or an empty dict when the file is
             absent.
    """
    if not path.exists():
        return {}

    warn = on_warning or (lambda message: print(message, file=sys.stderr))
    data = _load_toml(path)

    valid_keys = ", ".join(sorted(VALID_CONFIG_KEYS))
    for key in data:
        if key not in VALID_CONFIG_KEYS:
            warn(
                f"Ignoring unknown config key '{key}' in {path}. "
                f"Valid keys are: {valid_keys}."
            )
    for table_name in ("terminology-auth", "bulk-auth"):
        table = data.get(table_name)
        if isinstance(table, dict):
            valid_auth = ", ".join(sorted(VALID_AUTH_KEYS))
            for key in table:
                if key not in VALID_AUTH_KEYS:
                    warn(
                        f"Ignoring unknown config key '{table_name}.{key}' in "
                        f"{path}. Valid keys are: {valid_auth}."
                    )
    return data


def _resolve_tx_auth(
    file_data: dict,
    client_id: Optional[str],
    client_secret: Optional[str],
    token_endpoint: Optional[str],
    scope: Optional[str],
    env: Optional[dict],
) -> Optional[TxAuth]:
    """Merges terminology auth settings from flags and the config file.

    :param file_data: the parsed config file contents.
    :param client_id: the ``--tx-client-id`` flag value, or None.
    :param client_secret: the ``--tx-client-secret`` flag value, or None.
    :param token_endpoint: the ``--tx-token-endpoint`` flag value, or None.
    :param scope: the ``--tx-scope`` flag value, or None.
    :param env: the environment mapping for secret resolution.
    :return: a populated :class:`TxAuth`, or None when no auth is configured.
    """
    table = file_data.get("terminology-auth") or {}
    resolved_client_id = client_id or table.get("client-id")
    resolved_token_endpoint = token_endpoint or table.get("token-endpoint")
    resolved_scope = scope or table.get("scope")
    resolved_secret = resolve_secret(
        client_secret or table.get("client-secret"), None, env
    )

    if not any(
        [resolved_client_id, resolved_token_endpoint, resolved_scope, resolved_secret]
    ):
        return None
    return TxAuth(
        client_id=resolved_client_id,
        client_secret=resolved_secret,
        token_endpoint=resolved_token_endpoint,
        scope=resolved_scope,
    )


def resolve_bulk_auth(
    file_bulk_auth: Optional[dict],
    client_id: Optional[str] = None,
    client_secret: Optional[str] = None,
    private_key_jwk: Optional[str] = None,
    token_endpoint: Optional[str] = None,
    scope: Optional[str] = None,
    env: Optional[dict] = None,
) -> Optional[BulkAuth]:
    """Resolves bulk export authentication from flags and the config file.

    Authentication is considered configured only when a client identifier is
    present. Secret values are resolved as a literal, a ``@file`` reference, or
    the ``PATHLING_CLIENT_SECRET`` / ``PATHLING_PRIVATE_KEY_JWK`` environment
    variables.

    :param file_bulk_auth: the parsed ``[bulk-auth]`` table, or None.
    :param client_id: the ``--client-id`` flag value, or None.
    :param client_secret: the ``--client-secret`` flag value, or None.
    :param private_key_jwk: the ``--private-key-jwk`` flag value, or None.
    :param token_endpoint: the ``--token-endpoint`` flag value, or None.
    :param scope: the ``--scope`` flag value, or None.
    :param env: the environment mapping for secret resolution.
    :return: a populated :class:`BulkAuth`, or None when no client ID is set.
    :raises CliError: when the auth configuration is incomplete or ambiguous.
    """
    table = file_bulk_auth or {}
    resolved_client_id = client_id or table.get("client-id")
    if not resolved_client_id:
        return None

    resolved_token_endpoint = token_endpoint or table.get("token-endpoint")
    resolved_scope = scope or table.get("scope")
    resolved_secret = resolve_secret(
        client_secret or table.get("client-secret"), "PATHLING_CLIENT_SECRET", env
    )
    resolved_jwk = resolve_secret(
        private_key_jwk or table.get("private-key-jwk"),
        "PATHLING_PRIVATE_KEY_JWK",
        env,
    )

    from pathling.cli.errors import EXIT_USAGE, CliError

    if not resolved_token_endpoint:
        raise CliError(
            "Bulk export authentication requires a token endpoint. "
            "Add --token-endpoint, or set it in the [bulk-auth] config table.",
            exit_code=EXIT_USAGE,
        )
    if resolved_secret and resolved_jwk:
        raise CliError(
            "Provide exactly one of --client-secret or --private-key-jwk, not both.",
            exit_code=EXIT_USAGE,
        )
    if not resolved_secret and not resolved_jwk:
        raise CliError(
            "Bulk export authentication requires a credential. Provide one of "
            "--client-secret or --private-key-jwk.",
            exit_code=EXIT_USAGE,
        )

    return BulkAuth(
        client_id=resolved_client_id,
        token_endpoint=resolved_token_endpoint,
        private_key_jwk=resolved_jwk,
        client_secret=resolved_secret,
        scope=resolved_scope,
    )


def resolve_config(
    tx_server: Optional[str] = None,
    tx_client_id: Optional[str] = None,
    tx_client_secret: Optional[str] = None,
    tx_token_endpoint: Optional[str] = None,
    tx_scope: Optional[str] = None,
    fhir_version: Optional[str] = None,
    verbose: bool = False,
    config_path: Optional[Path] = None,
    env: Optional[dict] = None,
    on_warning: Optional[Callable[[str], None]] = None,
) -> CliConfig:
    """Resolves global configuration from flags, the config file, and defaults.

    Precedence is always flag > config file > built-in default.

    :param tx_server: the ``--tx-server`` flag value, or None.
    :param tx_client_id: the ``--tx-client-id`` flag value, or None.
    :param tx_client_secret: the ``--tx-client-secret`` flag value, or None.
    :param tx_token_endpoint: the ``--tx-token-endpoint`` flag value, or None.
    :param tx_scope: the ``--tx-scope`` flag value, or None.
    :param fhir_version: the ``--fhir-version`` flag value, or None.
    :param verbose: the ``--verbose`` flag value.
    :param config_path: an explicit config file path, or None for the default.
    :param env: the environment mapping for secret resolution.
    :param on_warning: an optional warning callback passed to the file loader.
    :return: the resolved :class:`CliConfig`.
    :raises CliError: if the resolved FHIR version is unsupported.
    """
    path = config_path or default_config_path()
    file_data = load_config_file(path, on_warning)

    resolved_tx_server = tx_server or file_data.get("tx-server") or DEFAULT_TX_SERVER
    resolved_fhir_version = (
        fhir_version or file_data.get("fhir-version") or DEFAULT_FHIR_VERSION
    )
    if resolved_fhir_version not in SUPPORTED_FHIR_VERSIONS:
        from pathling.cli.errors import CliError

        supported = ", ".join(SUPPORTED_FHIR_VERSIONS)
        raise CliError(
            f"Unsupported FHIR version '{resolved_fhir_version}'. "
            f"Supported versions are: {supported}.",
            exit_code=2,
        )

    tx_auth = _resolve_tx_auth(
        file_data,
        tx_client_id,
        tx_client_secret,
        tx_token_endpoint,
        tx_scope,
        env,
    )

    return CliConfig(
        tx_server=resolved_tx_server,
        tx_auth=tx_auth,
        fhir_version=resolved_fhir_version,
        verbose=verbose,
        config_path=path if path.exists() else None,
    )
