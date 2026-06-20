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

"""Parsing, validation, coercion, and merge for user-supplied Spark settings.

This module holds the pure logic that turns ``[spark]`` config-table entries and
``--spark-conf KEY=VALUE`` flags into the effective Spark configuration applied
when a session is built. Keys must begin with ``spark.``; scalar values are
coerced to the string form Spark expects; values support the existing
``@file``/environment secret resolution. The three managed keys
(``spark.jars.packages``, ``spark.sql.extensions``,
``spark.sql.catalog.spark_catalog``) are merged with item-level protection
against Pathling's managed defaults so the library keeps working while user
additions take effect. None of this requires PySpark, so it runs before any
Spark session starts.

Author: John Grimes.
"""

from typing import Callable, Optional

from pathling._spark_defaults import (
    CATALOG_KEY,
    EXTENSIONS_KEY,
    MANAGED_COORDINATES,
    PACKAGES_KEY,
    managed_spark_defaults,
)
from pathling.cli.errors import EXIT_USAGE, CliError


def parse_spark_conf_flags(flags) -> dict:
    """Parses repeatable ``--spark-conf KEY=VALUE`` flags into a mapping.

    Each flag is split on the first ``=`` only, so a value may itself contain
    ``=`` (for example a JVM option). When the same key appears more than once,
    the last occurrence wins.

    :param flags: an iterable of raw ``KEY=VALUE`` flag strings.
    :return: a mapping of key to value, with later duplicates overriding earlier.
    :raises CliError: if a flag is not of the form ``KEY=VALUE`` (exit code 2).
    """
    result = {}
    for flag in flags or ():
        if "=" not in flag:
            raise CliError(
                f"Invalid --spark-conf value '{flag}'. Expected the form KEY=VALUE.",
                exit_code=EXIT_USAGE,
            )
        key, value = flag.split("=", 1)
        result[key] = value
    return result


def validate_and_coerce(key: str, value) -> str:
    """Validates a Spark configuration key and coerces its value to a string.

    The key must begin with ``spark.``. Scalar values (string, integer, float,
    boolean) are coerced to the string form Spark expects, with booleans
    rendered as ``true``/``false``. Any other value type (a TOML array or table)
    is rejected.

    :param key: the Spark configuration key.
    :param value: the raw value from the config table or a flag.
    :return: the value coerced to a string.
    :raises CliError: if the key is not prefixed ``spark.`` or the value is not
            a scalar (exit code 2).
    """
    if not key.startswith("spark."):
        raise CliError(
            f"Invalid Spark configuration key '{key}'. "
            "Spark configuration keys must begin with 'spark.'.",
            exit_code=EXIT_USAGE,
        )
    # ``bool`` is a subclass of ``int``, so it must be checked first to render
    # as ``true``/``false`` rather than ``1``/``0``.
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, (int, float, str)):
        return str(value)
    raise CliError(
        f"Invalid value for Spark configuration key '{key}'. "
        "Only scalar values (string, integer, float, boolean) are allowed.",
        exit_code=EXIT_USAGE,
    )


def resolve_spark_conf(
    file_table: Optional[dict],
    flag_map: Optional[dict],
    env: Optional[dict] = None,
) -> dict:
    """Combines, validates, coerces, and secret-resolves the user Spark map.

    The flag map overrides the file table per key. Each entry's key and value
    are then validated and coerced, and string values are passed through the
    existing secret resolver so a ``@file`` reference is read from disk.

    :param file_table: the parsed ``[spark]`` table, or None.
    :param flag_map: the parsed ``--spark-conf`` flag map, or None.
    :param env: the environment mapping for secret resolution.
    :return: the validated, coerced, and resolved user Spark map.
    :raises CliError: if a key or value is invalid, or a ``@file`` reference
            cannot be read.
    """
    # The secret resolver is imported lazily to avoid a circular import with the
    # config module, which depends on this module.
    from pathling.cli.config import resolve_secret

    combined = {}
    combined.update(file_table or {})
    # Flag values win over the file table for the same key.
    combined.update(flag_map or {})

    resolved = {}
    for key, raw in combined.items():
        coerced = validate_and_coerce(key, raw)
        resolved[key] = resolve_secret(coerced, None, env)
    return resolved


def _split_list(value: str) -> list:
    """Splits a comma-separated Spark list value, dropping empty entries.

    :param value: a comma-separated string such as a packages or extensions list.
    :return: the non-empty, stripped items in order.
    """
    return [item.strip() for item in value.split(",") if item.strip()]


def _group_artifact(coordinate: str) -> str:
    """Returns the ``group:artifact`` identity of a Maven coordinate.

    :param coordinate: a ``group:artifact:version`` Maven coordinate.
    :return: the ``group:artifact`` prefix used to detect a version override.
    """
    return ":".join(coordinate.split(":")[:2])


def _merge_packages(
    user_value: str,
    on_warning: Optional[Callable[[str], None]],
) -> str:
    """Unions user package coordinates with the managed defaults.

    Managed coordinates appear first. A user coordinate whose ``group:artifact``
    is new is appended. A user coordinate that matches a managed
    ``group:artifact`` at a different version replaces the managed entry and, for
    a Pathling-managed coordinate, emits a single warning naming it.

    :param user_value: the user's comma-separated ``spark.jars.packages`` value.
    :param on_warning: a callback for the managed-version-override warning, or
           None to suppress it.
    :return: the merged, deduplicated comma-separated packages string.
    """
    result = []
    # Map each ``group:artifact`` to its index in ``result`` for deduplication.
    index_of = {}

    def add(coordinate: str) -> None:
        identity = _group_artifact(coordinate)
        if identity not in index_of:
            index_of[identity] = len(result)
            result.append(coordinate)
            return
        existing = result[index_of[identity]]
        if existing == coordinate:
            return
        # A different version of an already-present coordinate: the user wins.
        result[index_of[identity]] = coordinate
        if identity in MANAGED_COORDINATES and on_warning is not None:
            on_warning(
                f"The Spark configuration overrides the managed package "
                f"'{identity}' with '{coordinate}'. This non-default version "
                "may not be supported."
            )

    for coordinate in _split_list(managed_spark_defaults()[PACKAGES_KEY]):
        add(coordinate)
    for coordinate in _split_list(user_value):
        add(coordinate)
    return ",".join(result)


def _merge_extensions(user_value: str) -> str:
    """Unions user SQL extension classes with the managed defaults.

    The Delta extension (listed in the managed defaults) is always retained and
    appears first; user extensions are appended, deduplicated.

    :param user_value: the user's comma-separated ``spark.sql.extensions`` value.
    :return: the merged, deduplicated comma-separated extensions string.
    """
    result = []
    seen = set()
    managed = _split_list(managed_spark_defaults()[EXTENSIONS_KEY])
    for extension in managed + _split_list(user_value):
        if extension not in seen:
            seen.add(extension)
            result.append(extension)
    return ",".join(result)


def _merge_catalog(user_value: str, key: str) -> Optional[str]:
    """Validates the session catalog against the managed Delta catalog.

    :param user_value: the user's ``spark.sql.catalog.spark_catalog`` value.
    :param key: the configuration key, for the error message.
    :return: None when the value equals the managed Delta catalog (a no-op the
             builder already applies); the function never returns another value.
    :raises CliError: if the value differs from the managed Delta catalog (exit
            code 2).
    """
    managed = managed_spark_defaults()[CATALOG_KEY]
    if user_value == managed:
        return None
    raise CliError(
        f"The Spark configuration key '{key}' is managed by Pathling and must be "
        f"'{managed}'. Remove it or set it to the Delta catalog.",
        exit_code=EXIT_USAGE,
    )


def merge_spark_conf(
    user_map: dict,
    on_warning: Optional[Callable[[str], None]] = None,
) -> dict:
    """Merges the user Spark map with Pathling's managed defaults.

    Plain keys pass through unchanged. The managed list keys
    (``spark.jars.packages``, ``spark.sql.extensions``) are unioned with the
    managed defaults and deduplicated. ``spark.sql.catalog.spark_catalog`` is
    dropped when it equals the managed Delta catalog and is an error otherwise.
    Only keys the user actually set appear in the result; keys they did not touch
    are left to the session builder's managed defaults.

    :param user_map: the validated, coerced, and resolved user Spark map.
    :param on_warning: a callback for the managed-version-override warning, or
           None to suppress it.
    :return: the effective Spark configuration to apply on top of the defaults.
    :raises CliError: if the session catalog is set to a non-Delta value.
    """
    result = {}
    for key, value in user_map.items():
        if key == PACKAGES_KEY:
            result[key] = _merge_packages(value, on_warning)
        elif key == EXTENSIONS_KEY:
            result[key] = _merge_extensions(value)
        elif key == CATALOG_KEY:
            merged = _merge_catalog(value, key)
            if merged is not None:
                result[key] = merged
        else:
            result[key] = value
    return result
