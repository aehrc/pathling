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

This module holds the CLI-specific logic that turns ``[spark]`` config-table
entries and ``--spark-conf KEY=VALUE`` flags into the effective Spark
configuration applied when a session is built. Keys must begin with ``spark.``;
scalar values are coerced to the string form Spark expects; values support the
existing ``@file``/environment secret resolution. The merge of the resolved map
with Pathling's managed defaults (with item-level protection for the managed
keys ``spark.jars.packages``, ``spark.sql.extensions`` and
``spark.sql.catalog.spark_catalog``) lives in :mod:`pathling._spark_defaults`,
shared with the Python library API; this module wraps it so CLI usage errors
keep their exit code 2 contract. None of this requires PySpark, so it runs
before any Spark session starts.

Author: John Grimes.
"""

from __future__ import annotations

from typing import Callable, Iterable, Optional

from pathling._spark_defaults import merge_spark_conf as _shared_merge_spark_conf
from pathling.cli.errors import EXIT_USAGE, CliError


def parse_spark_conf_flags(flags: Optional[Iterable[str]]) -> dict:
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


def validate_and_coerce(key: str, value: object) -> str:
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


def merge_spark_conf(
    user_map: dict,
    on_warning: Optional[Callable[[str], None]] = None,
) -> dict:
    """Merges the user Spark map with Pathling's managed defaults.

    Delegates to :func:`pathling._spark_defaults.merge_spark_conf` and maps
    validation failures onto :class:`CliError`, preserving the CLI's usage-error
    (exit code 2) contract.

    :param user_map: the validated, coerced, and resolved user Spark map.
    :param on_warning: a callback for the managed-version-override warning, or
           None to suppress it.
    :return: the effective Spark configuration to apply on top of the defaults.
    :raises CliError: if the session catalog is set to a non-Delta value (exit
            code 2).
    """
    try:
        return _shared_merge_spark_conf(user_map, on_warning=on_warning)
    except ValueError as e:
        raise CliError(str(e), exit_code=EXIT_USAGE) from e
