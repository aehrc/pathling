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

"""The single, PySpark-free source of truth for Pathling's managed Spark defaults.

The managed coordinates, the Delta SQL extension, and the Delta catalog that
every Pathling Spark session requires are defined here, built from the versions
in :mod:`pathling._version`. Both the session builder
(:func:`pathling.context._build_spark_session`) and the CLI merge logic
(:mod:`pathling.cli.sparkconf`) import these values so the two cannot drift
apart. This module deliberately imports no PySpark, so the CLI configuration
path can reference the defaults without paying Spark's import cost.

It also hosts the pure validation and merge logic that combines a user-supplied
Spark configuration mapping with the managed defaults, shared by the CLI's
``--spark-conf`` flag and the ``spark_conf`` parameter of
:func:`pathling.context.PathlingContext.create`.

Author: John Grimes.
"""

from typing import Callable, Optional

from pathling._version import (
    __delta_version__,
    __java_version__,
    __scala_version__,
)

# The Spark configuration key holding the comma-separated Maven coordinates.
PACKAGES_KEY = "spark.jars.packages"

# The Spark configuration key holding the comma-separated SQL extension classes.
EXTENSIONS_KEY = "spark.sql.extensions"

# The Spark configuration key for the session catalog implementation.
CATALOG_KEY = "spark.sql.catalog.spark_catalog"

# The managed Maven coordinate (group:artifact) for the Pathling library runtime.
LIBRARY_RUNTIME_COORDINATE = "au.csiro.pathling:library-runtime"

# The managed Maven coordinate (group:artifact) for Delta Lake, which carries the
# Scala binary version in its artifact identifier.
DELTA_COORDINATE = f"io.delta:delta-spark_{__scala_version__}"

# The group:artifact identities of the coordinates Pathling manages, used to
# detect a user-supplied override at a different version.
MANAGED_COORDINATES = frozenset({LIBRARY_RUNTIME_COORDINATE, DELTA_COORDINATE})

# The Delta SQL extension class that Pathling always requires.
DELTA_EXTENSION = "io.delta.sql.DeltaSparkSessionExtension"

# The Delta catalog implementation that Pathling always requires.
DELTA_CATALOG = "org.apache.spark.sql.delta.catalog.DeltaCatalog"


def managed_spark_defaults() -> dict:
    """Returns the Spark configuration that Pathling always requires.

    The packages string lists both managed coordinates at the versions declared
    in :mod:`pathling._version` and retains a trailing comma, matching the
    historical inline literal. The extension and catalog are fixed Delta class
    names.

    :return: a mapping of managed Spark configuration key to value.
    """
    return {
        PACKAGES_KEY: (
            f"{LIBRARY_RUNTIME_COORDINATE}:{__java_version__},"
            f"{DELTA_COORDINATE}:{__delta_version__},"
        ),
        EXTENSIONS_KEY: DELTA_EXTENSION,
        CATALOG_KEY: DELTA_CATALOG,
    }


def validate_spark_conf(user_map: dict) -> dict:
    """Validates a user-supplied Spark configuration mapping.

    Every key must begin with ``spark.`` and every value must already be a
    string, the form Spark expects. This is the library-API counterpart of the
    CLI's :func:`pathling.cli.sparkconf.validate_and_coerce`: it drops the
    CLI-specific bits (TOML/flag scalar coercion, ``@file`` secret resolution)
    and accepts a plain ``dict[str, str]``.

    :param user_map: the user-supplied Spark configuration mapping.
    :return: the same mapping, validated.
    :raises ValueError: if a key is not prefixed ``spark.``.
    :raises TypeError: if a value is not a string.
    """
    for key, value in user_map.items():
        if not key.startswith("spark."):
            raise ValueError(
                f"Invalid Spark configuration key '{key}'. "
                "Spark configuration keys must begin with 'spark.'."
            )
        if not isinstance(value, str):
            raise TypeError(
                f"Invalid value for Spark configuration key '{key}'. "
                f"Only string values are allowed, got {type(value).__name__}."
            )
    return user_map


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
    :raises ValueError: if the value differs from the managed Delta catalog.
    """
    managed = managed_spark_defaults()[CATALOG_KEY]
    if user_value == managed:
        return None
    raise ValueError(
        f"The Spark configuration key '{key}' is managed by Pathling and must be "
        f"'{managed}'. Remove it or set it to the Delta catalog."
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

    :param user_map: the validated user Spark map.
    :param on_warning: a callback for the managed-version-override warning, or
           None to suppress it.
    :return: the effective Spark configuration to apply on top of the defaults.
    :raises ValueError: if the session catalog is set to a non-Delta value.
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
