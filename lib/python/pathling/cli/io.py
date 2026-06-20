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

"""Data source detection and reading for the Pathling command line interface.

Format is auto-detected from the contents of the input path, with an explicit
``--from`` override. Detection and validation run before any Spark session is
started so that obvious mistakes (a missing or empty path, an ambiguous
directory) fail quickly with an actionable message.

Author: John Grimes.
"""

import json
import re
from dataclasses import dataclass
from pathlib import Path
from typing import List, Optional

from pathling.cli.errors import EXIT_USAGE, CliError

# The number of leading characters of a candidate file inspected during format
# detection. A FHIR resource names its ``resourceType`` near the start of the
# object, so this is enough to recognise a Bundle without parsing the whole file.
_DETECTION_PREFIX_CHARS = 4096

# Matches a FHIR Bundle's ``resourceType`` declaration within the leading prefix.
_BUNDLE_JSON_MARKER = re.compile(r'"resourceType"\s*:\s*"Bundle"')


class SourceFormat:
    """The recognised data source formats."""

    NDJSON = "ndjson"
    BUNDLES = "bundles"
    PARQUET = "parquet"
    DELTA = "delta"
    # A single FHIR resource JSON file, used only by the ``fhirpath`` command.
    RESOURCE = "resource"


# The formats that may be supplied via ``--from`` (RESOURCE is auto-only).
FROM_CHOICES = (
    SourceFormat.NDJSON,
    SourceFormat.BUNDLES,
    SourceFormat.PARQUET,
    SourceFormat.DELTA,
)


@dataclass
class SourceSpec:
    """A resolved data source input.

    :param path: the input path; verified to exist.
    :param format: one of the :class:`SourceFormat` values.
    """

    path: Path
    format: str


def _looks_like_bundle(path: Path) -> bool:
    """Determines whether a JSON or XML file appears to contain a FHIR Bundle.

    Only a leading prefix of the file is read; a full parse is unnecessary to
    recognise a Bundle and wasteful for large files (FR-014).

    :param path: the file to inspect.
    :return: True when the file's leading prefix declares a Bundle resource.
    """
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as handle:
            head = handle.read(_DETECTION_PREFIX_CHARS)
    except OSError:
        return False
    if path.suffix == ".xml":
        return "<Bundle" in head
    return _BUNDLE_JSON_MARKER.search(head) is not None


def _describe_directory(path: Path) -> str:
    """Summarises the contents of a directory for an error message.

    :param path: the directory to describe.
    :return: a comma-separated list of the entry names, capped for brevity.
    """
    names = sorted(entry.name for entry in path.iterdir())
    if len(names) > 10:
        names = names[:10] + ["..."]
    return ", ".join(names) if names else "(empty)"


def detect_format(path: Path, allow_resource: bool = False) -> str:
    """Auto-detects the format of a data source path.

    :param path: the input path.
    :param allow_resource: when True, a single JSON file is treated as a single
           FHIR resource rather than rejected.
    :return: the detected :class:`SourceFormat` value.
    :raises CliError: when the path is missing or empty, or the format cannot
            be determined unambiguously.
    """
    if not path.exists():
        raise CliError(
            f"Input path does not exist: {path}. "
            "Check the path relative to your working directory.",
            exit_code=EXIT_USAGE,
        )

    if path.is_file():
        if allow_resource and path.suffix == ".json":
            return SourceFormat.RESOURCE
        raise CliError(
            f"Input path is a file: {path}. Data source commands expect a "
            "directory of FHIR data; pass a directory or specify --from.",
            exit_code=EXIT_USAGE,
        )

    entries = list(path.iterdir())
    if not entries:
        raise CliError(f"Input directory is empty: {path}.", exit_code=EXIT_USAGE)

    parquet_tables = [entry for entry in entries if entry.name.endswith(".parquet")]
    if parquet_tables:
        if any((table / "_delta_log").exists() for table in parquet_tables):
            return SourceFormat.DELTA
        return SourceFormat.PARQUET

    if any(entry.suffix in (".ndjson", ".jsonl") for entry in entries):
        return SourceFormat.NDJSON

    json_xml = [entry for entry in entries if entry.suffix in (".json", ".xml")]
    if json_xml:
        if _looks_like_bundle(sorted(json_xml)[0]):
            return SourceFormat.BUNDLES

    raise CliError(
        f"Could not determine the format of {path}. Found: "
        f"{_describe_directory(path)}. "
        "Specify the format with --from ndjson|bundles|parquet|delta.",
        exit_code=EXIT_USAGE,
    )


def resolve_source(
    path_str: str,
    from_format: Optional[str] = None,
    allow_resource: bool = False,
) -> SourceSpec:
    """Resolves a source path and format, validating before Spark startup.

    :param path_str: the positional source path.
    :param from_format: an explicit ``--from`` override, or None to auto-detect.
    :param allow_resource: whether a single resource JSON file is permitted.
    :return: the resolved :class:`SourceSpec`.
    :raises CliError: when the path is invalid or the format cannot be resolved.
    """
    path = Path(path_str)
    if from_format is not None:
        if not path.exists():
            raise CliError(
                f"Input path does not exist: {path}. "
                "Check the path relative to your working directory.",
                exit_code=EXIT_USAGE,
            )
        return SourceSpec(path=path, format=from_format)
    return SourceSpec(path=path, format=detect_format(path, allow_resource))


def discover_bundle_resource_types(path: Path) -> List[str]:
    """Discovers the distinct resource types contained in a directory of
    FHIR Bundles.

    :param path: the directory of bundle JSON files.
    :return: a sorted list of distinct resource type codes found in the bundle
             entries.
    :raises CliError: when no resource types can be found.
    """
    types = set()
    for entry in sorted(path.iterdir()):
        if entry.suffix != ".json":
            continue
        try:
            with open(entry, "r", encoding="utf-8") as handle:
                data = json.load(handle)
        except (OSError, ValueError):
            continue
        for bundle_entry in data.get("entry", []) or []:
            resource = bundle_entry.get("resource") or {}
            resource_type = resource.get("resourceType")
            if resource_type:
                types.add(resource_type)
    if not types:
        raise CliError(
            f"No FHIR resources were found in the bundles at {path}. "
            "Check that the directory contains FHIR Bundle JSON files."
        )
    return sorted(types)


def read_single_resource(path: Path) -> tuple:
    """Reads a single FHIR resource JSON file.

    :param path: the path to the resource file.
    :return: a tuple of (resource_type, resource_json_string).
    :raises CliError: when the file cannot be read or has no resource type.
    """
    try:
        text = path.read_text(encoding="utf-8")
        data = json.loads(text)
    except (OSError, ValueError) as exc:
        raise CliError(f"Could not read the FHIR resource at {path}: {exc}.") from exc
    resource_type = data.get("resourceType")
    if not resource_type:
        raise CliError(
            f"The file {path} does not contain a 'resourceType' and is not a "
            "FHIR resource."
        )
    return resource_type, text


def read_source(pc, spec: SourceSpec, types: Optional[List[str]] = None):
    """Reads a :class:`SourceSpec` into a Pathling ``DataSource``.

    :param pc: the :class:`PathlingContext` to read with.
    :param spec: the resolved source specification.
    :param types: the resource types to read from a Bundles source. When
           provided (non-empty), these are used directly and the driver-side
           discovery pass is skipped; when None or empty, discovery enumerates
           the types. Ignored for non-Bundles formats (FR-015).
    :return: a Pathling ``DataSource`` for the input.
    :raises CliError: when the format is not a data source format.
    """
    path = str(spec.path)
    if spec.format == SourceFormat.NDJSON:
        return pc.read.ndjson(path)
    if spec.format == SourceFormat.BUNDLES:
        resource_types = (
            list(types) if types else discover_bundle_resource_types(spec.path)
        )
        return pc.read.bundles(path, resource_types)
    if spec.format == SourceFormat.PARQUET:
        return pc.read.parquet(path)
    if spec.format == SourceFormat.DELTA:
        return pc.read.delta(path)
    raise CliError(
        f"Cannot read '{spec.format}' as a data source.", exit_code=EXIT_USAGE
    )
