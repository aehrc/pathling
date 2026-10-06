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

"""The Pathling terminology import commands.

Each command creates a session, imports terminology content into a local store
through the library API, and reports progress and a completion summary. Import
runs as Spark jobs; per-stage progress is logged by the library and, in verbose
mode, streamed to stderr.

Author: John Grimes.
"""

from __future__ import annotations

import dataclasses
from typing import TYPE_CHECKING, Optional

import click
from rich.console import Console

from pathling.cli import session
from pathling.cli.config import CliConfig
from pathling.cli.errors import EXIT_USAGE, CliError, unwrap_java_exception
from pathling.cli.render import progress_status

if TYPE_CHECKING:
    from pyspark.sql import Row, SparkSession

    from pathling import PathlingContext
    from pathling.cli.main import CliContext

# The name of the manifest table within a terminology store.
MANIFEST_TABLE = "manifest"

# The fixed phrase the library uses when a package does not match the checksum
# its registry publishes. The library must not name a CLI flag, so the hint
# naming --no-verify is added here, keyed off this phrase.
MISMATCH_PHRASE = "does not match the registry checksum"

# The hint appended to a checksum mismatch failure.
MISMATCH_HINT = "Re-run with --no-verify to import it anyway."


def _import_context(config: CliConfig, console: Console) -> PathlingContext:
    """Creates a context for importing into a store.

    The import commands populate a store rather than query terminology, so the
    context must not enter local mode: doing so would eagerly validate the
    target store and reject a not-yet-created one, breaking the first import
    into a configured store (FR-010). Clearing ``tx_store`` restores the plain
    context used before local mode existed; the resolved import target is passed
    to the import methods directly.

    :param config: the resolved CLI configuration.
    :param console: the stderr console for the status spinner.
    :return: a configured :class:`PathlingContext` not bound to any store.
    """
    return session.create_context(dataclasses.replace(config, tx_store=None), console)


def _resolve_storage_path(config: CliConfig, storage_path: Optional[str]) -> str:
    """Resolves the target store path, falling back to the configured store.

    The explicit ``STORAGE_PATH`` positional wins; otherwise the configured
    ``tx-store.path`` is used. When neither is available the command is a usage
    error naming both mechanisms (FR-010).

    :param config: the resolved CLI configuration.
    :param storage_path: the ``STORAGE_PATH`` positional, or None when omitted.
    :return: the resolved store path.
    :raises CliError: with EXIT_USAGE when no path is available from either
            source.
    """
    if storage_path is not None:
        return storage_path
    if config.tx_store is not None:
        return config.tx_store.path
    raise CliError(
        "No storage path given. Provide the STORAGE_PATH argument, or configure "
        "'tx-store.path' (or the --tx-store flag).",
        exit_code=EXIT_USAGE,
    )


def read_latest_import_row(
    spark: SparkSession, store: str, source: str
) -> Optional[Row]:
    """Reads the most recent manifest row recorded for a source.

    The store manifest carries the provenance of each import: the source hash,
    the package identity, and the outcome of the registry check. The latest row
    for this source describes the import that has just completed.

    A failure to read the manifest yields None rather than an error: the import
    has already committed, and reporting less detail is better than failing a
    command whose work succeeded.

    :param spark: the Spark session to read with.
    :param store: the terminology store path.
    :param source: the import source, matched against the manifest ``source``
           column.
    :return: the latest manifest row for the source, or None when there is none
             or the manifest cannot be read.
    """
    from pyspark.sql import functions as F

    manifest_path = f"{store.rstrip('/')}/{MANIFEST_TABLE}"
    try:
        rows = (
            spark.read.format("delta")
            .load(manifest_path)
            .filter(F.col("source") == source)
            .orderBy(F.col("imported_at").desc())
            .take(1)
        )
    except Exception:  # noqa: BLE001 - detail only; the import already committed.
        return None
    return rows[0] if rows else None


def format_import_summary(
    command_noun: str, source: str, store: str, row: Optional[Row]
) -> str:
    """Renders the completion line for a finished import.

    The line always names what was imported, from where, and into where. Where
    the manifest records provenance it is reported in parentheses: the source
    hash, and for a package the identity and the outcome of the registry check.

    :param command_noun: what was imported, such as ``"SNOMED CT"``.
    :param source: the import source as given on the command line.
    :param store: the resolved store path.
    :param row: the manifest row describing the import, or None when it could
           not be read.
    :return: the completion line.
    """
    summary = f"Imported {command_noun} from {source} into {store}"
    if row is None:
        return summary
    values = row.asDict() if hasattr(row, "asDict") else dict(row)
    sha256 = values.get("source_sha256")
    if sha256 is None:
        # A directory source has no single set of bytes to fingerprint.
        return summary
    status = values.get("package_verification")
    name = values.get("package_name")
    version = values.get("package_version")
    identity = f"{name} {version}" if name and version else None
    if status == "verified":
        registry = values.get("package_registry")
        detail = f"verified {identity} against {registry}"
    elif status == "unverified":
        subject = identity if identity else "package"
        detail = (
            f"{subject} not verified against a registry; re-run with --verbose "
            "for the reason"
        )
    elif status == "skipped":
        detail = (
            f"{identity}, verification skipped"
            if identity
            else "package verification skipped"
        )
    else:
        # Not a package: the hash alone is the provenance.
        return f"{summary} (sha256 {sha256})"
    return f"{summary} ({detail}; sha256 {sha256})"


@click.command(name="import-snomed")
@click.argument("source")
@click.argument("storage_path", required=False)
@click.option(
    "--edition-uri", "edition_uri", help="Override the SNOMED edition/version URI."
)
@click.option(
    "--dense-id-order",
    "dense_id_order",
    type=click.Choice(["code-order", "pre-order"]),
    default="code-order",
    show_default=True,
    help=(
        "How internal concept identifiers are assigned. 'pre-order' makes the "
        "hierarchy index materially smaller, in exchange for identifiers that "
        "shift more between releases."
    ),
)
@click.option(
    "--default-dialect",
    "default_dialect",
    help=(
        "The dialect whose preferred synonyms become the stored display: a tag "
        "such as 'en-GB', or a language reference set identifier. Falls back to "
        "the 'tx-store.default-dialect' config key; chosen from the release "
        "when neither is set."
    ),
)
@click.pass_obj
def import_snomed(
    obj: CliContext,
    source: str,
    storage_path: Optional[str],
    edition_uri: Optional[str],
    dense_id_order: str,
    default_dialect: Optional[str],
) -> None:
    """Import a SNOMED CT RF2 snapshot release into a local terminology store.

    STORAGE_PATH may be omitted when 'tx-store.path' (or --tx-store) is set.

    Example:

        pathling import-snomed /data/rf2.zip /data/tx-store
    """
    config = obj.config
    console = obj.console
    resolved_path = _resolve_storage_path(config, storage_path)
    resolved_dialect = (
        default_dialect
        if default_dialect is not None
        else (config.tx_store.default_dialect if config.tx_store is not None else None)
    )
    pc = _import_context(config, console)
    with progress_status(console, "Importing SNOMED CT...", config.verbose):
        pc.import_snomed(
            source, resolved_path, edition_uri, dense_id_order, resolved_dialect
        )
    row = read_latest_import_row(pc.spark, resolved_path, source)
    click.echo(format_import_summary("SNOMED CT", source, resolved_path, row))


@click.command(name="import-fhir-terminology")
@click.argument("source")
@click.argument("storage_path", required=False)
@click.option(
    "--no-verify",
    "no_verify",
    is_flag=True,
    help=(
        "Do not check a FHIR NPM package against its registry's published "
        "checksum. Use for offline imports or packages not published to a "
        "registry."
    ),
)
@click.option(
    "--package-registry",
    "package_registry",
    metavar="URL",
    help=(
        "The FHIR package registry to check a package against. Falls back to "
        "the 'package-registry' config key, then https://packages.fhir.org."
    ),
)
@click.pass_obj
def import_fhir_terminology(
    obj: CliContext,
    source: str,
    storage_path: Optional[str],
    no_verify: bool,
    package_registry: Optional[str],
) -> None:
    """Import FHIR CodeSystem, ValueSet, and ConceptMap resources into a store.

    The source may be a JSON file, a directory of JSON files, or a FHIR NPM
    package (.tgz). A package is checked against the checksum its registry
    publishes; a package that does not match fails the import and leaves the
    store unchanged. STORAGE_PATH may be omitted when 'tx-store.path' (or
    --tx-store) is set.

    Example:

        pathling import-fhir-terminology /data/hl7.terminology.tgz /data/tx-store
    """
    config = obj.config
    console = obj.console
    resolved_path = _resolve_storage_path(config, storage_path)
    # The flag wins over the config key; with neither set no registry is named
    # and the library default applies, so the CLI never holds that URL.
    resolved_registry = (
        package_registry if package_registry is not None else config.package_registry
    )
    pc = _import_context(config, console)
    with progress_status(console, "Importing FHIR terminology...", config.verbose):
        try:
            pc.import_fhir_terminology(
                source,
                resolved_path,
                verify_package=not no_verify,
                package_registry=resolved_registry,
            )
        except Exception as exc:  # noqa: BLE001 - enrich a mismatch failure.
            root_message = unwrap_java_exception(exc)
            if MISMATCH_PHRASE not in root_message:
                raise
            raise CliError(f"{root_message} {MISMATCH_HINT}") from exc
    row = read_latest_import_row(pc.spark, resolved_path, source)
    click.echo(format_import_summary("FHIR terminology", source, resolved_path, row))


#: The terminology import commands registered by the root command group.
IMPORT_COMMANDS = (import_snomed, import_fhir_terminology)
