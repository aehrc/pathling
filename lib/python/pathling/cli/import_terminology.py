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
from pathling.cli.errors import EXIT_USAGE, CliError
from pathling.cli.render import progress_status

if TYPE_CHECKING:
    from pathling import PathlingContext
    from pathling.cli.main import CliContext


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
        "such as 'en-GB', or a language reference set identifier. Chosen from "
        "the release when omitted."
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
    pc = _import_context(config, console)
    with progress_status(console, "Importing SNOMED CT...", config.verbose):
        pc.import_snomed(
            source, resolved_path, edition_uri, dense_id_order, default_dialect
        )
    click.echo(f"Imported SNOMED CT from {source} into {resolved_path}")


@click.command(name="import-fhir-terminology")
@click.argument("source")
@click.argument("storage_path", required=False)
@click.pass_obj
def import_fhir_terminology(
    obj: CliContext, source: str, storage_path: Optional[str]
) -> None:
    """Import FHIR CodeSystem, ValueSet, and ConceptMap resources into a store.

    The source may be a JSON file, a directory of JSON files, or a FHIR NPM
    package (.tgz). STORAGE_PATH may be omitted when 'tx-store.path' (or
    --tx-store) is set.

    Example:

        pathling import-fhir-terminology /data/hl7.terminology.tgz /data/tx-store
    """
    config = obj.config
    console = obj.console
    resolved_path = _resolve_storage_path(config, storage_path)
    pc = _import_context(config, console)
    with progress_status(console, "Importing FHIR terminology...", config.verbose):
        pc.import_fhir_terminology(source, resolved_path)
    click.echo(f"Imported FHIR terminology from {source} into {resolved_path}")


#: The terminology import commands registered by the root command group.
IMPORT_COMMANDS = (import_snomed, import_fhir_terminology)
