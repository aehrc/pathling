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

import click

from pathling.cli import session
from pathling.cli.render import progress_status


@click.command(name="import-snomed")
@click.argument("source")
@click.argument("storage_path")
@click.option(
    "--edition-uri", "edition_uri", help="Override the SNOMED edition/version URI."
)
@click.pass_obj
def import_snomed(obj, source, storage_path, edition_uri):
    """Import a SNOMED CT RF2 snapshot release into a local terminology store.

    Example:

        pathling import-snomed /data/rf2.zip /data/tx-store
    """
    config = obj.config
    console = obj.console
    pc = session.create_context(config, console)
    with progress_status(console, "Importing SNOMED CT...", config.verbose):
        pc.import_snomed(source, storage_path, edition_uri)
    click.echo(f"Imported SNOMED CT from {source} into {storage_path}")


@click.command(name="import-fhir-terminology")
@click.argument("source")
@click.argument("storage_path")
@click.pass_obj
def import_fhir_terminology(obj, source, storage_path):
    """Import FHIR CodeSystem, ValueSet, and ConceptMap resources into a store.

    The source may be a JSON file, a directory of JSON files, or a FHIR NPM
    package (.tgz).

    Example:

        pathling import-fhir-terminology /data/hl7.terminology.tgz /data/tx-store
    """
    config = obj.config
    console = obj.console
    pc = session.create_context(config, console)
    with progress_status(console, "Importing FHIR terminology...", config.verbose):
        pc.import_fhir_terminology(source, storage_path)
    click.echo(f"Imported FHIR terminology from {source} into {storage_path}")


#: The terminology import commands registered by the root command group.
IMPORT_COMMANDS = (import_snomed, import_fhir_terminology)
