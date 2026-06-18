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

"""The ``pathling view`` command.

Executes a SQL on FHIR ViewDefinition (a JSON file or inline string) against a
data source and emits the tabular result in the requested format, optionally
restricting the resources processed with a FHIR search ``--filter``.

Author: John Grimes.
"""

import json
from pathlib import Path

import click

from pathling.cli import session
from pathling.cli.errors import EXIT_USAGE, CliError
from pathling.cli.io import FROM_CHOICES, read_source, resolve_source
from pathling.cli.render import (
    output_options,
    progress_status,
    resolve_output,
    write_output,
)


def _load_view(view_path, view_json) -> tuple:
    """Loads and minimally validates a ViewDefinition.

    :param view_path: the ``--view`` file path, or None.
    :param view_json: the ``--view-json`` inline string, or None.
    :return: a tuple of (view_json_string, resource_type).
    :raises CliError: when neither or both sources are given, or the JSON is
            malformed or missing a resource.
    """
    if view_path and view_json:
        raise CliError(
            "Provide either --view or --view-json, not both.", exit_code=EXIT_USAGE
        )
    if not view_path and not view_json:
        raise CliError(
            "A ViewDefinition is required. Pass --view <path> or --view-json '<json>'.",
            exit_code=EXIT_USAGE,
        )

    if view_path:
        path = Path(view_path)
        if not path.exists():
            raise CliError(
                f"ViewDefinition file does not exist: {path}.", exit_code=EXIT_USAGE
            )
        text = path.read_text(encoding="utf-8")
        origin = str(path)
    else:
        text = view_json
        origin = "the inline --view-json value"

    try:
        parsed = json.loads(text)
    except json.JSONDecodeError as exc:
        raise CliError(
            f"The ViewDefinition in {origin} is not valid JSON: {exc}. "
            "Check for a missing comma, brace, or quote near that position."
        ) from exc

    resource_type = parsed.get("resource")
    if not resource_type:
        raise CliError(
            f"The ViewDefinition in {origin} has no 'resource'. A view must name "
            'the resource it is based on, e.g. {"resource": "Patient", ...}.'
        )
    return text, resource_type


@click.command(name="view")
@click.argument("source")
@click.option(
    "--from",
    "from_format",
    type=click.Choice(FROM_CHOICES),
    help="Input format (auto-detected when omitted).",
)
@click.option("--view", "view_path", help="Path to a ViewDefinition JSON file.")
@click.option("--view-json", "view_json", help="Inline ViewDefinition JSON string.")
@click.option(
    "--filter", "filter_expr", help="FHIR search expression to restrict rows."
)
@output_options
@click.pass_obj
def view(
    obj,
    source,
    from_format,
    view_path,
    view_json,
    filter_expr,
    output_format,
    output,
    limit,
    overwrite,
    departition,
):
    """Run a SQL on FHIR ViewDefinition against a data source.

    \b
    See the ViewDefinition specification:
    https://sql-on-fhir.org/ig/StructureDefinition-ViewDefinition.html

    Examples:

        pathling view data/ --view patients.json

        pathling view data/ --view patients.json --format csv

        pathling view data/ --view-json '{"resource":"Patient",...}' -o out.parquet
    """
    config = obj.config
    console = obj.console

    spec = resolve_source(source, from_format)
    view_text, resource_type = _load_view(view_path, view_json)
    output_spec = resolve_output(output, output_format, limit, overwrite, departition)

    pc = session.create_context(config, console)
    # The view already knows its single subject resource type, so pass it to the
    # Bundles reader to avoid a redundant driver-side discovery pass (FR-015).
    data_source = read_source(pc, spec, types=[resource_type])

    if filter_expr:
        resources = data_source.read(resource_type)
        filter_column = pc.search_to_column(resource_type, filter_expr)
        filtered = resources.filter(filter_column)
        data_source = pc.read.datasets({resource_type: filtered})

    with progress_status(console, "Running view...", config.verbose):
        result = data_source.view(json=view_text)
        write_output(result, output_spec, console)
