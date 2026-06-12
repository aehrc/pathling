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

"""The ``pathling convert`` command.

Reads any supported FHIR data source and writes ndjson, Parquet, or Delta to an
output path, with save-mode control and a summary of the resource types and row
counts written.

Author: John Grimes.
"""

from pathlib import Path

import click
from rich.table import Table

from pathling.cli import session
from pathling.cli.errors import EXIT_USAGE, CliError
from pathling.cli.io import FROM_CHOICES, read_source, resolve_source
from pathling.cli.render import check_overwrite, progress_status

# The output formats convert can write.
TO_CHOICES = ("ndjson", "parquet", "delta")

# The save modes accepted on the command line.
MODE_CHOICES = ("overwrite", "error", "append", "merge")


@click.command(name="convert")
@click.argument("source")
@click.option(
    "--from",
    "from_format",
    type=click.Choice(FROM_CHOICES),
    help="Input format (auto-detected when omitted).",
)
@click.option(
    "--to",
    "to_format",
    required=True,
    type=click.Choice(TO_CHOICES),
    help="Output format.",
)
@click.option(
    "-o",
    "--output",
    "output",
    required=True,
    help="Output directory.",
)
@click.option(
    "--mode",
    default="error",
    type=click.Choice(MODE_CHOICES),
    show_default=True,
    help="Save mode for parquet/delta output; 'merge' is delta only.",
)
@click.option(
    "--overwrite",
    is_flag=True,
    help="Replace an existing output path (equivalent to --mode overwrite).",
)
@click.pass_obj
def convert(obj, source, from_format, to_format, output, mode, overwrite):
    """Convert FHIR data between formats.

    Examples:

        pathling convert data/ --to parquet -o warehouse/

        pathling convert bundles/ --from bundles --to ndjson -o out/
    """
    config = obj.config
    console = obj.console

    spec = resolve_source(source, from_format)

    effective_mode = "overwrite" if overwrite else mode
    if effective_mode == "merge" and to_format != "delta":
        raise CliError(
            "The 'merge' save mode is only valid for delta output. "
            "Use --to delta, or choose --mode overwrite|error|append.",
            exit_code=EXIT_USAGE,
        )

    output_path = Path(output)
    # Error-if-exists is the default; pre-check so the message names --overwrite.
    if effective_mode == "error":
        check_overwrite(output_path, overwrite=False)

    pc = session.create_context(config, console)
    data_source = read_source(pc, spec)

    with progress_status(
        console, f"Writing {to_format} to {output_path}...", config.verbose
    ):
        sink = data_source.write
        target = str(output_path)
        if to_format == "ndjson":
            sink.ndjson(target, save_mode=effective_mode)
        elif to_format == "parquet":
            sink.parquet(target, save_mode=effective_mode)
        else:
            sink.delta(target, save_mode=effective_mode)

    _print_summary(console, data_source)


def _print_summary(console, data_source) -> None:
    """Prints a table of the resource types written and their row counts.

    :param console: the stderr console to print the summary to.
    :param data_source: the data source that was written.
    """
    table = Table(title="Conversion summary")
    table.add_column("Resource type")
    table.add_column("Rows", justify="right")
    total = 0
    for resource_type in sorted(data_source.resource_types()):
        count = data_source.read(resource_type).count()
        total += count
        table.add_row(resource_type, str(count))
    table.caption = f"{total} rows total"
    console.print(table)
