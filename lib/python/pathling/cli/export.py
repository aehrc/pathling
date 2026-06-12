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

"""The ``pathling export`` command.

Performs a FHIR Bulk Data export (system, group, or patient level) with the
library's filters and SMART backend services authentication, downloading
ndjson to the output directory and reporting a summary of files and resource
counts.

Author: John Grimes.
"""

from datetime import datetime
from pathlib import Path

import click
from rich.table import Table

from pathling.cli import session
from pathling.cli.config import default_config_path, load_config_file, resolve_bulk_auth
from pathling.cli.errors import EXIT_USAGE, CliError, unwrap_java_exception
from pathling.cli.render import check_overwrite, progress_status


def _parse_since(since):
    """Parses a ``--since`` ISO 8601 timestamp with a timezone offset.

    :param since: the raw ``--since`` value, or None.
    :return: a timezone-aware datetime, or None when not given.
    :raises CliError: when the value is not ISO 8601 with a timezone.
    """
    if since is None:
        return None
    # Python 3.9's fromisoformat does not accept the 'Z' UTC suffix.
    normalised = since[:-1] + "+00:00" if since.endswith("Z") else since
    try:
        parsed = datetime.fromisoformat(normalised)
    except ValueError as exc:
        raise CliError(
            f"Could not parse --since value '{since}': {exc}. Use an ISO 8601 "
            "timestamp with a timezone, e.g. 2024-01-01T00:00:00Z.",
            exit_code=EXIT_USAGE,
        ) from exc
    if parsed.tzinfo is None:
        raise CliError(
            f"The --since value '{since}' has no timezone. Use an ISO 8601 "
            "timestamp with a timezone, e.g. 2024-01-01T00:00:00+10:00.",
            exit_code=EXIT_USAGE,
        )
    return parsed


def _build_auth_config(bulk_auth) -> dict:
    """Builds the library's auth config dict from resolved bulk auth.

    :param bulk_auth: the resolved :class:`BulkAuth`.
    :return: the auth config dict accepted by the bulk export client.
    """
    auth_config = {
        "enabled": True,
        "use_smart": True,
        "client_id": bulk_auth.client_id,
        "token_endpoint": bulk_auth.token_endpoint,
    }
    if bulk_auth.private_key_jwk:
        auth_config["private_key_jwk"] = bulk_auth.private_key_jwk
    if bulk_auth.client_secret:
        auth_config["client_secret"] = bulk_auth.client_secret
    if bulk_auth.scope:
        auth_config["scope"] = bulk_auth.scope
    return auth_config


@click.command(name="export")
@click.argument("endpoint")
@click.option("-o", "--output", "output", required=True, help="Output directory.")
@click.option("--group", "group", help="Group ID for a group-level export.")
@click.option(
    "--patient",
    "patients",
    multiple=True,
    help="Patient reference for a patient-level export (repeatable).",
)
@click.option(
    "--type", "types", multiple=True, help="Resource type to include (repeatable)."
)
@click.option(
    "--elements", "elements", multiple=True, help="Element to include (repeatable)."
)
@click.option(
    "--type-filter", "type_filters", multiple=True, help="Type filter (repeatable)."
)
@click.option(
    "--include-associated-data",
    "include_associated_data",
    multiple=True,
    help="Associated data to include (repeatable).",
)
@click.option(
    "--since", "since", help="Only include resources modified since (ISO 8601)."
)
@click.option("--timeout", "timeout", type=int, help="Timeout in seconds.")
@click.option(
    "--max-downloads",
    "max_downloads",
    type=int,
    default=10,
    show_default=True,
    help="Maximum concurrent downloads.",
)
@click.option("--client-id", "client_id", help="SMART auth client ID.")
@click.option("--token-endpoint", "token_endpoint", help="SMART auth token endpoint.")
@click.option("--scope", "scope", help="SMART auth scope.")
@click.option(
    "--private-key-jwk",
    "private_key_jwk",
    help="SMART auth private key JWK (literal, @file, or env).",
)
@click.option(
    "--client-secret",
    "client_secret",
    help="SMART auth client secret (literal, @file, or env).",
)
@click.option("--overwrite", is_flag=True, help="Replace an existing output directory.")
@click.pass_obj
def export(
    obj,
    endpoint,
    output,
    group,
    patients,
    types,
    elements,
    type_filters,
    include_associated_data,
    since,
    timeout,
    max_downloads,
    client_id,
    token_endpoint,
    scope,
    private_key_jwk,
    client_secret,
    overwrite,
):
    """Bulk export data from a FHIR server.

    Examples:

        pathling export https://server/fhir -o out/ --type Patient

        pathling export https://server/fhir -o out/ --group 123
    """
    config = obj.config
    console = obj.console

    if group and patients:
        raise CliError(
            "--group and --patient are mutually exclusive. Choose a group-level "
            "or a patient-level export, not both.",
            exit_code=EXIT_USAGE,
        )

    output_path = Path(output)
    check_overwrite(output_path, overwrite)
    since_dt = _parse_since(since)

    file_data = load_config_file(config.config_path or default_config_path())
    bulk_auth = resolve_bulk_auth(
        file_data.get("bulk-auth"),
        client_id=client_id,
        client_secret=client_secret,
        private_key_jwk=private_key_jwk,
        token_endpoint=token_endpoint,
        scope=scope,
    )
    auth_config = _build_auth_config(bulk_auth) if bulk_auth else None

    pc = session.create_context(config, console)

    with progress_status(
        console,
        f"Exporting from {endpoint} (kick-off, polling, download)...",
        config.verbose,
    ):
        try:
            data_source = pc.read.bulk(
                fhir_endpoint_url=endpoint,
                output_dir=str(output_path),
                overwrite=True,
                group_id=group or None,
                patients=list(patients) or None,
                since=since_dt,
                types=list(types) or None,
                elements=list(elements) or None,
                include_associated_data=list(include_associated_data) or None,
                type_filters=list(type_filters) or None,
                timeout=timeout,
                max_concurrent_downloads=max_downloads,
                auth_config=auth_config,
            )
        except Exception as exc:  # noqa: BLE001 - re-raised as a friendly error.
            if bulk_auth is not None:
                raise CliError(
                    f"Authentication failed using {bulk_auth.mechanism} against "
                    f"{bulk_auth.token_endpoint}: {unwrap_java_exception(exc)}. "
                    "Check the client ID, credential, and token endpoint."
                ) from exc
            raise

    _print_summary(console, data_source, output_path)


def _print_summary(console, data_source, output_path) -> None:
    """Prints a summary of the files written and resource counts.

    :param console: the stderr console.
    :param data_source: the ndjson data source for the exported files.
    :param output_path: the output directory the files were written to.
    """
    table = Table(title="Export summary")
    table.add_column("Resource type")
    table.add_column("Rows", justify="right")
    total = 0
    for resource_type in sorted(data_source.resource_types()):
        count = data_source.read(resource_type).count()
        total += count
        table.add_row(resource_type, str(count))

    files = sorted(p.name for p in output_path.glob("*.ndjson"))
    table.caption = f"{len(files)} files, {total} rows downloaded to {output_path}"
    console.print(table)
