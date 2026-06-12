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

"""Integration and unit tests for the ``pathling export`` command.

The export integration tests run against the same mock bulk data server used by
the library test suite. Authentication resolution is unit tested without Spark.

Author: John Grimes.
"""

import os

import pytest
from flask import Response, request

from pathling.cli.config import resolve_bulk_auth
from pathling.cli.errors import CliError
from pathling.cli.main import cli


@pytest.fixture
def bulk_server(mock_server, test_data_dir):
    """A mock bulk data server serving Patient and Condition ndjson."""
    ndjson_dir = os.path.join(test_data_dir, "ndjson")
    captured = {}

    @mock_server.route("/fhir/$export", methods=["GET"])
    def export_route():
        captured["args"] = dict(request.args)
        resp = Response(status=202)
        resp.headers["content-location"] = mock_server.url("/pool")
        return resp

    @mock_server.route("/pool", methods=["GET"])
    def pool():
        return dict(
            transactionTime="1970-01-01T00:00:00.000Z",
            output=[
                dict(type=rt, url=mock_server.url(f"/download/{rt}"), count=1)
                for rt in ("Patient", "Condition")
            ],
        )

    @mock_server.route("/download/<resource>", methods=["GET"])
    def download(resource):
        with open(os.path.join(ndjson_dir, f"{resource}.ndjson")) as handle:
            return handle.read()

    mock_server.captured = captured
    return mock_server


# ========== System-level export ==========


def test_system_export_with_summary(runner, patched_context, bulk_server, tmp_path):
    """A system-level export downloads files and prints a summary."""
    out = tmp_path / "export"

    with bulk_server.run():
        result = runner.invoke(
            cli, ["export", bulk_server.url("/fhir"), "-o", str(out)]
        )

    assert result.exit_code == 0, result.stderr
    assert any(out.glob("Patient*.ndjson"))
    assert "Patient" in result.stderr


def test_type_and_since_passed_through(runner, patched_context, bulk_server, tmp_path):
    """The --type and --since options are sent on the export request."""
    out = tmp_path / "export"

    with bulk_server.run():
        result = runner.invoke(
            cli,
            [
                "export",
                bulk_server.url("/fhir"),
                "-o",
                str(out),
                "--type",
                "Patient",
                "--since",
                "2020-01-01T00:00:00Z",
            ],
        )

    assert result.exit_code == 0, result.stderr
    args = bulk_server.captured["args"]
    assert "Patient" in args.get("_type", "")
    assert "_since" in args


# ========== Error paths ==========


def test_group_and_patient_mutually_exclusive(runner, patched_context, tmp_path):
    """--group and --patient together is a usage error naming both flags."""
    result = runner.invoke(
        cli,
        [
            "export",
            "http://localhost:9/fhir",
            "-o",
            str(tmp_path / "out"),
            "--group",
            "123",
            "--patient",
            "Patient/1",
        ],
    )

    assert result.exit_code == 2
    assert "--group" in result.stderr and "--patient" in result.stderr


def test_since_parse_error(runner, patched_context, tmp_path):
    """An unparseable --since shows the expected ISO 8601 form."""
    result = runner.invoke(
        cli,
        [
            "export",
            "http://localhost:9/fhir",
            "-o",
            str(tmp_path / "out"),
            "--since",
            "yesterday",
        ],
    )

    assert result.exit_code == 2
    assert "ISO 8601" in result.stderr


def test_existing_output_without_overwrite(runner, patched_context, tmp_path):
    """An existing output directory without --overwrite fails showing the flag."""
    out = tmp_path / "export"
    out.mkdir()

    result = runner.invoke(cli, ["export", "http://localhost:9/fhir", "-o", str(out)])

    assert result.exit_code == 1
    assert "--overwrite" in result.stderr


def test_auth_failure_names_mechanism(runner, patched_context, bulk_server, tmp_path):
    """An authentication failure names the credential mechanism attempted."""
    out = tmp_path / "export"

    @bulk_server.route("/token", methods=["POST"])
    def token():
        return Response(status=401)

    with bulk_server.run():
        result = runner.invoke(
            cli,
            [
                "export",
                bulk_server.url("/fhir"),
                "-o",
                str(out),
                "--client-id",
                "my-client",
                "--token-endpoint",
                bulk_server.url("/token"),
                "--client-secret",
                "shh",
            ],
        )

    assert result.exit_code == 1
    assert (
        "authenticat" in result.stderr.lower()
        or "client secret" in result.stderr.lower()
    )


# ========== Auth resolution (no Spark) ==========


def test_resolve_bulk_auth_secret_from_file(tmp_path):
    """A client secret @file reference is read for bulk auth."""
    secret_file = tmp_path / "secret.txt"
    secret_file.write_text("file-secret\n")

    auth = resolve_bulk_auth(
        None,
        client_id="c",
        token_endpoint="https://auth/token",
        client_secret=f"@{secret_file}",
    )

    assert auth.client_secret == "file-secret"
    assert auth.mechanism == "a client secret"


def test_resolve_bulk_auth_secret_from_env():
    """A client secret falls back to the environment variable."""
    auth = resolve_bulk_auth(
        None,
        client_id="c",
        token_endpoint="https://auth/token",
        env={"PATHLING_CLIENT_SECRET": "env-secret"},
    )

    assert auth.client_secret == "env-secret"


def test_resolve_bulk_auth_requires_token_endpoint():
    """Bulk auth without a token endpoint is a usage error."""
    with pytest.raises(CliError) as exc_info:
        resolve_bulk_auth(None, client_id="c", client_secret="shh")

    assert exc_info.value.exit_code == 2
    assert "token endpoint" in exc_info.value.message.lower()


def test_resolve_bulk_auth_exactly_one_credential():
    """Providing both a secret and a JWK is a usage error."""
    with pytest.raises(CliError) as exc_info:
        resolve_bulk_auth(
            None,
            client_id="c",
            token_endpoint="https://auth/token",
            client_secret="shh",
            private_key_jwk="{...}",
        )

    assert exc_info.value.exit_code == 2


def test_resolve_bulk_auth_none_without_client_id():
    """No client ID means no authentication is configured."""
    assert resolve_bulk_auth(None) is None


def test_resolve_bulk_auth_from_config_table():
    """Bulk auth is resolved from the [bulk-auth] config table."""
    table = {
        "client-id": "table-client",
        "token-endpoint": "https://auth/token",
        "client-secret": "table-secret",
    }

    auth = resolve_bulk_auth(table)

    assert auth.client_id == "table-client"
    assert auth.client_secret == "table-secret"
