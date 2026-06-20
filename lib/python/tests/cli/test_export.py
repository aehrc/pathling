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

import io as _io
import os

import pytest
from flask import Response, request
from rich.console import Console

from pathling.cli.config import resolve_bulk_auth
from pathling.cli.errors import CliError
from pathling.cli.main import cli


class _FakeReadable:
    """A stand-in dataset exposing the row ``count`` the summary may read."""

    def count(self):
        return 1


class _FakeDataSource:
    """A stand-in bulk data source for summary printing without Spark."""

    def resource_types(self):
        return ["Patient"]

    def read(self, resource_type):
        return _FakeReadable()


class _FakeReader:
    """A stand-in ``pc.read`` whose ``bulk`` delegates to a supplied callback."""

    def __init__(self, on_bulk):
        self._on_bulk = on_bulk

    def bulk(self, **kwargs):
        return self._on_bulk(kwargs)


class _FakePc:
    """A minimal stand-in :class:`PathlingContext` for the export command."""

    def __init__(self, on_bulk):
        self.read = _FakeReader(on_bulk)


def _patch_bulk(monkeypatch, on_bulk):
    """Routes ``export`` at a fake context whose ``read.bulk`` runs ``on_bulk``.

    :param monkeypatch: the pytest monkeypatch fixture.
    :param on_bulk: a callback ``(kwargs) -> data_source`` (may raise).
    :return: the fake context.
    """
    pc = _FakePc(on_bulk)
    monkeypatch.setattr(
        "pathling.cli.session.create_context", lambda config, console=None: pc
    )
    return pc


# ========== Failure classification (US1, no Spark) ==========


def test_non_auth_failure_reports_root_cause(runner, monkeypatch, tmp_path):
    """A non-auth failure under configured auth reports the root cause and does
    not claim authentication failed (FR-001)."""

    def boom(_kwargs):
        raise RuntimeError("HTTP 500 Internal Server Error during download")

    _patch_bulk(monkeypatch, boom)
    out = tmp_path / "export"

    result = runner.invoke(
        cli,
        [
            "export",
            "https://server/fhir",
            "-o",
            str(out),
            "--client-id",
            "c",
            "--token-endpoint",
            "https://auth/token",
            "--client-secret",
            "shh",
        ],
    )

    assert result.exit_code == 1
    assert "500" in result.stderr
    assert "authentication failed" not in result.stderr.lower()


def test_auth_failure_reports_auth_message(runner, monkeypatch, tmp_path):
    """A genuine auth failure names the mechanism and token endpoint (FR-001)."""

    def boom(_kwargs):
        # The real SMART backend-services setup failure surfaces this message.
        raise RuntimeError("Failed to retrieve SMART configuration")

    _patch_bulk(monkeypatch, boom)
    out = tmp_path / "export"

    result = runner.invoke(
        cli,
        [
            "export",
            "https://server/fhir",
            "-o",
            str(out),
            "--client-id",
            "c",
            "--token-endpoint",
            "https://auth/token",
            "--client-secret",
            "shh",
        ],
    )

    assert result.exit_code == 1
    assert "authentication failed" in result.stderr.lower()
    assert "https://auth/token" in result.stderr
    assert "a client secret" in result.stderr


def test_export_resolves_bulk_auth_from_resolved_config(runner, monkeypatch, tmp_path):
    """Export resolves bulk auth from the resolved config's [bulk-auth] table,
    not by re-reading a config file path (FR-003)."""
    captured = {}

    def spy_resolve(file_bulk_auth, **_kwargs):
        captured["table"] = file_bulk_auth
        return None

    monkeypatch.setattr("pathling.cli.export.resolve_bulk_auth", spy_resolve)

    # Export must resolve from the already-parsed config, never re-read a file.
    def _fail_if_reread(*_args, **_kwargs):
        raise AssertionError("export must not re-read a config file")

    monkeypatch.setattr(
        "pathling.cli.export.load_config_file", _fail_if_reread, raising=False
    )

    def ok_bulk(kwargs):
        captured["auth_config"] = kwargs.get("auth_config")
        return _FakeDataSource()

    _patch_bulk(monkeypatch, ok_bulk)

    config_file = tmp_path / "config.toml"
    config_file.write_text(
        "[bulk-auth]\n"
        'client-id = "table-client"\n'
        'token-endpoint = "https://auth/token"\n'
        'client-secret = "shh"\n',
        encoding="utf-8",
    )
    out = tmp_path / "export"

    result = runner.invoke(
        cli,
        ["--config", str(config_file), "export", "https://server/fhir", "-o", str(out)],
    )

    assert result.exit_code == 0, result.stderr
    # The table passed to resolution is the resolved config's [bulk-auth] table.
    assert captured["table"] == {
        "client-id": "table-client",
        "token-endpoint": "https://auth/token",
        "client-secret": "shh",
    }
    # No auth was configured by the spy, so the export ran unauthenticated.
    assert captured["auth_config"] is None


class _SummaryFakeDataSource:
    """A data source whose ``read`` raises, to catch any per-type count."""

    def resource_types(self):
        return ["Condition", "Patient"]

    def read(self, resource_type):
        raise AssertionError("the summary must not read per-type counts")


def test_export_summary_reports_files_types_path_without_count(tmp_path):
    """The export summary reports the file count, resource types, and output
    path without a per-type row count (FR-013)."""
    from pathling.cli.export import _print_summary

    out = tmp_path / "export"
    out.mkdir()
    (out / "Patient.0000.ndjson").write_text("{}\n", encoding="utf-8")
    (out / "Condition.0000.ndjson").write_text("{}\n", encoding="utf-8")

    buffer = _io.StringIO()
    console = Console(file=buffer, width=200, highlight=False)

    _print_summary(console, _SummaryFakeDataSource(), out)

    rendered = buffer.getvalue()
    assert "Patient" in rendered
    assert "Condition" in rendered
    assert "2 files" in rendered
    assert str(out) in rendered


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
