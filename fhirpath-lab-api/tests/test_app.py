#  Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
#  Organisation (CSIRO) ABN 41 687 119 230.
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.

"""Tests for the Flask application endpoints.

These tests use a mock PathlingContext to avoid Spark dependencies.

Author: John Grimes
"""

import json
from unittest.mock import MagicMock

import pytest

from fhirpath_lab_api.app import create_app


@pytest.fixture()
def mock_context():
    """Creates a mock PathlingContext for testing."""
    ctx = MagicMock()
    ctx.version.return_value = "9.3.1"
    ctx.evaluate_fhirpath.return_value = {
        "results": [{"type": "string", "value": "Smith"}],
        "expectedReturnType": "string",
    }
    return ctx


@pytest.fixture()
def client(mock_context):
    """Creates a Flask test client with a mock context."""
    app = create_app(pathling_context=mock_context)
    app.config["TESTING"] = True
    with app.test_client() as client:
        yield client


@pytest.fixture()
def valid_request_body():
    """Returns a valid FHIR Parameters request body."""
    return {
        "resourceType": "Parameters",
        "parameter": [
            {"name": "expression", "valueString": "name.family"},
            {
                "name": "resource",
                "resource": {"resourceType": "Patient", "id": "example"},
            },
        ],
    }


# ========== Health check tests ==========


def test_healthcheck_returns_200(client):
    """The health check endpoint returns 200 with status ok."""
    response = client.get("/healthcheck")
    assert response.status_code == 200
    data = json.loads(response.data)
    assert data["status"] == "ok"


# ========== POST /$fhirpath-r4 tests ==========


def test_successful_evaluation(client, mock_context, valid_request_body):
    """A valid request returns a FHIR Parameters response with results."""
    response = client.post(
        "/$fhirpath-r4",
        data=json.dumps(valid_request_body),
        content_type="application/json",
    )

    assert response.status_code == 200
    assert response.content_type == "application/fhir+json"

    data = json.loads(response.data)
    assert data["resourceType"] == "Parameters"

    # Verify the evaluator was called correctly.
    mock_context.evaluate_fhirpath.assert_called_once_with(
        resource_type="Patient",
        resource_json=json.dumps({"resourceType": "Patient", "id": "example"}),
        fhirpath_expression="name.family",
        context_expression=None,
        variables=None,
    )


def test_missing_expression_returns_400(client):
    """A request without expression returns 400 with OperationOutcome."""
    body = {
        "resourceType": "Parameters",
        "parameter": [
            {
                "name": "resource",
                "resource": {"resourceType": "Patient", "id": "example"},
            },
        ],
    }
    response = client.post(
        "/$fhirpath-r4",
        data=json.dumps(body),
        content_type="application/json",
    )

    assert response.status_code == 400
    data = json.loads(response.data)
    assert data["resourceType"] == "OperationOutcome"
    assert "expression" in data["issue"][0]["details"]["text"]


def test_missing_resource_returns_400(client):
    """A request without resource returns 400 with OperationOutcome."""
    body = {
        "resourceType": "Parameters",
        "parameter": [
            {"name": "expression", "valueString": "name.family"},
        ],
    }
    response = client.post(
        "/$fhirpath-r4",
        data=json.dumps(body),
        content_type="application/json",
    )

    assert response.status_code == 400
    data = json.loads(response.data)
    assert data["resourceType"] == "OperationOutcome"
    assert "resource" in data["issue"][0]["details"]["text"]


def test_malformed_json_returns_400(client):
    """A request with malformed JSON returns 400."""
    response = client.post(
        "/$fhirpath-r4",
        data="not valid json{",
        content_type="application/json",
    )

    assert response.status_code == 400
    data = json.loads(response.data)
    assert data["resourceType"] == "OperationOutcome"


def test_empty_body_returns_400(client):
    """A request with an empty body returns 400."""
    response = client.post(
        "/$fhirpath-r4",
        data="",
        content_type="application/json",
    )

    assert response.status_code == 400


def test_non_parameters_resource_returns_400(client):
    """A request with a non-Parameters resource returns 400."""
    body = {"resourceType": "Patient", "id": "example"}
    response = client.post(
        "/$fhirpath-r4",
        data=json.dumps(body),
        content_type="application/json",
    )

    assert response.status_code == 400
    data = json.loads(response.data)
    assert data["resourceType"] == "OperationOutcome"
    assert "Parameters" in data["issue"][0]["details"]["text"]


def test_evaluation_error_returns_500(client, mock_context, valid_request_body):
    """An evaluation error returns 500 with OperationOutcome."""
    mock_context.evaluate_fhirpath.side_effect = RuntimeError("Evaluation failed")

    response = client.post(
        "/$fhirpath-r4",
        data=json.dumps(valid_request_body),
        content_type="application/json",
    )

    assert response.status_code == 500
    data = json.loads(response.data)
    assert data["resourceType"] == "OperationOutcome"
    assert "error" in data["issue"][0]["severity"]


def test_response_includes_evaluator_string(client, valid_request_body):
    """The response includes the evaluator identification string."""
    response = client.post(
        "/$fhirpath-r4",
        data=json.dumps(valid_request_body),
        content_type="application/json",
    )

    data = json.loads(response.data)
    params_part = data["parameter"][0]
    evaluator = next(p for p in params_part["part"] if p["name"] == "evaluator")
    assert "Pathling" in evaluator["valueString"]
    assert "R4" in evaluator["valueString"]


def test_cors_headers_present(client, valid_request_body):
    """CORS headers are present in responses."""
    response = client.post(
        "/$fhirpath-r4",
        data=json.dumps(valid_request_body),
        content_type="application/json",
        headers={"Origin": "https://fhirpath-lab.com"},
    )

    assert response.headers.get("Access-Control-Allow-Origin") is not None


def test_preflight_cors_request(client):
    """OPTIONS preflight request returns CORS headers."""
    response = client.options(
        "/$fhirpath-r4",
        headers={
            "Origin": "https://fhirpath-lab.com",
            "Access-Control-Request-Method": "POST",
            "Access-Control-Request-Headers": "Content-Type",
        },
    )

    assert response.status_code == 200
    assert response.headers.get("Access-Control-Allow-Origin") is not None


def test_context_expression_passed_to_evaluator(client, mock_context):
    """A context expression is passed through to the evaluator."""
    body = {
        "resourceType": "Parameters",
        "parameter": [
            {"name": "expression", "valueString": "given.first()"},
            {
                "name": "resource",
                "resource": {"resourceType": "Patient", "id": "example"},
            },
            {"name": "context", "valueString": "name"},
        ],
    }
    response = client.post(
        "/$fhirpath-r4",
        data=json.dumps(body),
        content_type="application/json",
    )

    assert response.status_code == 200
    mock_context.evaluate_fhirpath.assert_called_once_with(
        resource_type="Patient",
        resource_json=json.dumps({"resourceType": "Patient", "id": "example"}),
        fhirpath_expression="given.first()",
        context_expression="name",
        variables=None,
    )
