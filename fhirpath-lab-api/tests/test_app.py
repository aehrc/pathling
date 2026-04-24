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

"""Tests for the Flask application endpoints.

These tests use a mock PathlingContext to avoid Spark dependencies.

Author: John Grimes
"""

import json
import os
from unittest.mock import MagicMock, patch

import pytest
from py4j.protocol import Py4JJavaError

from fhirpath_lab_api import app as app_module
from fhirpath_lab_api.app import _extract_friendly_message, create_app

TEST_CORS_ORIGINS = "https://fhirpath-lab.azurewebsites.net,http://localhost:3000"


@pytest.fixture()
def mock_context():
    """Creates a mock PathlingContext for testing."""
    ctx = MagicMock()
    ctx.version.return_value = "9.3.1"
    ctx.evaluate_fhirpath.return_value = {
        "results": [{"type": "string", "value": "Smith"}],
        "expectedReturnType": "string",
        "traces": [],
    }
    return ctx


@pytest.fixture()
def client(mock_context):
    """Creates a Flask test client with CORS origins configured via env var."""
    with patch.dict(os.environ, {"CORS_ALLOWED_ORIGINS": TEST_CORS_ORIGINS}):
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


# ========== POST /fhir/$fhirpath tests ==========


def test_successful_evaluation(client, mock_context, valid_request_body):
    """A valid request returns a FHIR Parameters response with results."""
    response = client.post(
        "/fhir/$fhirpath",
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
        "/fhir/$fhirpath",
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
        "/fhir/$fhirpath",
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
        "/fhir/$fhirpath",
        data="not valid json{",
        content_type="application/json",
    )

    assert response.status_code == 400
    data = json.loads(response.data)
    assert data["resourceType"] == "OperationOutcome"


def test_empty_body_returns_400(client):
    """A request with an empty body returns 400."""
    response = client.post(
        "/fhir/$fhirpath",
        data="",
        content_type="application/json",
    )

    assert response.status_code == 400


def test_empty_expression_returns_empty_collection(client, mock_context):
    """An empty expression returns a successful response with no results."""
    body = {
        "resourceType": "Parameters",
        "parameter": [
            {"name": "expression", "valueString": ""},
            {
                "name": "resource",
                "resource": {"resourceType": "Patient", "id": "example"},
            },
        ],
    }
    response = client.post(
        "/fhir/$fhirpath",
        data=json.dumps(body),
        content_type="application/json",
    )

    assert response.status_code == 200
    assert response.content_type == "application/fhir+json"

    data = json.loads(response.data)
    assert data["resourceType"] == "Parameters"

    # Should contain a parameters part but no result part.
    assert len(data["parameter"]) == 1
    assert data["parameter"][0]["name"] == "parameters"

    # The engine should not have been invoked.
    mock_context.evaluate_fhirpath.assert_not_called()


def test_whitespace_expression_returns_empty_collection(client, mock_context):
    """A whitespace-only expression returns a successful response with no results."""
    body = {
        "resourceType": "Parameters",
        "parameter": [
            {"name": "expression", "valueString": "   \t\n  "},
            {
                "name": "resource",
                "resource": {"resourceType": "Patient", "id": "example"},
            },
        ],
    }
    response = client.post(
        "/fhir/$fhirpath",
        data=json.dumps(body),
        content_type="application/json",
    )

    assert response.status_code == 200
    data = json.loads(response.data)
    assert data["resourceType"] == "Parameters"
    assert len(data["parameter"]) == 1
    assert data["parameter"][0]["name"] == "parameters"
    mock_context.evaluate_fhirpath.assert_not_called()


def test_non_parameters_resource_returns_400(client):
    """A request with a non-Parameters resource returns 400."""
    body = {"resourceType": "Patient", "id": "example"}
    response = client.post(
        "/fhir/$fhirpath",
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
        "/fhir/$fhirpath",
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
        "/fhir/$fhirpath",
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
        "/fhir/$fhirpath",
        data=json.dumps(valid_request_body),
        content_type="application/json",
        headers={"Origin": "https://fhirpath-lab.azurewebsites.net"},
    )

    assert response.headers.get("Access-Control-Allow-Origin") is not None


def test_preflight_cors_request(client):
    """OPTIONS preflight request returns CORS headers."""
    response = client.options(
        "/fhir/$fhirpath",
        headers={
            "Origin": "https://fhirpath-lab.azurewebsites.net",
            "Access-Control-Request-Method": "POST",
            "Access-Control-Request-Headers": "Content-Type",
        },
    )

    assert response.status_code == 200
    assert response.headers.get("Access-Control-Allow-Origin") is not None


def test_cors_no_origins_configured(mock_context):
    """No CORS headers when CORS_ALLOWED_ORIGINS is unset."""
    with patch.dict(os.environ, {}, clear=True):
        # Remove CORS_ALLOWED_ORIGINS if present.
        os.environ.pop("CORS_ALLOWED_ORIGINS", None)
        app = create_app(pathling_context=mock_context)
        app.config["TESTING"] = True
        with app.test_client() as c:
            response = c.options(
                "/fhir/$fhirpath",
                headers={
                    "Origin": "https://fhirpath-lab.azurewebsites.net",
                    "Access-Control-Request-Method": "POST",
                },
            )
            assert response.headers.get("Access-Control-Allow-Origin") is None


def test_cors_multiple_origins(mock_context, valid_request_body):
    """All comma-separated origins in CORS_ALLOWED_ORIGINS are permitted."""
    origins = "https://example.com,https://other.example.com"
    with patch.dict(os.environ, {"CORS_ALLOWED_ORIGINS": origins}):
        app = create_app(pathling_context=mock_context)
        app.config["TESTING"] = True
        with app.test_client() as c:
            for origin in ["https://example.com", "https://other.example.com"]:
                response = c.post(
                    "/fhir/$fhirpath",
                    data=json.dumps(valid_request_body),
                    content_type="application/json",
                    headers={"Origin": origin},
                )
                assert (
                    response.headers.get("Access-Control-Allow-Origin") is not None
                ), f"Expected CORS header for {origin}"


def test_traces_included_in_response(client, mock_context, valid_request_body):
    """Trace entries from evaluation appear inside the result part."""
    mock_context.evaluate_fhirpath.return_value = {
        "results": [{"type": "boolean", "value": True}],
        "expectedReturnType": "boolean",
        "traces": [
            {
                "label": "flag",
                "values": [{"type": "boolean", "value": True}],
            },
        ],
    }

    response = client.post(
        "/fhir/$fhirpath",
        data=json.dumps(valid_request_body),
        content_type="application/json",
    )

    assert response.status_code == 200
    data = json.loads(response.data)
    result_part = data["parameter"][1]
    assert result_part["name"] == "result"

    trace_parts = [p for p in result_part["part"] if p["name"] == "trace"]
    assert len(trace_parts) == 1
    assert trace_parts[0]["valueString"] == "flag"
    assert trace_parts[0]["part"] == [{"name": "boolean", "valueBoolean": True}]


# ========== Context-grouped evaluation tests ==========


def _resource_example():
    """Returns the canonical Patient resource used in grouped-context tests."""
    return {"resourceType": "Patient", "id": "example"}


def _grouped_request_body(context="name", expression="given.first()"):
    """Builds a request body with the given context and expression."""
    return {
        "resourceType": "Parameters",
        "parameter": [
            {"name": "expression", "valueString": expression},
            {"name": "resource", "resource": _resource_example()},
            {"name": "context", "valueString": context},
        ],
    }


def test_grouped_context_multi_element(client, mock_context):
    """Multi-element context produces one result part per element, with
    per-call results isolated to the matching group."""
    mock_context.evaluate_fhirpath.side_effect = [
        # Count call: context "name" returns 2 elements.
        {
            "results": [
                {"type": "HumanName", "value": '{"family": "Chalmers"}'},
                {"type": "HumanName", "value": '{"family": "Windsor"}'},
            ],
            "expectedReturnType": "HumanName",
            "traces": [],
        },
        # Index 0 evaluation.
        {
            "results": [{"type": "string", "value": "Peter"}],
            "expectedReturnType": "string",
            "traces": [],
        },
        # Index 1 evaluation.
        {
            "results": [{"type": "string", "value": "Jim"}],
            "expectedReturnType": "string",
            "traces": [],
        },
    ]

    response = client.post(
        "/fhir/$fhirpath",
        data=json.dumps(_grouped_request_body()),
        content_type="application/json",
    )

    assert response.status_code == 200
    data = json.loads(response.data)
    result_parts = [p for p in data["parameter"] if p["name"] == "result"]
    assert len(result_parts) == 2
    assert result_parts[0]["valueString"] == "name[0]"
    assert result_parts[0]["part"] == [{"name": "string", "valueString": "Peter"}]
    assert result_parts[1]["valueString"] == "name[1]"
    assert result_parts[1]["part"] == [{"name": "string", "valueString": "Jim"}]

    # Three calls in order: count, index 0, index 1.
    calls = mock_context.evaluate_fhirpath.call_args_list
    assert len(calls) == 3
    assert calls[0].kwargs["fhirpath_expression"] == "name"
    assert calls[0].kwargs["context_expression"] is None
    assert calls[1].kwargs["fhirpath_expression"] == "given.first()"
    assert calls[1].kwargs["context_expression"] == "(name)[0]"
    assert calls[2].kwargs["context_expression"] == "(name)[1]"


def test_grouped_context_single_element(client, mock_context):
    """A single-element context still produces a grouped result (labelled
    ``name[0]``), not a flattened one."""
    mock_context.evaluate_fhirpath.side_effect = [
        {
            "results": [{"type": "HumanName", "value": '{"family": "Smith"}'}],
            "expectedReturnType": "HumanName",
            "traces": [],
        },
        {
            "results": [{"type": "string", "value": "John"}],
            "expectedReturnType": "string",
            "traces": [],
        },
    ]

    response = client.post(
        "/fhir/$fhirpath",
        data=json.dumps(_grouped_request_body()),
        content_type="application/json",
    )

    assert response.status_code == 200
    data = json.loads(response.data)
    result_parts = [p for p in data["parameter"] if p["name"] == "result"]
    assert len(result_parts) == 1
    assert result_parts[0]["valueString"] == "name[0]"
    assert result_parts[0]["part"] == [{"name": "string", "valueString": "John"}]


def test_grouped_context_zero_elements(client, mock_context):
    """A zero-element context produces no result parts and issues no per-index
    calls to the evaluator."""
    mock_context.evaluate_fhirpath.side_effect = [
        {"results": [], "expectedReturnType": "HumanName", "traces": []},
    ]

    response = client.post(
        "/fhir/$fhirpath",
        data=json.dumps(_grouped_request_body()),
        content_type="application/json",
    )

    assert response.status_code == 200
    data = json.loads(response.data)
    result_parts = [p for p in data["parameter"] if p["name"] == "result"]
    assert result_parts == []
    # Only the count call was made.
    assert mock_context.evaluate_fhirpath.call_count == 1


def test_grouped_context_with_traces(client, mock_context):
    """Per-iteration trace entries appear inside the matching result part
    only, not across groups."""
    mock_context.evaluate_fhirpath.side_effect = [
        # Count: 2 elements.
        {
            "results": [
                {"type": "HumanName", "value": "{}"},
                {"type": "HumanName", "value": "{}"},
            ],
            "expectedReturnType": "HumanName",
            "traces": [],
        },
        # Index 0.
        {
            "results": [{"type": "string", "value": "Peter James, Chalmers"}],
            "expectedReturnType": "string",
            "traces": [
                {
                    "label": "trc",
                    "values": [
                        {"type": "HumanName", "value": '{"family": "Chalmers"}'}
                    ],
                }
            ],
        },
        # Index 1.
        {
            "results": [{"type": "string", "value": "Peter James, Windsor"}],
            "expectedReturnType": "string",
            "traces": [
                {
                    "label": "trc",
                    "values": [{"type": "HumanName", "value": '{"family": "Windsor"}'}],
                }
            ],
        },
    ]

    body = _grouped_request_body(
        expression="trace('trc').given.join(' ').combine(family).join(', ')"
    )
    response = client.post(
        "/fhir/$fhirpath",
        data=json.dumps(body),
        content_type="application/json",
    )

    assert response.status_code == 200
    data = json.loads(response.data)
    result_parts = [p for p in data["parameter"] if p["name"] == "result"]

    trace0 = next(p for p in result_parts[0]["part"] if p["name"] == "trace")
    assert "Chalmers" in trace0["part"][0]["extension"][0]["valueString"]
    assert "Windsor" not in trace0["part"][0]["extension"][0]["valueString"]

    trace1 = next(p for p in result_parts[1]["part"] if p["name"] == "trace")
    assert "Windsor" in trace1["part"][0]["extension"][0]["valueString"]
    assert "Chalmers" not in trace1["part"][0]["extension"][0]["valueString"]


def test_grouped_context_with_variables(client, mock_context):
    """Variables are passed through to every call (count + per-index)."""
    mock_context.evaluate_fhirpath.side_effect = [
        {
            "results": [{"type": "HumanName", "value": "{}"}],
            "expectedReturnType": "HumanName",
            "traces": [],
        },
        {
            "results": [{"type": "boolean", "value": True}],
            "expectedReturnType": "boolean",
            "traces": [],
        },
    ]

    body = {
        "resourceType": "Parameters",
        "parameter": [
            {"name": "expression", "valueString": "given = %target"},
            {"name": "resource", "resource": _resource_example()},
            {"name": "context", "valueString": "name"},
            {
                "name": "variables",
                "part": [{"name": "target", "valueString": "John"}],
            },
        ],
    }
    response = client.post(
        "/fhir/$fhirpath",
        data=json.dumps(body),
        content_type="application/json",
    )

    assert response.status_code == 200
    calls = mock_context.evaluate_fhirpath.call_args_list
    assert len(calls) == 2
    for call in calls:
        assert call.kwargs["variables"] == {"target": "John"}


def test_grouped_context_complex_expression(client, mock_context):
    """A complex context is parenthesised in the per-index call but not in
    the response label."""
    mock_context.evaluate_fhirpath.side_effect = [
        {
            "results": [{"type": "HumanName", "value": "{}"}],
            "expectedReturnType": "HumanName",
            "traces": [],
        },
        {
            "results": [{"type": "string", "value": "John"}],
            "expectedReturnType": "string",
            "traces": [],
        },
    ]

    context_expr = "name.where(use='official')"
    response = client.post(
        "/fhir/$fhirpath",
        data=json.dumps(_grouped_request_body(context=context_expr)),
        content_type="application/json",
    )

    assert response.status_code == 200
    data = json.loads(response.data)
    result_parts = [p for p in data["parameter"] if p["name"] == "result"]
    assert result_parts[0]["valueString"] == "name.where(use='official')[0]"

    calls = mock_context.evaluate_fhirpath.call_args_list
    assert calls[1].kwargs["context_expression"] == "(name.where(use='official'))[0]"


def test_grouped_context_error_in_iteration_returns_500(client, mock_context):
    """An error during a per-index iteration surfaces as a 500 via the
    existing error pipeline."""
    mock_context.evaluate_fhirpath.side_effect = [
        {
            "results": [{"type": "HumanName", "value": "{}"}],
            "expectedReturnType": "HumanName",
            "traces": [],
        },
        RuntimeError("Evaluation failed on index 0"),
    ]

    response = client.post(
        "/fhir/$fhirpath",
        data=json.dumps(_grouped_request_body()),
        content_type="application/json",
    )

    assert response.status_code == 500
    data = json.loads(response.data)
    assert data["resourceType"] == "OperationOutcome"
    assert "Evaluation failed on index 0" in data["issue"][0]["details"]["text"]


def test_grouped_context_empty_expression_short_circuits(client, mock_context):
    """An empty expression short-circuits even when a context is supplied,
    and no Pathling calls are issued."""
    body = {
        "resourceType": "Parameters",
        "parameter": [
            {"name": "expression", "valueString": ""},
            {"name": "resource", "resource": _resource_example()},
            {"name": "context", "valueString": "name"},
        ],
    }
    response = client.post(
        "/fhir/$fhirpath",
        data=json.dumps(body),
        content_type="application/json",
    )

    assert response.status_code == 200
    data = json.loads(response.data)
    # Only the parameters part, no result parts.
    assert len(data["parameter"]) == 1
    assert data["parameter"][0]["name"] == "parameters"
    # Context is still echoed in the metadata.
    context_parts = [p for p in data["parameter"][0]["part"] if p["name"] == "context"]
    assert context_parts[0]["valueString"] == "name"
    mock_context.evaluate_fhirpath.assert_not_called()


def test_grouped_context_exceeds_limit_returns_400(client, mock_context):
    """A context that yields more than MAX_CONTEXT_ELEMENTS elements returns
    400 with an OperationOutcome naming the limit."""
    oversized = [{"type": "HumanName", "value": "{}"}] * 3

    mock_context.evaluate_fhirpath.side_effect = [
        {"results": oversized, "expectedReturnType": "HumanName", "traces": []},
    ]

    with patch.object(app_module, "MAX_CONTEXT_ELEMENTS", 2):
        response = client.post(
            "/fhir/$fhirpath",
            data=json.dumps(_grouped_request_body()),
            content_type="application/json",
        )

    assert response.status_code == 400
    data = json.loads(response.data)
    assert data["resourceType"] == "OperationOutcome"
    assert "3 elements" in data["issue"][0]["details"]["text"]
    assert "limit of 2" in data["issue"][0]["details"]["text"]
    # No per-index calls were made.
    assert mock_context.evaluate_fhirpath.call_count == 1


def test_grouped_context_at_limit_succeeds(client, mock_context):
    """A context whose cardinality exactly equals MAX_CONTEXT_ELEMENTS is
    accepted (the limit is inclusive)."""
    at_limit = [{"type": "HumanName", "value": "{}"}] * 2
    iter_response = {
        "results": [{"type": "string", "value": "John"}],
        "expectedReturnType": "string",
        "traces": [],
    }
    mock_context.evaluate_fhirpath.side_effect = [
        {"results": at_limit, "expectedReturnType": "HumanName", "traces": []},
        iter_response,
        iter_response,
    ]

    with patch.object(app_module, "MAX_CONTEXT_ELEMENTS", 2):
        response = client.post(
            "/fhir/$fhirpath",
            data=json.dumps(_grouped_request_body()),
            content_type="application/json",
        )

    assert response.status_code == 200
    data = json.loads(response.data)
    result_parts = [p for p in data["parameter"] if p["name"] == "result"]
    assert len(result_parts) == 2
    # Count call plus one call per element.
    assert mock_context.evaluate_fhirpath.call_count == 3


def test_grouped_context_expected_return_type_from_first_iteration(
    client, mock_context
):
    """The metadata expectedReturnType is taken from the first per-index
    iteration."""
    mock_context.evaluate_fhirpath.side_effect = [
        {
            "results": [
                {"type": "HumanName", "value": "{}"},
                {"type": "HumanName", "value": "{}"},
            ],
            "expectedReturnType": "HumanName",
            "traces": [],
        },
        {
            "results": [{"type": "string", "value": "John"}],
            "expectedReturnType": "string",
            "traces": [],
        },
        {
            "results": [{"type": "string", "value": "Jim"}],
            "expectedReturnType": "string",
            "traces": [],
        },
    ]

    response = client.post(
        "/fhir/$fhirpath",
        data=json.dumps(_grouped_request_body()),
        content_type="application/json",
    )

    assert response.status_code == 200
    data = json.loads(response.data)
    params_part = data["parameter"][0]["part"]
    ert = next(p for p in params_part if p["name"] == "expectedReturnType")
    assert ert["valueString"] == "string"


def test_flat_context_no_longer_passed_through_in_flat_path(client, mock_context):
    """Requests without a context parameter still use the flat single-call
    path with ``context_expression=None``."""
    body = {
        "resourceType": "Parameters",
        "parameter": [
            {"name": "expression", "valueString": "name.family"},
            {"name": "resource", "resource": _resource_example()},
        ],
    }
    response = client.post(
        "/fhir/$fhirpath",
        data=json.dumps(body),
        content_type="application/json",
    )

    assert response.status_code == 200
    mock_context.evaluate_fhirpath.assert_called_once_with(
        resource_type="Patient",
        resource_json=json.dumps(_resource_example()),
        fhirpath_expression="name.family",
        context_expression=None,
        variables=None,
    )


# ========== Friendly error message tests ==========


class _TestPy4JJavaError(Py4JJavaError):
    """A testable Py4JJavaError that does not require a live JVM."""

    def __init__(self, message: str):
        # Skip parent __init__ to avoid JVM dependency.
        Exception.__init__(self, message)
        self._message = message

    def __str__(self):
        return self._message


# A realistic Py4J error string with newlines and tab-indented stack trace.
PY4J_ERROR_WITH_TRACE = (
    "An error occurred while calling o84.evaluateFhirPath.\n"
    ": au.csiro.pathling.errors.UnsupportedFhirPathFeatureError:"
    " Unsupported function: trace\n"
    "\tat au.csiro.pathling.fhirpath.path.Paths$EvalFunction.apply"
    "(Paths.java:190)\n"
    "\tat au.csiro.pathling.fhirpath.FhirPath$Composite.lambda$apply$0"
    "(FhirPath.java:179)\n"
)


def test_extract_friendly_message_from_py4j_error():
    """Extracts the short class name and message from a Py4J error string."""
    result = _extract_friendly_message(PY4J_ERROR_WITH_TRACE)
    assert result == "UnsupportedFhirPathFeatureError: Unsupported function: trace"


def test_extract_friendly_message_returns_none_for_non_java_error():
    """Returns None when the error string does not match the Java pattern."""
    result = _extract_friendly_message("Something went wrong")
    assert result is None


def test_py4j_error_returns_friendly_message_and_diagnostics(
    client, mock_context, valid_request_body
):
    """A Py4J error returns a concise message in details.text and the full
    trace in diagnostics."""
    mock_context.evaluate_fhirpath.side_effect = _TestPy4JJavaError(
        PY4J_ERROR_WITH_TRACE
    )

    response = client.post(
        "/fhir/$fhirpath",
        data=json.dumps(valid_request_body),
        content_type="application/json",
    )

    assert response.status_code == 500
    data = json.loads(response.data)
    issue = data["issue"][0]
    assert (
        issue["details"]["text"]
        == "UnsupportedFhirPathFeatureError: Unsupported function: trace"
    )
    assert "au.csiro.pathling.fhirpath.path.Paths" in issue["diagnostics"]


def test_py4j_error_with_unparseable_format_falls_back(
    client, mock_context, valid_request_body
):
    """A Py4J error with an unparseable string falls back to the full message
    with no diagnostics."""
    mock_context.evaluate_fhirpath.side_effect = _TestPy4JJavaError(
        "Some unexpected Py4J message"
    )

    response = client.post(
        "/fhir/$fhirpath",
        data=json.dumps(valid_request_body),
        content_type="application/json",
    )

    assert response.status_code == 500
    data = json.loads(response.data)
    issue = data["issue"][0]
    assert "Some unexpected Py4J message" in issue["details"]["text"]
    assert "diagnostics" not in issue


def test_non_py4j_error_passes_through(client, mock_context, valid_request_body):
    """A non-Py4J exception uses the message directly with no diagnostics."""
    mock_context.evaluate_fhirpath.side_effect = RuntimeError("Evaluation failed")

    response = client.post(
        "/fhir/$fhirpath",
        data=json.dumps(valid_request_body),
        content_type="application/json",
    )

    assert response.status_code == 500
    data = json.loads(response.data)
    issue = data["issue"][0]
    assert issue["details"]["text"] == "Error evaluating expression: Evaluation failed"
    assert "diagnostics" not in issue
