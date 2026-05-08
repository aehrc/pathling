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

"""Tests for FHIR Parameters parsing and response construction.

Author: John Grimes
"""

import json

import pytest

from fhirpath_lab_api.parameters import (
    JSON_VALUE_EXTENSION_URL,
    build_grouped_response_parameters,
    build_operation_outcome,
    build_response_parameters,
    parse_input_parameters,
)

# ========== Input parameter parsing tests ==========


def test_parse_expression_and_resource():
    """Extracts expression and resource from a valid Parameters body."""
    body = {
        "resourceType": "Parameters",
        "parameter": [
            {"name": "expression", "valueString": "name.family"},
            {
                "name": "resource",
                "resource": {"resourceType": "Patient", "id": "example"},
            },
        ],
    }
    result = parse_input_parameters(body)
    assert result.expression == "name.family"
    assert result.resource == {"resourceType": "Patient", "id": "example"}
    assert result.resource_type == "Patient"
    assert result.context is None
    assert result.variables is None


def test_parse_context_expression():
    """Extracts the optional context expression parameter."""
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
    result = parse_input_parameters(body)
    assert result.context == "name"


def test_parse_variables():
    """Extracts variables from the variables parameter parts."""
    body = {
        "resourceType": "Parameters",
        "parameter": [
            {"name": "expression", "valueString": "%myVar"},
            {
                "name": "resource",
                "resource": {"resourceType": "Patient", "id": "example"},
            },
            {
                "name": "variables",
                "part": [
                    {"name": "myVar", "valueString": "test"},
                ],
            },
        ],
    }
    result = parse_input_parameters(body)
    assert result.variables == {"myVar": "test"}


def test_parse_resource_via_json_extension():
    """Extracts a resource from the json-value extension."""
    resource_json = json.dumps({"resourceType": "Patient", "id": "ext-example"})
    body = {
        "resourceType": "Parameters",
        "parameter": [
            {"name": "expression", "valueString": "id"},
            {
                "name": "resource",
                "extension": [
                    {
                        "url": JSON_VALUE_EXTENSION_URL,
                        "valueString": resource_json,
                    }
                ],
            },
        ],
    }
    result = parse_input_parameters(body)
    assert result.resource["id"] == "ext-example"


def test_parse_missing_expression_raises():
    """Raises ValueError when expression parameter is missing."""
    body = {
        "resourceType": "Parameters",
        "parameter": [
            {
                "name": "resource",
                "resource": {"resourceType": "Patient", "id": "example"},
            },
        ],
    }
    with pytest.raises(ValueError, match="Missing required parameter: expression"):
        parse_input_parameters(body)


def test_parse_missing_resource_raises():
    """Raises ValueError when resource parameter is missing."""
    body = {
        "resourceType": "Parameters",
        "parameter": [
            {"name": "expression", "valueString": "name.family"},
        ],
    }
    with pytest.raises(ValueError, match="Missing required parameter: resource"):
        parse_input_parameters(body)


def test_parse_invalid_resource_type_raises():
    """Raises ValueError when the body is not a Parameters resource."""
    body = {"resourceType": "Patient", "id": "example"}
    with pytest.raises(ValueError, match="Expected FHIR Parameters resource"):
        parse_input_parameters(body)


# ========== Response construction tests ==========


def test_build_response_with_primitive_results():
    """Builds a response with primitive string results."""
    results = [
        {"type": "string", "value": "Smith"},
        {"type": "string", "value": "Jones"},
    ]
    response = build_response_parameters(
        evaluator_string="Pathling 9.3.1 (R4)",
        expression="name.family",
        resource={"resourceType": "Patient", "id": "example"},
        expected_return_type="string",
        results=results,
    )

    assert response["resourceType"] == "Parameters"
    params = response["parameter"]
    assert len(params) == 2

    # Check the parameters part.
    params_part = params[0]
    assert params_part["name"] == "parameters"
    evaluator_part = params_part["part"][0]
    assert evaluator_part["name"] == "evaluator"
    assert evaluator_part["valueString"] == "Pathling 9.3.1 (R4)"

    # Check the result part.
    result_part = params[1]
    assert result_part["name"] == "result"
    assert len(result_part["part"]) == 2
    assert result_part["part"][0] == {"name": "string", "valueString": "Smith"}
    assert result_part["part"][1] == {"name": "string", "valueString": "Jones"}


def test_build_response_with_complex_type_results():
    """Builds a response where complex types use the json-value extension."""
    results = [
        {"type": "HumanName", "value": '{"family": "Smith", "given": ["John"]}'},
    ]
    response = build_response_parameters(
        evaluator_string="Pathling 9.3.1 (R4)",
        expression="name",
        resource={"resourceType": "Patient", "id": "example"},
        expected_return_type="HumanName",
        results=results,
    )

    result_part = response["parameter"][1]
    name_part = result_part["part"][0]
    assert name_part["name"] == "HumanName"
    assert name_part["extension"][0]["url"] == JSON_VALUE_EXTENSION_URL
    assert "Smith" in name_part["extension"][0]["valueString"]


def test_build_response_with_empty_results():
    """Builds a response with no result parts when results are empty."""
    response = build_response_parameters(
        evaluator_string="Pathling 9.3.1 (R4)",
        expression="deceased",
        resource={"resourceType": "Patient", "id": "example"},
        expected_return_type="unknown",
        results=[],
    )

    # Should only have the parameters part, no result part.
    assert len(response["parameter"]) == 1
    assert response["parameter"][0]["name"] == "parameters"


def test_build_response_with_context():
    """Includes the context expression in the parameters part."""
    response = build_response_parameters(
        evaluator_string="Pathling 9.3.1 (R4)",
        expression="given.first()",
        resource={"resourceType": "Patient", "id": "example"},
        expected_return_type="string",
        results=[{"type": "string", "value": "John"}],
        context="name",
    )

    params_part = response["parameter"][0]["part"]
    context_parts = [p for p in params_part if p["name"] == "context"]
    assert len(context_parts) == 1
    assert context_parts[0]["valueString"] == "name"


def test_build_response_includes_expected_return_type():
    """Includes the expectedReturnType in the parameters part."""
    response = build_response_parameters(
        evaluator_string="Pathling 9.3.1 (R4)",
        expression="active",
        resource={"resourceType": "Patient", "id": "example"},
        expected_return_type="boolean",
        results=[{"type": "boolean", "value": True}],
    )

    params_part = response["parameter"][0]["part"]
    return_type_parts = [p for p in params_part if p["name"] == "expectedReturnType"]
    assert len(return_type_parts) == 1
    assert return_type_parts[0]["valueString"] == "boolean"


# ========== Trace rendering tests ==========


def test_build_response_with_traces():
    """Trace entries appear inside the result part with label and typed values."""
    traces = [
        {
            "label": "myLabel",
            "values": [
                {"type": "string", "value": "id1"},
                {"type": "string", "value": "id2"},
            ],
        },
    ]
    response = build_response_parameters(
        evaluator_string="Pathling 9.6.0 (R4)",
        expression="name.given.trace('myLabel')",
        resource={"resourceType": "Patient", "id": "example"},
        expected_return_type="string",
        results=[{"type": "string", "value": "John"}],
        traces=traces,
    )

    result_part = response["parameter"][1]
    assert result_part["name"] == "result"

    # First part is the result value, second is the trace.
    assert result_part["part"][0] == {"name": "string", "valueString": "John"}

    trace_part = result_part["part"][1]
    assert trace_part["name"] == "trace"
    assert trace_part["valueString"] == "myLabel"
    assert trace_part["part"] == [
        {"name": "string", "valueString": "id1"},
        {"name": "string", "valueString": "id2"},
    ]


def test_build_response_with_empty_traces():
    """No trace parts appear when the traces list is empty."""
    response = build_response_parameters(
        evaluator_string="Pathling 9.6.0 (R4)",
        expression="active",
        resource={"resourceType": "Patient", "id": "example"},
        expected_return_type="boolean",
        results=[{"type": "boolean", "value": True}],
        traces=[],
    )

    result_part = response["parameter"][1]
    trace_parts = [p for p in result_part["part"] if p["name"] == "trace"]
    assert trace_parts == []


def test_build_response_with_complex_trace_values():
    """Complex trace values use the json-value extension."""
    traces = [
        {
            "label": "names",
            "values": [
                {"type": "HumanName", "value": '{"family": "Smith"}'},
            ],
        },
    ]
    response = build_response_parameters(
        evaluator_string="Pathling 9.6.0 (R4)",
        expression="name.trace('names')",
        resource={"resourceType": "Patient", "id": "example"},
        expected_return_type="HumanName",
        results=[],
        traces=traces,
    )

    # Traces present even when results are empty.
    result_part = response["parameter"][1]
    assert result_part["name"] == "result"

    trace_part = result_part["part"][0]
    assert trace_part["name"] == "trace"
    assert trace_part["valueString"] == "names"
    value_part = trace_part["part"][0]
    assert value_part["name"] == "HumanName"
    assert value_part["extension"][0]["url"] == JSON_VALUE_EXTENSION_URL
    assert "Smith" in value_part["extension"][0]["valueString"]


# ========== Grouped response construction tests ==========


def test_build_response_grouped_empty():
    """A zero-element grouped response emits only the parameters part."""
    response = build_grouped_response_parameters(
        evaluator_string="Pathling 9.6.0 (R4)",
        expression="given.first()",
        resource={"resourceType": "Patient", "id": "example"},
        expected_return_type="",
        context="name",
        grouped_results=[],
    )

    assert len(response["parameter"]) == 1
    assert response["parameter"][0]["name"] == "parameters"


def test_build_response_grouped_single_group():
    """A single-group grouped response emits one labelled result part."""
    response = build_grouped_response_parameters(
        evaluator_string="Pathling 9.6.0 (R4)",
        expression="given.first()",
        resource={"resourceType": "Patient", "id": "example"},
        expected_return_type="string",
        context="name",
        grouped_results=[
            ("name[0]", [{"type": "string", "value": "John"}], []),
        ],
    )

    assert len(response["parameter"]) == 2
    result_part = response["parameter"][1]
    assert result_part["name"] == "result"
    assert result_part["valueString"] == "name[0]"
    assert result_part["part"] == [{"name": "string", "valueString": "John"}]


def test_build_response_grouped_multiple_groups():
    """Multiple groups each produce their own labelled result part; per-group
    traces do not bleed across groups."""
    grouped_results = [
        (
            "name[0]",
            [{"type": "string", "value": "John"}],
            [{"label": "trc", "values": [{"type": "string", "value": "hit0"}]}],
        ),
        (
            "name[1]",
            [{"type": "string", "value": "Jane"}],
            [{"label": "trc", "values": [{"type": "string", "value": "hit1"}]}],
        ),
    ]
    response = build_grouped_response_parameters(
        evaluator_string="Pathling 9.6.0 (R4)",
        expression="given.first().trace('trc')",
        resource={"resourceType": "Patient", "id": "example"},
        expected_return_type="string",
        context="name",
        grouped_results=grouped_results,
    )

    result_parts = [p for p in response["parameter"] if p["name"] == "result"]
    assert len(result_parts) == 2

    assert result_parts[0]["valueString"] == "name[0]"
    assert result_parts[0]["part"][0] == {"name": "string", "valueString": "John"}
    trace0 = result_parts[0]["part"][1]
    assert trace0["name"] == "trace"
    assert trace0["valueString"] == "trc"
    assert trace0["part"] == [{"name": "string", "valueString": "hit0"}]

    assert result_parts[1]["valueString"] == "name[1]"
    assert result_parts[1]["part"][0] == {"name": "string", "valueString": "Jane"}
    trace1 = result_parts[1]["part"][1]
    assert trace1["part"] == [{"name": "string", "valueString": "hit1"}]


def test_build_response_grouped_complex_trace_value():
    """Complex trace values inside a group use the json-value extension."""
    grouped_results = [
        (
            "name[0]",
            [],
            [
                {
                    "label": "trc",
                    "values": [
                        {"type": "HumanName", "value": '{"family": "Chalmers"}'}
                    ],
                }
            ],
        ),
    ]
    response = build_grouped_response_parameters(
        evaluator_string="Pathling 9.6.0 (R4)",
        expression="trace('trc')",
        resource={"resourceType": "Patient", "id": "example"},
        expected_return_type="HumanName",
        context="name",
        grouped_results=grouped_results,
    )

    result_part = response["parameter"][1]
    trace_part = result_part["part"][0]
    assert trace_part["name"] == "trace"
    value_part = trace_part["part"][0]
    assert value_part["name"] == "HumanName"
    assert value_part["extension"][0]["url"] == JSON_VALUE_EXTENSION_URL
    assert "Chalmers" in value_part["extension"][0]["valueString"]


def test_build_response_grouped_preserves_metadata():
    """The parameters metadata part is present and correct in grouped responses."""
    response = build_grouped_response_parameters(
        evaluator_string="Pathling 9.6.0 (R4)",
        expression="given.first()",
        resource={"resourceType": "Patient", "id": "example"},
        expected_return_type="string",
        context="name",
        grouped_results=[
            ("name[0]", [{"type": "string", "value": "John"}], []),
        ],
    )

    params_part = response["parameter"][0]
    assert params_part["name"] == "parameters"
    names = {p["name"]: p for p in params_part["part"]}
    assert names["evaluator"]["valueString"] == "Pathling 9.6.0 (R4)"
    assert names["expression"]["valueString"] == "given.first()"
    assert names["context"]["valueString"] == "name"
    assert names["expectedReturnType"]["valueString"] == "string"


def test_build_response_grouped_empty_group_still_emitted():
    """A group with no results and no traces still produces a labelled result
    part with an empty part list, preserving index continuity."""
    response = build_grouped_response_parameters(
        evaluator_string="Pathling 9.6.0 (R4)",
        expression="given.first()",
        resource={"resourceType": "Patient", "id": "example"},
        expected_return_type="string",
        context="name",
        grouped_results=[
            ("name[0]", [{"type": "string", "value": "John"}], []),
            ("name[1]", [], []),
            ("name[2]", [{"type": "string", "value": "Peter"}], []),
        ],
    )

    result_parts = [p for p in response["parameter"] if p["name"] == "result"]
    labels = [p["valueString"] for p in result_parts]
    assert labels == ["name[0]", "name[1]", "name[2]"]
    assert result_parts[1]["part"] == []


# ========== OperationOutcome construction tests ==========


def test_build_operation_outcome():
    """Builds a valid OperationOutcome resource."""
    outcome = build_operation_outcome("error", "invalid", "Something went wrong")
    assert outcome["resourceType"] == "OperationOutcome"
    assert len(outcome["issue"]) == 1
    issue = outcome["issue"][0]
    assert issue["severity"] == "error"
    assert issue["code"] == "invalid"
    assert issue["details"]["text"] == "Something went wrong"


def test_build_operation_outcome_with_diagnostics():
    """Includes diagnostics when provided."""
    outcome = build_operation_outcome(
        "error", "exception", "Server error", diagnostics="Stack trace here"
    )
    assert outcome["issue"][0]["diagnostics"] == "Stack trace here"
