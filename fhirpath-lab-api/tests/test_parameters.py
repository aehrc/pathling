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

"""Tests for FHIR Parameters parsing and response construction.

Author: John Grimes
"""

import json

import pytest

from fhirpath_lab_api.parameters import (
    JSON_VALUE_EXTENSION_URL,
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


def test_build_response_complex_type_omits_null_fields():
    """Null-valued fields are stripped from complex type JSON output."""
    # Simulates a Quantity result with nulls for unset fields.
    results = [
        {
            "type": "Quantity",
            "value": {
                "id": None,
                "value": 1.5,
                "comparator": None,
                "unit": "mmol/L",
                "system": "http://unitsofmeasure.org",
                "code": "mmol/L",
            },
        },
    ]
    response = build_response_parameters(
        evaluator_string="Pathling 9.3.1 (R4)",
        expression="value.ofType(Quantity)",
        resource={"resourceType": "Observation", "id": "example"},
        expected_return_type="Quantity",
        results=results,
    )

    result_part = response["parameter"][1]
    quantity_part = result_part["part"][0]
    quantity_json = json.loads(quantity_part["extension"][0]["valueString"])

    # Null-valued fields should be absent.
    assert "id" not in quantity_json
    assert "comparator" not in quantity_json
    # Non-null fields should be present.
    assert quantity_json["value"] == 1.5
    assert quantity_json["unit"] == "mmol/L"
    assert quantity_json["system"] == "http://unitsofmeasure.org"
    assert quantity_json["code"] == "mmol/L"


def test_build_response_complex_type_strips_nulls_recursively():
    """Null-valued fields are stripped recursively from nested structs."""
    results = [
        {
            "type": "HumanName",
            "value": {
                "family": "Smith",
                "given": ["John"],
                "period": {
                    "start": "2020-01-01",
                    "end": None,
                },
                "text": None,
            },
        },
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
    name_json = json.loads(name_part["extension"][0]["valueString"])

    # Top-level null should be stripped.
    assert "text" not in name_json
    # Nested null should be stripped.
    assert "end" not in name_json["period"]
    # Non-null values preserved.
    assert name_json["family"] == "Smith"
    assert name_json["period"]["start"] == "2020-01-01"


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
