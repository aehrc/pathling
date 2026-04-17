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

"""Tests for PathlingContext.evaluate_fhirpath()."""

import json

import pytest

PATIENT_JSON = json.dumps(
    {
        "resourceType": "Patient",
        "id": "example",
        "active": True,
        "gender": "male",
        "birthDate": "1990-01-01",
        "name": [
            {"use": "official", "family": "Smith", "given": ["John", "James"]},
            {"use": "nickname", "family": "Smith", "given": ["Johnny"]},
        ],
        "telecom": [{"system": "phone", "value": "555-1234"}],
        "address": [{"city": "Melbourne", "state": "VIC"}],
    }
)


# ========== Basic result evaluation ==========


def test_string_result(pathling_ctx):
    """Evaluating a string path returns typed string values."""
    result = pathling_ctx.evaluate_fhirpath("Patient", PATIENT_JSON, "gender")

    assert result == {
        "expectedReturnType": "code",
        "resultGroups": [
            {
                "contextKey": None,
                "results": [{"type": "code", "value": "male"}],
                "traces": [],
            }
        ],
    }


def test_boolean_result(pathling_ctx):
    """Evaluating a boolean path returns typed boolean values."""
    result = pathling_ctx.evaluate_fhirpath("Patient", PATIENT_JSON, "active")

    assert result == {
        "expectedReturnType": "boolean",
        "resultGroups": [
            {
                "contextKey": None,
                "results": [{"type": "boolean", "value": True}],
                "traces": [],
            }
        ],
    }


def test_date_result(pathling_ctx):
    """Evaluating a date path returns typed date values."""
    result = pathling_ctx.evaluate_fhirpath("Patient", PATIENT_JSON, "birthDate")

    assert result == {
        "expectedReturnType": "date",
        "resultGroups": [
            {
                "contextKey": None,
                "results": [{"type": "date", "value": "1990-01-01"}],
                "traces": [],
            }
        ],
    }


def test_integer_result(pathling_ctx):
    """Evaluating count() returns a typed integer value."""
    result = pathling_ctx.evaluate_fhirpath("Patient", PATIENT_JSON, "name.count()")

    assert result == {
        "expectedReturnType": "integer",
        "resultGroups": [
            {
                "contextKey": None,
                "results": [{"type": "integer", "value": 2}],
                "traces": [],
            }
        ],
    }


def test_multiple_results(pathling_ctx):
    """Evaluating a path that matches multiple elements returns all values."""
    result = pathling_ctx.evaluate_fhirpath("Patient", PATIENT_JSON, "name.given")

    assert result["expectedReturnType"] == "string"
    assert len(result["resultGroups"]) == 1
    group = result["resultGroups"][0]
    assert group["contextKey"] is None
    assert group["traces"] == []
    values = {r["value"] for r in group["results"]}
    assert values == {"John", "James", "Johnny"}
    assert all(r["type"] == "string" for r in group["results"])


def test_empty_result(pathling_ctx):
    """Evaluating a path that matches nothing returns an empty list."""
    result = pathling_ctx.evaluate_fhirpath(
        "Patient", PATIENT_JSON, "multipleBirthBoolean"
    )

    assert result == {
        "expectedReturnType": "boolean",
        "resultGroups": [
            {
                "contextKey": None,
                "results": [],
                "traces": [],
            }
        ],
    }


def test_complex_type_result(pathling_ctx):
    """Evaluating a complex type path returns typed values with string
    representations."""
    result = pathling_ctx.evaluate_fhirpath("Patient", PATIENT_JSON, "name")

    assert result["expectedReturnType"] == "HumanName"
    assert len(result["resultGroups"]) == 1
    group = result["resultGroups"][0]
    assert group["contextKey"] is None
    assert group["traces"] == []
    assert len(group["results"]) == 2
    assert all(r["type"] == "HumanName" for r in group["results"])
    assert all(isinstance(r["value"], str) for r in group["results"])


# ========== Expected return type ==========


def test_primitive_return_type(pathling_ctx):
    """The return type for a string expression is correctly reported."""
    result = pathling_ctx.evaluate_fhirpath("Patient", PATIENT_JSON, "name.family")

    assert result["expectedReturnType"] == "string"


def test_boolean_return_type(pathling_ctx):
    """The return type for a boolean expression is correctly reported."""
    result = pathling_ctx.evaluate_fhirpath("Patient", PATIENT_JSON, "active")

    assert result["expectedReturnType"] == "boolean"


def test_complex_return_type(pathling_ctx):
    """The return type for a complex type expression is correctly reported."""
    result = pathling_ctx.evaluate_fhirpath("Patient", PATIENT_JSON, "name")

    assert result["expectedReturnType"] == "HumanName"


# ========== Context expression ==========


def test_with_context_expression(pathling_ctx):
    """A context expression produces one result group per context element."""
    result = pathling_ctx.evaluate_fhirpath(
        "Patient", PATIENT_JSON, "given", context_expression="name"
    )

    assert result["expectedReturnType"] == "unknown"
    assert len(result["resultGroups"]) == 2

    group0 = result["resultGroups"][0]
    assert group0["contextKey"] == "name[0]"
    values0 = {r["value"] for r in group0["results"]}
    assert values0 == {"John", "James"}

    group1 = result["resultGroups"][1]
    assert group1["contextKey"] == "name[1]"
    values1 = {r["value"] for r in group1["results"]}
    assert values1 == {"Johnny"}


def test_without_context_expression(pathling_ctx):
    """Omitting context_expression returns a single group with null contextKey."""
    result = pathling_ctx.evaluate_fhirpath("Patient", PATIENT_JSON, "active")

    assert result == {
        "expectedReturnType": "boolean",
        "resultGroups": [
            {
                "contextKey": None,
                "results": [{"type": "boolean", "value": True}],
                "traces": [],
            }
        ],
    }


# ========== Variables ==========


def test_with_string_variable(pathling_ctx):
    """A string variable is resolvable in the expression."""
    result = pathling_ctx.evaluate_fhirpath(
        "Patient", PATIENT_JSON, "%greeting", variables={"greeting": "hello"}
    )

    assert result == {
        "expectedReturnType": "string",
        "resultGroups": [
            {
                "contextKey": None,
                "results": [{"type": "string", "value": "hello"}],
                "traces": [],
            }
        ],
    }


def test_with_integer_variable(pathling_ctx):
    """An integer variable is resolvable in the expression."""
    result = pathling_ctx.evaluate_fhirpath(
        "Patient", PATIENT_JSON, "%count", variables={"count": 42}
    )

    assert result == {
        "expectedReturnType": "integer",
        "resultGroups": [
            {
                "contextKey": None,
                "results": [{"type": "integer", "value": 42}],
                "traces": [],
            }
        ],
    }


def test_with_boolean_variable(pathling_ctx):
    """A boolean variable is resolvable in the expression."""
    result = pathling_ctx.evaluate_fhirpath(
        "Patient", PATIENT_JSON, "%flag", variables={"flag": True}
    )

    assert result == {
        "expectedReturnType": "boolean",
        "resultGroups": [
            {
                "contextKey": None,
                "results": [{"type": "boolean", "value": True}],
                "traces": [],
            }
        ],
    }


def test_without_variables(pathling_ctx):
    """Omitting variables evaluates normally."""
    result = pathling_ctx.evaluate_fhirpath("Patient", PATIENT_JSON, "active")

    assert result == {
        "expectedReturnType": "boolean",
        "resultGroups": [
            {
                "contextKey": None,
                "results": [{"type": "boolean", "value": True}],
                "traces": [],
            }
        ],
    }


# ========== Trace output ==========


def test_trace_boolean(pathling_ctx):
    """Tracing a boolean expression returns the traced value with correct
    label and type."""
    result = pathling_ctx.evaluate_fhirpath(
        "Patient", PATIENT_JSON, "active.trace('flag')"
    )

    assert result == {
        "expectedReturnType": "boolean",
        "resultGroups": [
            {
                "contextKey": None,
                "results": [{"type": "boolean", "value": True}],
                "traces": [
                    {
                        "label": "flag",
                        "values": [{"type": "boolean", "value": True}],
                    }
                ],
            }
        ],
    }


def test_trace_entry_has_no_type_key(pathling_ctx):
    """Each trace entry has label and values keys, but no top-level type key."""
    result = pathling_ctx.evaluate_fhirpath(
        "Patient", PATIENT_JSON, "active.trace('flag')"
    )

    for group in result["resultGroups"]:
        for trace in group["traces"]:
            assert set(trace.keys()) == {"label", "values"}


def test_multiple_trace_calls(pathling_ctx):
    """Multiple trace() calls produce distinct trace entries with correct
    labels."""
    result = pathling_ctx.evaluate_fhirpath(
        "Patient",
        PATIENT_JSON,
        "name.trace('before').where(use = 'official').trace('after').family",
    )

    assert result["expectedReturnType"] == "string"
    group = result["resultGroups"][0]
    assert group["results"] == [{"type": "string", "value": "Smith"}]
    labels = [t["label"] for t in group["traces"]]
    assert "before" in labels
    assert "after" in labels


def test_no_trace_returns_empty_list(pathling_ctx):
    """An expression without trace() returns an empty traces list."""
    result = pathling_ctx.evaluate_fhirpath("Patient", PATIENT_JSON, "active")

    assert result["resultGroups"][0]["traces"] == []


# ========== Error handling ==========


def test_invalid_expression(pathling_ctx):
    """An invalid FHIRPath expression raises an exception."""
    with pytest.raises(Exception):
        pathling_ctx.evaluate_fhirpath("Patient", PATIENT_JSON, "!!invalid!!")


def test_malformed_json(pathling_ctx):
    """Malformed JSON resource raises an exception."""
    with pytest.raises(Exception):
        pathling_ctx.evaluate_fhirpath("Patient", "{not valid json}", "active")
