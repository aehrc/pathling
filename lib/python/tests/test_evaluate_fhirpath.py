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
        "results": [{"type": "code", "value": "male"}],
        "expectedReturnType": "code",
        "traces": [],
    }


def test_boolean_result(pathling_ctx):
    """Evaluating a boolean path returns typed boolean values."""
    result = pathling_ctx.evaluate_fhirpath("Patient", PATIENT_JSON, "active")

    assert result == {
        "results": [{"type": "boolean", "value": True}],
        "expectedReturnType": "boolean",
        "traces": [],
    }


def test_date_result(pathling_ctx):
    """Evaluating a date path returns typed date values."""
    result = pathling_ctx.evaluate_fhirpath("Patient", PATIENT_JSON, "birthDate")

    assert result == {
        "results": [{"type": "date", "value": "1990-01-01"}],
        "expectedReturnType": "date",
        "traces": [],
    }


def test_integer_result(pathling_ctx):
    """Evaluating count() returns a typed integer value."""
    result = pathling_ctx.evaluate_fhirpath("Patient", PATIENT_JSON, "name.count()")

    assert result == {
        "results": [{"type": "integer", "value": 2}],
        "expectedReturnType": "integer",
        "traces": [],
    }


def test_multiple_results(pathling_ctx):
    """Evaluating a path that matches multiple elements returns all values."""
    result = pathling_ctx.evaluate_fhirpath("Patient", PATIENT_JSON, "name.given")

    assert result["expectedReturnType"] == "string"
    assert result["traces"] == []
    values = {r["value"] for r in result["results"]}
    assert values == {"John", "James", "Johnny"}
    assert all(r["type"] == "string" for r in result["results"])


def test_empty_result(pathling_ctx):
    """Evaluating a path that matches nothing returns an empty list."""
    result = pathling_ctx.evaluate_fhirpath(
        "Patient", PATIENT_JSON, "multipleBirthBoolean"
    )

    assert result == {
        "results": [],
        "expectedReturnType": "boolean",
        "traces": [],
    }


def test_complex_type_result(pathling_ctx):
    """Evaluating a complex type path returns typed values with string
    representations."""
    result = pathling_ctx.evaluate_fhirpath("Patient", PATIENT_JSON, "name")

    assert result["expectedReturnType"] == "HumanName"
    assert result["traces"] == []
    assert len(result["results"]) == 2
    assert all(r["type"] == "HumanName" for r in result["results"])
    assert all(isinstance(r["value"], str) for r in result["results"])


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
    """A context expression composes with the main expression."""
    result = pathling_ctx.evaluate_fhirpath(
        "Patient", PATIENT_JSON, "given", context_expression="name"
    )

    assert result["expectedReturnType"] == "unknown"
    assert result["traces"] == []
    values = {r["value"] for r in result["results"]}
    assert values == {"John", "James", "Johnny"}


def test_without_context_expression(pathling_ctx):
    """Omitting context_expression evaluates the main expression directly."""
    result = pathling_ctx.evaluate_fhirpath("Patient", PATIENT_JSON, "active")

    assert result == {
        "results": [{"type": "boolean", "value": True}],
        "expectedReturnType": "boolean",
        "traces": [],
    }


# ========== Variables ==========


def test_with_string_variable(pathling_ctx):
    """A string variable is resolvable in the expression."""
    result = pathling_ctx.evaluate_fhirpath(
        "Patient", PATIENT_JSON, "%greeting", variables={"greeting": "hello"}
    )

    assert result == {
        "results": [{"type": "string", "value": "hello"}],
        "expectedReturnType": "string",
        "traces": [],
    }


def test_with_integer_variable(pathling_ctx):
    """An integer variable is resolvable in the expression."""
    result = pathling_ctx.evaluate_fhirpath(
        "Patient", PATIENT_JSON, "%count", variables={"count": 42}
    )

    assert result == {
        "results": [{"type": "integer", "value": 42}],
        "expectedReturnType": "integer",
        "traces": [],
    }


def test_with_boolean_variable(pathling_ctx):
    """A boolean variable is resolvable in the expression."""
    result = pathling_ctx.evaluate_fhirpath(
        "Patient", PATIENT_JSON, "%flag", variables={"flag": True}
    )

    assert result == {
        "results": [{"type": "boolean", "value": True}],
        "expectedReturnType": "boolean",
        "traces": [],
    }


def test_without_variables(pathling_ctx):
    """Omitting variables evaluates normally."""
    result = pathling_ctx.evaluate_fhirpath("Patient", PATIENT_JSON, "active")

    assert result == {
        "results": [{"type": "boolean", "value": True}],
        "expectedReturnType": "boolean",
        "traces": [],
    }


# ========== Trace output ==========


def test_trace_boolean(pathling_ctx):
    """Tracing a boolean expression returns the traced value with correct
    label and type."""
    result = pathling_ctx.evaluate_fhirpath(
        "Patient", PATIENT_JSON, "active.trace('flag')"
    )

    assert result == {
        "results": [{"type": "boolean", "value": True}],
        "expectedReturnType": "boolean",
        "traces": [
            {
                "label": "flag",
                "values": [{"type": "boolean", "value": True}],
            }
        ],
    }


def test_trace_entry_has_no_type_key(pathling_ctx):
    """Each trace entry has label and values keys, but no top-level type key."""
    result = pathling_ctx.evaluate_fhirpath(
        "Patient", PATIENT_JSON, "active.trace('flag')"
    )

    for trace in result["traces"]:
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
    assert result["results"] == [{"type": "string", "value": "Smith"}]
    labels = [t["label"] for t in result["traces"]]
    assert "before" in labels
    assert "after" in labels


def test_no_trace_returns_empty_list(pathling_ctx):
    """An expression without trace() returns an empty traces list."""
    result = pathling_ctx.evaluate_fhirpath("Patient", PATIENT_JSON, "active")

    assert result["traces"] == []


# ========== Error handling ==========


def test_invalid_expression(pathling_ctx):
    """An invalid FHIRPath expression raises an exception."""
    with pytest.raises(Exception):
        pathling_ctx.evaluate_fhirpath("Patient", PATIENT_JSON, "!!invalid!!")


def test_malformed_json(pathling_ctx):
    """Malformed JSON resource raises an exception."""
    with pytest.raises(Exception):
        pathling_ctx.evaluate_fhirpath("Patient", "{not valid json}", "active")
