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

"""Functions for parsing FHIR Parameters requests and constructing responses.

Author: John Grimes
"""

from __future__ import annotations

import json
from typing import Any, Optional

# The extension URL for representing complex FHIR types as JSON strings.
JSON_VALUE_EXTENSION_URL = "http://fhir.forms-lab.com/StructureDefinition/json-value"

# FHIR primitive types that can be represented directly as Parameters value types.
PRIMITIVE_TYPE_MAP = {
    "boolean": "valueBoolean",
    "string": "valueString",
    "code": "valueCode",
    "date": "valueDate",
    "dateTime": "valueDateTime",
    "instant": "valueInstant",
    "time": "valueTime",
    "integer": "valueInteger",
    "unsignedInt": "valueUnsignedInt",
    "positiveInt": "valuePositiveInt",
    "decimal": "valueDecimal",
    "uri": "valueUri",
    "url": "valueUrl",
    "canonical": "valueCanonical",
    "oid": "valueOid",
    "id": "valueId",
    "uuid": "valueUuid",
    "markdown": "valueMarkdown",
    "base64Binary": "valueBase64Binary",
}


class InputParameters:
    """Parsed input parameters from a FHIR Parameters request.

    :param expression: the FHIRPath expression to evaluate
    :param resource: the FHIR resource to evaluate against
    :param resource_type: the resource type name
    :param context: an optional context expression
    :param variables: optional named variables
    """

    def __init__(
        self,
        expression: str,
        resource: dict,
        resource_type: str,
        context: Optional[str] = None,
        variables: Optional[dict[str, Any]] = None,
    ):
        self.expression = expression
        self.resource = resource
        self.resource_type = resource_type
        self.context = context
        self.variables = variables


def parse_input_parameters(body: dict) -> InputParameters:
    """Parses a FHIR Parameters request body and extracts the input parameters.

    :param body: the parsed JSON body of the request
    :return: an InputParameters instance with the extracted values
    :raises ValueError: if required parameters are missing or the body is invalid
    """
    if body.get("resourceType") != "Parameters":
        raise ValueError("Expected FHIR Parameters resource")

    parameters = body.get("parameter", [])
    expression = None
    resource = None
    resource_type = None
    context = None
    variables = None

    for param in parameters:
        name = param.get("name")
        if name == "expression":
            expression = param.get("valueString")
        elif name == "resource":
            resource = _extract_resource(param)
            if resource is not None:
                resource_type = resource.get("resourceType")
        elif name == "context":
            context = param.get("valueString")
        elif name == "variables":
            variables = _extract_variables(param)

    if expression is None:
        raise ValueError("Missing required parameter: expression")
    if resource is None:
        raise ValueError("Missing required parameter: resource")
    if resource_type is None:
        raise ValueError("Resource is missing resourceType")

    return InputParameters(
        expression=expression,
        resource=resource,
        resource_type=resource_type,
        context=context,
        variables=variables,
    )


def _extract_resource(param: dict) -> Optional[dict]:
    """Extracts the resource from a parameter, supporting both direct embedding and
    the json-value extension.

    :param param: the parameter dict
    :return: the resource as a dict, or None
    """
    # Direct resource embedding.
    resource = param.get("resource")
    if resource is not None:
        return resource

    # Resource via json-value extension.
    for ext in param.get("extension", []):
        if ext.get("url") == JSON_VALUE_EXTENSION_URL:
            json_string = ext.get("valueString")
            if json_string is not None:
                return json.loads(json_string)

    return None


def _extract_variables(param: dict) -> Optional[dict[str, Any]]:
    """Extracts variables from a variables parameter.

    :param param: the variables parameter dict
    :return: a dict of variable names to values, or None
    """
    parts = param.get("part", [])
    if not parts:
        return None

    variables = {}
    for part in parts:
        name = part.get("name")
        if name is None:
            continue
        # Extract the value from whichever value[x] property is present.
        value = _extract_value(part)
        variables[name] = value
    return variables


def _extract_value(part: dict) -> Any:
    """Extracts the value from a Parameters part, checking all value[x] properties.

    :param part: the part dict
    :return: the extracted value, or None
    """
    for key, value in part.items():
        if key.startswith("value") and key != "valueString":
            return value
    # Check valueString last to avoid conflicting with other value types.
    return part.get("valueString")


def build_response_parameters(
    evaluator_string: str,
    expression: str,
    resource: dict,
    expected_return_type: str,
    results: list[dict],
    context: Optional[str] = None,
) -> dict:
    """Constructs a FHIR Parameters response from evaluation results.

    :param evaluator_string: the evaluator identification string
    :param expression: the original expression
    :param resource: the original resource
    :param expected_return_type: the statically inferred return type
    :param results: the list of typed result values
    :param context: the optional context expression
    :return: a FHIR Parameters resource dict
    """
    # Build the parameters metadata part.
    params_parts = [
        {"name": "evaluator", "valueString": evaluator_string},
        {"name": "expression", "valueString": expression},
        {"name": "resource", "resource": resource},
    ]

    if context is not None:
        params_parts.append({"name": "context", "valueString": context})

    params_parts.append(
        {"name": "expectedReturnType", "valueString": expected_return_type}
    )

    output_parameters: list[dict] = [{"name": "parameters", "part": params_parts}]

    # Build result parts.
    if results:
        result_parts = []
        for typed_value in results:
            result_part = _build_result_part(typed_value)
            result_parts.append(result_part)
        output_parameters.append({"name": "result", "part": result_parts})

    return {"resourceType": "Parameters", "parameter": output_parameters}


def _build_result_part(typed_value: dict) -> dict:
    """Builds a single result part from a typed value.

    :param typed_value: a dict with 'type' and 'value' keys
    :return: a FHIR Parameters part dict
    """
    type_name = typed_value["type"]
    value = typed_value["value"]

    # Check if this is a primitive type with a direct value[x] mapping.
    value_key = PRIMITIVE_TYPE_MAP.get(type_name)
    if value_key is not None:
        return {"name": type_name, value_key: value}

    # For complex types, use the json-value extension.
    json_string = value if isinstance(value, str) else json.dumps(value)
    return {
        "name": type_name,
        "extension": [
            {
                "url": JSON_VALUE_EXTENSION_URL,
                "valueString": json_string,
            }
        ],
    }


def build_operation_outcome(
    severity: str, code: str, message: str, diagnostics: Optional[str] = None
) -> dict:
    """Constructs a FHIR OperationOutcome resource.

    :param severity: the issue severity (error, fatal, warning, information)
    :param code: the issue type code
    :param message: the human-readable error message
    :param diagnostics: optional additional diagnostic information
    :return: a FHIR OperationOutcome resource dict
    """
    issue: dict[str, Any] = {
        "severity": severity,
        "code": code,
        "details": {"text": message},
    }
    if diagnostics is not None:
        issue["diagnostics"] = diagnostics

    return {"resourceType": "OperationOutcome", "issue": [issue]}
