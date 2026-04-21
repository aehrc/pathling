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

"""Flask application implementing the FHIRPath Lab server API.

Author: John Grimes
"""

from __future__ import annotations

import json
import logging
import os
import re
from typing import Optional

from flask import Flask, Response, request
from flask_cors import CORS
from py4j.protocol import Py4JJavaError

from fhirpath_lab_api.parameters import (
    InputParameters,
    build_operation_outcome,
    build_response_parameters,
    parse_input_parameters,
)

logger = logging.getLogger(__name__)

# Upper bound on the number of context elements permitted in a grouped
# evaluation. Evaluating the main expression once per context element requires
# N additional round-trips through the Pathling engine, so a sanity cap
# prevents accidental denial-of-service on contexts like ``Bundle.entry``.
MAX_CONTEXT_ELEMENTS = 100


class _ContextTooLargeError(Exception):
    """Raised when the context expression yields more elements than
    MAX_CONTEXT_ELEMENTS."""

    def __init__(self, count: int, limit: int):
        super().__init__(
            f"Context expression produced {count} elements, exceeding the "
            f"limit of {limit}. Please narrow the context expression."
        )
        self.count = count
        self.limit = limit


def create_app(pathling_context=None) -> Flask:
    """Creates and configures the Flask application.

    :param pathling_context: an optional pre-configured PathlingContext for testing.
        If None, one will be created at startup.
    :return: a configured Flask application instance
    """
    app = Flask(__name__)

    # Configure CORS origins from the environment variable.
    origins_value = os.environ.get("CORS_ALLOWED_ORIGINS", "")
    cors_origins = [
        origin.strip() for origin in origins_value.split(",") if origin.strip()
    ]
    if cors_origins:
        CORS(app, origins=cors_origins)

    # Store the PathlingContext in app config.
    # Lazy initialisation: create at first request if not provided.
    app.config["pathling_context"] = pathling_context

    def _get_pathling_context():
        """Returns the PathlingContext, creating it on first use."""
        ctx = app.config.get("pathling_context")
        if ctx is None:
            from pathling import PathlingContext

            logger.info("Initialising PathlingContext...")
            ctx = PathlingContext.create(enable_extensions=True)
            app.config["pathling_context"] = ctx
            logger.info("PathlingContext initialised.")
        return ctx

    @app.route("/healthcheck", methods=["GET"])
    def healthcheck():
        """Health check endpoint for liveness and readiness probes."""
        return Response(
            json.dumps({"status": "ok"}),
            status=200,
            content_type="application/json",
        )

    @app.route("/fhir/$fhirpath", methods=["POST"])
    def evaluate_fhirpath():
        """Evaluates a FHIRPath expression against a FHIR resource.

        Accepts a FHIR Parameters resource and returns a FHIR Parameters resource
        containing the evaluation results.
        """
        return _handle_evaluate(_get_pathling_context)

    return app


def _handle_evaluate(get_context) -> Response:
    """Handles the FHIRPath evaluation request.

    :param get_context: a callable that returns the PathlingContext
    :return: a Flask Response
    """
    # Parse the request body.
    try:
        body = request.get_json(force=True)
    except Exception:
        return _error_response(400, "invalid", "Request body is empty or malformed")

    if body is None:
        return _error_response(400, "invalid", "Request body is empty or malformed")

    # Extract input parameters.
    try:
        params = parse_input_parameters(body)
    except ValueError as e:
        return _error_response(400, "required", str(e))

    # An empty expression evaluates to an empty collection.
    if not params.expression.strip():
        response_body = build_response_parameters(
            evaluator_string="",
            expression=params.expression,
            resource=params.resource,
            expected_return_type="",
            results=[],
            context=params.context,
        )
        return Response(
            json.dumps(response_body),
            status=200,
            content_type="application/fhir+json",
        )

    # Evaluate the expression.
    use_grouped = params.context is not None and params.context.strip() != ""
    try:
        pc = get_context()
        if use_grouped:
            grouped = _evaluate_grouped(pc, params)
        else:
            result = pc.evaluate_fhirpath(
                resource_type=params.resource_type,
                resource_json=json.dumps(params.resource),
                fhirpath_expression=params.expression,
                context_expression=params.context,
                variables=params.variables,
            )
    except _ContextTooLargeError as e:
        return _error_response(400, "too-costly", str(e))
    except Py4JJavaError as e:
        full_message = str(e)
        logger.exception("Error evaluating FHIRPath expression")
        friendly = _extract_friendly_message(full_message)
        if friendly:
            return _error_response(500, "exception", friendly, diagnostics=full_message)
        return _error_response(500, "exception", full_message)
    except Exception as e:
        logger.exception("Error evaluating FHIRPath expression")
        return _error_response(500, "exception", f"Error evaluating expression: {e}")

    # Build the response.
    evaluator_string = f"Pathling {pc.version()} (R4)"
    if use_grouped:
        response_body = build_response_parameters(
            evaluator_string=evaluator_string,
            expression=params.expression,
            resource=params.resource,
            expected_return_type=grouped["expected_return_type"],
            context=params.context,
            grouped_results=grouped["groups"],
        )
    else:
        response_body = build_response_parameters(
            evaluator_string=evaluator_string,
            expression=params.expression,
            resource=params.resource,
            expected_return_type=result["expectedReturnType"],
            results=result["results"],
            context=params.context,
            traces=result.get("traces", []),
        )

    return Response(
        json.dumps(response_body),
        status=200,
        content_type="application/fhir+json",
    )


def _evaluate_grouped(pc, params: InputParameters) -> dict:
    """Evaluates the main expression once per element of the context expression.

    First evaluates the context expression alone to determine its cardinality,
    then evaluates the main expression N times, each time scoping the context
    to a single element using the FHIRPath indexer ``(context)[i]``. Each
    iteration's results and trace entries are collected into a separate group
    labelled ``"<context>[i]"``.

    This results in 1 + N calls through the Pathling engine. The cardinality
    is capped by :data:`MAX_CONTEXT_ELEMENTS`.

    :param pc: the PathlingContext to use for evaluation
    :param params: the parsed input parameters; ``params.context`` must be
        non-empty
    :return: a dict with keys ``groups`` (list of
        ``(label, results, traces)`` tuples), ``expected_return_type`` (string,
        taken from the first iteration or empty when the context yields no
        elements), and ``context_count`` (int)
    :raises _ContextTooLargeError: if the context expression yields more than
        :data:`MAX_CONTEXT_ELEMENTS` elements
    """
    resource_json = json.dumps(params.resource)

    count_result = pc.evaluate_fhirpath(
        resource_type=params.resource_type,
        resource_json=resource_json,
        fhirpath_expression=params.context,
        context_expression=None,
        variables=params.variables,
    )
    context_count = len(count_result["results"])

    if context_count > MAX_CONTEXT_ELEMENTS:
        raise _ContextTooLargeError(context_count, MAX_CONTEXT_ELEMENTS)

    groups: list[tuple[str, list[dict], list[dict]]] = []
    expected_return_type = ""
    for i in range(context_count):
        iter_result = pc.evaluate_fhirpath(
            resource_type=params.resource_type,
            resource_json=resource_json,
            fhirpath_expression=params.expression,
            context_expression=f"({params.context})[{i}]",
            variables=params.variables,
        )
        label = f"{params.context}[{i}]"
        groups.append((label, iter_result["results"], iter_result.get("traces", [])))
        if i == 0:
            expected_return_type = iter_result["expectedReturnType"]

    return {
        "groups": groups,
        "expected_return_type": expected_return_type,
        "context_count": context_count,
    }


# Module-level WSGI application instance for use by production WSGI servers
# (e.g. gunicorn -w 1 fhirpath_lab_api.app:application).
application = create_app()


def _error_response(
    status: int,
    code: str,
    message: str,
    diagnostics: Optional[str] = None,
) -> Response:
    """Creates an error response with a FHIR OperationOutcome.

    :param status: the HTTP status code
    :param code: the OperationOutcome issue code
    :param message: the error message
    :param diagnostics: optional diagnostics
    :return: a Flask Response
    """
    outcome = build_operation_outcome("error", code, message, diagnostics)
    return Response(
        json.dumps(outcome),
        status=status,
        content_type="application/fhir+json",
    )


# Matches a fully-qualified Java exception class name (ending in Error or Exception)
# followed by a colon and the exception message, stopping before the stack trace.
_JAVA_EXCEPTION_RE = re.compile(
    r"([\w.$]+(?:Error|Exception)):\s*(.+?)(?=\n?\tat |\Z)",
    re.DOTALL,
)


def _extract_friendly_message(error_str: str) -> Optional[str]:
    """Extracts a human-readable message from a Py4J Java error string.

    Parses the Java exception class name and message from the error string,
    returning a concise form like "ClassName: message". Returns None if the
    string does not match the expected pattern.

    :param error_str: the full string representation of the Py4J error
    :return: a friendly message, or None if the pattern does not match
    """
    match = _JAVA_EXCEPTION_RE.search(error_str)
    if not match:
        return None
    fqcn = match.group(1)
    message = match.group(2).strip()
    # Use only the short class name (after the last dot).
    short_name = fqcn.rsplit(".", 1)[-1]
    return f"{short_name}: {message}"
