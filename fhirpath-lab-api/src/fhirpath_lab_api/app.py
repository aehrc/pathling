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

"""Flask application implementing the FHIRPath Lab server API.

Author: John Grimes
"""

from __future__ import annotations

import json
import logging
import os
from typing import Optional

from flask import Flask, Response, request
from flask_cors import CORS

from fhirpath_lab_api.parameters import (
    build_operation_outcome,
    build_response_parameters,
    parse_input_parameters,
)

logger = logging.getLogger(__name__)


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
            ctx = PathlingContext.create()
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

    @app.route("/$fhirpath-r4", methods=["POST"])
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

    # Evaluate the expression.
    try:
        pc = get_context()
        result = pc.evaluate_fhirpath(
            resource_type=params.resource_type,
            resource_json=json.dumps(params.resource),
            fhirpath_expression=params.expression,
            context_expression=params.context,
            variables=params.variables,
        )
    except Exception as e:
        logger.exception("Error evaluating FHIRPath expression")
        return _error_response(500, "exception", f"Error evaluating expression: {e}")

    # Build the response.
    evaluator_string = f"Pathling {pc.version()} (R4)"
    response_body = build_response_parameters(
        evaluator_string=evaluator_string,
        expression=params.expression,
        resource=params.resource,
        expected_return_type=result["expectedReturnType"],
        results=result["results"],
        context=params.context,
    )

    return Response(
        json.dumps(response_body),
        status=200,
        content_type="application/fhir+json",
    )


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
