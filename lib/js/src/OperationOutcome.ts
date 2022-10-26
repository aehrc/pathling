/*
 * Copyright 2022 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import { AxiosError, AxiosResponse } from "axios";
import { OperationOutcome } from "fhir/r4";

interface OpOutcomeSourceError extends AxiosError {
  response: AxiosResponse;
}

/**
 * Custom error class for representing an error returned from a FHIR API as an
 * OperationOutcome resource.
 */
export class OpOutcomeError extends Error {
  resource: OperationOutcome;

  constructor(error: OpOutcomeSourceError) {
    const opOutcome: OperationOutcome = error.response.data;
    const issue = opOutcome.issue[0],
      message =
        issue.code === "login"
          ? "You are not authorized to access this server"
          : issue.diagnostics;
    super(message);
    this.name = "OpOutcomeError";
    this.resource = opOutcome;
    if ("captureStackTrace" in Error) {
      // @ts-ignore
      Error.captureStackTrace(this, OpOutcomeError);
    }
  }
}

/**
 * Create an {@link OpOutcomeError} from a response that may contain an
 * OperationOutcome resource.
 */
export const buildResponseError = (error: AxiosError): Error => {
  if (
    !error.response ||
    !error.response.data ||
    !responseIsOpOutcome(error.response.data)
  ) {
    return error;
  } else {
    return new OpOutcomeError(error as OpOutcomeSourceError);
  }
};

/**
 * Check if a response contains an OperationOutcome resource.
 */
export const responseIsOpOutcome = (parsed: any) =>
  parsed.resourceType === "OperationOutcome" &&
  parsed.issue &&
  parsed.issue.length > 0;
