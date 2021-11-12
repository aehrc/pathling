/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

import { IOperationOutcome } from "@ahryman40k/ts-fhir-types/lib/R4";
import { AxiosError, AxiosResponse } from "axios";

interface OpOutcomeSourceError extends AxiosError {
  response: AxiosResponse;
}

/**
 * Custom error class for representing an error returned from a FHIR API as an
 * OperationOutcome resource.
 */
export class OpOutcomeError extends Error {
  resource: IOperationOutcome;

  constructor(error: OpOutcomeSourceError) {
    const opOutcome: IOperationOutcome = error.response.data;
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
