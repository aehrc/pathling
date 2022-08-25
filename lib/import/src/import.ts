/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

/**
 * @author John Grimes
 */

import { OperationOutcome, Parameters } from "fhir/r4";
import {
  buildClient,
  FHIR_JSON_CONTENT_TYPE,
  getStatusUrl,
  MaybeAuthenticated,
  validateAsyncResponse,
} from "./common.js";

export type ImportParams = {
  endpoint: string;
  parameters: Parameters;
} & MaybeAuthenticated;

export type ImportResult = string | null;

/**
 * Imports a set of files from S3 into Pathling.
 *
 * @return either a job status URL or `null`, which indicates that there was
 * nothing to import
 * @see https://pathling.csiro.au/docs/import.html
 */
export async function importFromParameters(
  options: ImportParams
): Promise<ImportResult> {
  const { parameters } = options;
  if (
    parameters.parameter &&
    parameters.parameter.filter((p) => p.name === "source").length < 1
  ) {
    console.info("Nothing to import");
    return null;
  }
  const client = await buildClient({
    ...options,
    validator: validateAsyncResponse,
  });

  console.info("Initiating import request: %j", parameters);
  const response = await client.post<OperationOutcome>("$import", parameters, {
    headers: {
      Accept: FHIR_JSON_CONTENT_TYPE,
      "Content-Type": FHIR_JSON_CONTENT_TYPE,
      Prefer: "respond-async",
    },
  });

  const statusUrl = getStatusUrl(response);
  console.info("Import operation returned status URL: %s", statusUrl);
  return statusUrl;
}
