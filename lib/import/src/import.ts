/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

/**
 * @author John Grimes
 */

import {
  IOperationOutcome,
  IParameters,
} from "@ahryman40k/ts-fhir-types/lib/R4";
import {
  buildAuthenticatedClient,
  FHIR_JSON_CONTENT_TYPE,
  getStatusUrl,
} from "./common.js";

export interface ImportParams {
  endpoint: string;
  clientId: string;
  clientSecret: string;
  parameters: IParameters;
}

export type ImportResult = string | null;

/**
 * Imports a set of files from S3 into Pathling.
 *
 * @return either a job status URL or the {@link IOperationOutcome} resulting from the operation,
 *   depending on whether async processing is enabled on the Pathling instance
 * @see https://pathling.csiro.au/docs/import.html
 */
export async function importFromParameters({
  endpoint,
  clientId,
  clientSecret,
  parameters,
}: ImportParams): Promise<ImportResult> {
  if (
    parameters.parameter &&
    parameters.parameter.filter((p) => p.name === "source").length < 1
  ) {
    console.info("Nothing to import");
    return null;
  }

  const client = await buildAuthenticatedClient(
    endpoint,
    clientId,
    clientSecret,
    "user/*.write"
  );

  console.info("Initiating import request: %j", parameters);
  const response = await client.post<IOperationOutcome>("$import", parameters, {
    headers: {
      Accept: FHIR_JSON_CONTENT_TYPE,
      "Content-Type": FHIR_JSON_CONTENT_TYPE,
      Prefer: "respond-async",
    },
  });

  if (response.status === 202) {
    const statusUrl = getStatusUrl(response);
    console.info("Import operation returned status URL: %s", statusUrl);
    return statusUrl;
  } else {
    throw `Unexpected status: ${response.status} ${response.statusText}`;
  }
}
