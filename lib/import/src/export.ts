/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

/**
 * @author John Grimes
 */

import {
  buildClient,
  FHIR_JSON_CONTENT_TYPE,
  FHIR_NDJSON_CONTENT_TYPE,
  getStatusUrl,
  MaybeAuthenticated,
  validateAsyncResponse,
} from "./common.js";

export type ExportParams = {
  endpoint: string;
  resourceTypes?: string;
  since: string;
} & MaybeAuthenticated;

export type ExportResult = string;

export interface FhirBulkResult {
  output: FhirBulkOutput[];
}

export interface FhirBulkOutput {
  type: string;
  url: string;
}

/**
 * Kicks off a FHIR bulk export operation.
 *
 * @return the job status endpoint
 * @see https://hl7.org/fhir/uv/bulkdata/export/index.html#endpoint---system-level-export
 */
export async function fhirBulkExport(
  options: ExportParams
): Promise<ExportResult> {
  const { resourceTypes, since } = options;
  const client = await buildClient({
    ...options,
    validator: validateAsyncResponse,
  });

  let params = {
    _outputFormat: FHIR_NDJSON_CONTENT_TYPE,
    _since: since,
    _type: resourceTypes,
  };
  console.info("Initiating export request: %j", params);
  const response = await client.get<FhirBulkResult>("/$export", {
    headers: {
      Accept: FHIR_JSON_CONTENT_TYPE,
      Prefer: "respond-async",
    },
    params: params,
  });

  const statusUrl = getStatusUrl(response);
  console.info("Export operation returned status URL: %s", statusUrl);
  return statusUrl;
}
