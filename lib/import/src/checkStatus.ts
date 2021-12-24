/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

/**
 * @author John Grimes
 */

import { buildAuthenticatedClient, FHIR_JSON_CONTENT_TYPE } from "./common.js";
import { AxiosResponse } from "axios";

export interface CheckImportJobStatusParams {
  endpoint: string;
  clientId: string;
  clientSecret: string;
  statusUrl: string;
}

export interface CheckExportJobStatusParams extends CheckImportJobStatusParams {
  scopes: string;
}

export type CheckStatusResult = object;

/**
 * Checks the status of a FHIR async job.
 *
 * @return the final result of the operation
 * @throws a "Job in progress" error if the job is still running
 * @see https://hl7.org/fhir/uv/bulkdata/export/index.html#endpoint---system-level-export
 */
export async function checkExportJobStatus({
  endpoint,
  clientId,
  clientSecret,
  scopes,
  statusUrl,
}: CheckExportJobStatusParams): Promise<CheckStatusResult> {
  const client = await buildAuthenticatedClient(
    endpoint,
    clientId,
    clientSecret,
    scopes
  );

  const response = await client.get<undefined, AxiosResponse<object>>(
    statusUrl,
    {
      headers: { Accept: FHIR_JSON_CONTENT_TYPE },
    }
  );

  if (response.status === 200) {
    return response.data;
  } else if (response.status === 202) {
    const progress = response.headers["x-progress"];
    const message = progress ? progress : "(no progress message)";
    throw new JobInProgressError(`Job in progress: ${message}`);
  } else {
    throw `Unexpected status: ${response.status} ${response.statusText}`;
  }
}

/**
 * Checks the status of a Pathling import job.
 *
 * @return the final result of the operation
 * @throws a "Job in progress" error if the job is still running
 * @see https://hl7.org/fhir/uv/bulkdata/export/index.html#endpoint---system-level-export
 */
export async function checkImportJobStatus(
  params: CheckImportJobStatusParams
): Promise<object> {
  return checkExportJobStatus({ ...params, scopes: "user/*.write" });
}

class JobInProgressError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "JobInProgressError";
  }
}
