/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

/**
 * @author John Grimes
 */

import { AxiosResponse } from "axios";
import {
  buildClient,
  FHIR_JSON_CONTENT_TYPE,
  MaybeAuthenticated,
  MaybeValidated,
} from "./common.js";
import { FhirBulkResult } from "./export.js";

export type CheckJobStatusParams = {
  endpoint: string;
  statusUrl: string;
  authenticationEnabled: boolean;
} & MaybeAuthenticated;

export type ImportStatusResult = unknown;

/**
 * Checks the status of a FHIR async job.
 *
 * @return the final result of the operation
 * @throws a "Job in progress" error if the job is still running
 * @see https://hl7.org/fhir/uv/bulkdata/export/index.html#endpoint---system-level-export
 */
export async function checkJobStatus<
  UnvalidatedType = unknown,
  ResultType = unknown
>(
  options: CheckJobStatusParams & MaybeValidated<UnvalidatedType, ResultType>
): Promise<ResultType> {
  const { statusUrl } = options,
    client = await buildClient<UnvalidatedType, ResultType>(options);

  const response = await client.get<ResultType>(statusUrl, {
    headers: { Accept: FHIR_JSON_CONTENT_TYPE },
  });

  if (response.status === 200) {
    console.debug(response.data);
    return response.data;
  } else if (response.status === 202) {
    const progress = response.headers["x-progress"];
    const message = progress ? progress : "(no progress message)";
    throw new JobInProgressError(`Job in progress: ${message}`);
  } else {
    throw new Error(
      `Unexpected status: ${response.status} ${response.statusText}`
    );
  }
}

export async function checkExportJobStatus(
  options: CheckJobStatusParams
): Promise<FhirBulkResult> {
  return checkJobStatus<Partial<FhirBulkResult>, FhirBulkResult>({
    ...options,
    validator: validateExportResponse,
  });
}

function validateExportResponse(
  response: AxiosResponse<Partial<FhirBulkResult>>
): AxiosResponse<FhirBulkResult> {
  if (
    response.status === 200 &&
    (!response.data.output || !Array.isArray(response.data.output))
  ) {
    console.log(response.data);
    throw new Error("FHIR bulk export did not contain outputs");
  }
  return response as AxiosResponse<FhirBulkResult>;
}

/**
 * Checks the status of a Pathling import job.
 *
 * @return the final result of the operation
 * @throws a "Job in progress" error if the job is still running
 * @see https://hl7.org/fhir/uv/bulkdata/export/index.html#endpoint---system-level-export
 */
export async function checkImportJobStatus(
  options: CheckJobStatusParams
): Promise<ImportStatusResult> {
  const { endpoint, statusUrl, authenticationEnabled } = options;
  let checkStatusOptions: CheckJobStatusParams = {
    endpoint,
    statusUrl,
    authenticationEnabled: false,
  };

  if (authenticationEnabled) {
    const { clientId, clientSecret, scopes } = options;
    checkStatusOptions = {
      ...checkStatusOptions,
      authenticationEnabled: true,
      clientId,
      clientSecret,
      scopes: scopes ?? "user/*.write",
    };
  }

  return checkJobStatus(checkStatusOptions);
}

export class JobInProgressError extends Error {
  constructor(message: string) {
    super(message);
    this.name = "JobInProgressError";
  }
}
