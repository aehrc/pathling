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
 *
 * Author: John Grimes
 */

import type { Binary, Parameters, ParametersParameter } from "fhir/r4";
import type {
  BulkSubmitOptions,
  BulkSubmitResult,
  BulkSubmitStatusOptions,
  BulkSubmitStatusResult,
  BulkSubmitDownloadOptions,
  BulkSubmitManifest,
} from "../types/api";
import {
  buildHeaders,
  buildUrl,
  checkResponse,
  extractJobIdFromUrl,
} from "./utils";

/**
 * Type guard to check if a response is a FHIR Binary resource with data.
 */
function isBinaryResource(value: unknown): value is Binary & { data: string } {
  return (
    typeof value === "object" &&
    value !== null &&
    "resourceType" in value &&
    (value as { resourceType: string }).resourceType === "Binary" &&
    "data" in value &&
    typeof (value as { data: unknown }).data === "string"
  );
}

/**
 * Builds FHIR Parameters resource for bulk submit request.
 */
function buildSubmitParameters(options: BulkSubmitOptions): Parameters {
  const parameter: ParametersParameter[] = [
    { name: "submissionId", valueString: options.submissionId },
    {
      name: "submitter",
      valueIdentifier: {
        system: options.submitter.system,
        value: options.submitter.value,
      },
    },
    {
      name: "submissionStatus",
      valueCoding: { code: options.submissionStatus },
    },
  ];

  // manifestUrl is optional (not required for abort).
  if (options.manifestUrl) {
    parameter.push({ name: "manifestUrl", valueUrl: options.manifestUrl });
  }

  if (options.fhirBaseUrl) {
    parameter.push({ name: "fhirBaseUrl", valueUrl: options.fhirBaseUrl });
  }

  if (options.replacesManifestUrl) {
    parameter.push({
      name: "replacesManifestUrl",
      valueUrl: options.replacesManifestUrl,
    });
  }

  if (options.oauthMetadataUrl) {
    parameter.push({
      name: "oauthMetadataUrl",
      valueUrl: options.oauthMetadataUrl,
    });
  }

  if (options.metadata) {
    for (const [key, value] of Object.entries(options.metadata)) {
      parameter.push({
        name: "metadata",
        part: [
          { name: "key", valueString: key },
          { name: "value", valueString: value },
        ],
      });
    }
  }

  if (options.fileRequestHeaders) {
    for (const [key, value] of Object.entries(options.fileRequestHeaders)) {
      parameter.push({
        name: "fileRequestHeader",
        part: [
          { name: "name", valueString: key },
          { name: "value", valueString: value },
        ],
      });
    }
  }

  return { resourceType: "Parameters", parameter };
}

/**
 * Extracts result from Parameters response.
 */
function parseSubmitResponse(params: Parameters): BulkSubmitResult {
  let submissionId = "";
  let status = "";

  for (const param of params.parameter ?? []) {
    if (param.name === "submissionId" && param.valueString) {
      submissionId = param.valueString;
    } else if (param.name === "status" && param.valueString) {
      status = param.valueString;
    }
  }

  return { submissionId, status };
}

/**
 * Creates or updates a bulk submission.
 *
 * @param baseUrl - The FHIR server base URL.
 * @param options - Submit options including submission details.
 * @returns The submission result with ID and status.
 * @throws {UnauthorizedError} When the request receives a 401 response.
 * @throws {Error} For other non-successful responses.
 *
 * @example
 * const result = await bulkSubmit("https://example.com/fhir", {
 *   submitter: { system: "http://example.org", value: "org-123" },
 *   submissionId: "sub-456",
 *   submissionStatus: "in-progress",
 *   manifestUrl: "https://source.com/manifest.json",
 *   accessToken: "token123"
 * });
 */
export async function bulkSubmit(
  baseUrl: string,
  options: BulkSubmitOptions,
): Promise<BulkSubmitResult> {
  const url = buildUrl(baseUrl, "/$bulk-submit");
  const headers = buildHeaders({
    accessToken: options.accessToken,
    contentType: "application/fhir+json",
  });

  const parameters = buildSubmitParameters(options);

  const response = await fetch(url, {
    method: "POST",
    headers,
    body: JSON.stringify(parameters),
  });

  await checkResponse(response, "Bulk submit");

  const responseBody = (await response.json()) as Parameters;
  return parseSubmitResponse(responseBody);
}

/**
 * Checks the status of a bulk submission.
 *
 * @param baseUrl - The FHIR server base URL.
 * @param options - Status options including submitter and submission ID.
 * @returns The submission status with optional progress or completed manifest.
 * @throws {UnauthorizedError} When the request receives a 401 response.
 * @throws {Error} For other non-successful responses.
 */
export async function bulkSubmitStatus(
  baseUrl: string,
  options: BulkSubmitStatusOptions,
): Promise<BulkSubmitStatusResult> {
  const url = buildUrl(baseUrl, "/$bulk-submit-status");
  const headers = buildHeaders({
    accessToken: options.accessToken,
    contentType: "application/fhir+json",
    prefer: "respond-async",
  });

  const parameters: Parameters = {
    resourceType: "Parameters",
    parameter: [
      { name: "submissionId", valueString: options.submissionId },
      {
        name: "submitter",
        valueIdentifier: {
          system: options.submitter.system,
          value: options.submitter.value,
        },
      },
    ],
  };

  const response = await fetch(url, {
    method: "POST",
    headers,
    body: JSON.stringify(parameters),
  });

  // 202 with Content-Location indicates job started.
  if (response.status === 202) {
    const contentLocation = response.headers.get("Content-Location");
    const progressHeader = response.headers.get("X-Progress");

    if (contentLocation) {
      const jobId = extractJobIdFromUrl(contentLocation);
      return {
        status: "in-progress",
        progress: progressHeader ?? undefined,
        jobId,
      };
    }

    // 202 without Content-Location means job not started yet.
    return {
      status: "in-progress",
      progress: progressHeader ?? undefined,
    };
  }

  // 200 indicates already complete.
  if (response.status === 200) {
    const responseBody: unknown = await response.json();

    let manifest: BulkSubmitManifest;
    if (isBinaryResource(responseBody)) {
      const decodedData = atob(responseBody.data);
      manifest = JSON.parse(decodedData) as BulkSubmitManifest;
    } else {
      manifest = responseBody as BulkSubmitManifest;
    }

    return {
      status: "completed",
      manifest,
    };
  }

  await checkResponse(response, "Bulk submit status");

  throw new Error("Unexpected response");
}

/**
 * Downloads a file from a bulk submission.
 *
 * @param baseUrl - The FHIR server base URL.
 * @param options - Download options including submission ID and file name.
 * @returns A ReadableStream of the file contents.
 * @throws {UnauthorizedError} When the request receives a 401 response.
 * @throws {Error} For other non-successful responses.
 */
export async function bulkSubmitDownload(
  baseUrl: string,
  options: BulkSubmitDownloadOptions,
): Promise<ReadableStream> {
  const url = buildUrl(
    baseUrl,
    `/$bulk-submit-download/${options.submissionId}/${options.fileName}`,
  );
  const headers = buildHeaders({ accessToken: options.accessToken });

  const response = await fetch(url, {
    method: "GET",
    headers,
  });

  await checkResponse(response, "Bulk submit download");

  if (!response.body) {
    throw new Error("Bulk submit download failed: No response body");
  }

  return response.body;
}
