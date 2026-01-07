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

import type {
  AllPatientsExportKickOffOptions,
  BulkExportCancelOptions,
  BulkExportDownloadOptions,
  BulkExportKickOffResult,
  BulkExportStatusOptions,
  BulkExportStatusResult,
  ExportManifest,
  GroupExportKickOffOptions,
  PatientExportKickOffOptions,
  SystemExportKickOffOptions,
} from "../types/api";
import { buildHeaders, buildUrl, checkResponse, resolveUrl } from "./utils";

/**
 * Builds query parameters for bulk export operations.
 */
function buildExportParams(
  options: SystemExportKickOffOptions | AllPatientsExportKickOffOptions,
): Record<string, string> {
  const params: Record<string, string> = {};

  if (options.types && options.types.length > 0) {
    params._type = options.types.join(",");
  }

  if (options.since) {
    params._since = options.since;
  }

  if (options.until) {
    params._until = options.until;
  }

  if (options.elements) {
    params._elements = options.elements;
  }

  return params;
}

/**
 * Kicks off an export and returns the polling URL.
 */
async function kickOffExport(
  baseUrl: string,
  path: string,
  options: SystemExportKickOffOptions,
): Promise<BulkExportKickOffResult> {
  const params = buildExportParams(options);
  const url = buildUrl(
    baseUrl,
    path,
    Object.keys(params).length > 0 ? params : undefined,
  );
  const headers = buildHeaders({
    accessToken: options.accessToken,
    prefer: "respond-async",
  });

  const response = await fetch(url, {
    method: "GET",
    headers,
  });

  await checkResponse(response, "Export kick-off");

  if (response.status !== 202) {
    const errorBody = await response.text();
    throw new Error(
      `Export kick-off failed: ${response.status} - ${errorBody}`,
    );
  }

  const contentLocation = response.headers.get("Content-Location");
  if (!contentLocation) {
    throw new Error("Export kick-off failed: No Content-Location header");
  }

  return { pollingUrl: contentLocation };
}

/**
 * Kicks off a system-level export of all resources.
 *
 * @param baseUrl - The FHIR server base URL.
 * @param options - Export options including resource types and since date.
 * @returns The polling URL for checking export status.
 * @throws {UnauthorizedError} When the request receives a 401 response.
 * @throws {Error} For other non-successful responses.
 *
 * @example
 * const { pollingUrl } = await systemExportKickOff("https://example.com/fhir", {
 *   types: ["Patient", "Observation"],
 *   accessToken: "token123"
 * });
 */
export async function systemExportKickOff(
  baseUrl: string,
  options: SystemExportKickOffOptions,
): Promise<BulkExportKickOffResult> {
  return kickOffExport(baseUrl, "/$export", options);
}

/**
 * Kicks off an export of all patient compartment resources.
 *
 * @param baseUrl - The FHIR server base URL.
 * @param options - Export options including resource types and since date.
 * @returns The polling URL for checking export status.
 * @throws {UnauthorizedError} When the request receives a 401 response.
 * @throws {Error} For other non-successful responses.
 */
export async function allPatientsExportKickOff(
  baseUrl: string,
  options: AllPatientsExportKickOffOptions,
): Promise<BulkExportKickOffResult> {
  return kickOffExport(baseUrl, "/Patient/$export", options);
}

/**
 * Kicks off an export for a specific patient's compartment resources.
 *
 * @param baseUrl - The FHIR server base URL.
 * @param options - Export options including patient ID, resource types, and since date.
 * @returns The polling URL for checking export status.
 * @throws {UnauthorizedError} When the request receives a 401 response.
 * @throws {Error} For other non-successful responses.
 */
export async function patientExportKickOff(
  baseUrl: string,
  options: PatientExportKickOffOptions,
): Promise<BulkExportKickOffResult> {
  return kickOffExport(
    baseUrl,
    `/Patient/${options.patientId}/$export`,
    options,
  );
}

/**
 * Kicks off an export for a group's member resources.
 *
 * @param baseUrl - The FHIR server base URL.
 * @param options - Export options including group ID, resource types, and since date.
 * @returns The polling URL for checking export status.
 * @throws {UnauthorizedError} When the request receives a 401 response.
 * @throws {Error} For other non-successful responses.
 */
export async function groupExportKickOff(
  baseUrl: string,
  options: GroupExportKickOffOptions,
): Promise<BulkExportKickOffResult> {
  return kickOffExport(baseUrl, `/Group/${options.groupId}/$export`, options);
}

/**
 * Checks the status of a bulk export operation.
 *
 * @param baseUrl - The FHIR server base URL.
 * @param options - Status options including polling URL.
 * @returns The export status with optional progress or completed manifest.
 * @throws {UnauthorizedError} When the request receives a 401 response.
 * @throws {Error} For other non-successful responses.
 *
 * @example
 * const status = await bulkExportStatus("https://example.com/fhir", {
 *   pollingUrl: "https://example.com/$job?id=abc",
 *   accessToken: "token123"
 * });
 * if (status.status === "complete") {
 *   console.log(status.manifest);
 * }
 */
export async function bulkExportStatus(
  baseUrl: string,
  options: BulkExportStatusOptions,
): Promise<BulkExportStatusResult> {
  const url = resolveUrl(baseUrl, options.pollingUrl);
  const headers = buildHeaders({ accessToken: options.accessToken });

  const response = await fetch(url, {
    method: "GET",
    headers,
  });

  // 202 indicates export still in progress.
  if (response.status === 202) {
    const progressHeader = response.headers.get("X-Progress");
    return {
      status: "in-progress",
      progress: progressHeader ?? undefined,
    };
  }

  // 200 indicates export complete.
  if (response.status === 200) {
    const manifest = (await response.json()) as ExportManifest;

    return {
      status: "complete",
      manifest,
    };
  }

  await checkResponse(response, "Export status");

  // This should never be reached, but TypeScript needs it.
  throw new Error("Unexpected response");
}

/**
 * Cancels a bulk export operation.
 *
 * @param baseUrl - The FHIR server base URL.
 * @param options - Cancel options including polling URL.
 * @throws {UnauthorizedError} When the request receives a 401 response.
 * @throws {Error} For other non-successful responses.
 *
 * @example
 * await bulkExportCancel("https://example.com/fhir", {
 *   pollingUrl: "https://example.com/$job?id=abc",
 *   accessToken: "token123"
 * });
 */
export async function bulkExportCancel(
  baseUrl: string,
  options: BulkExportCancelOptions,
): Promise<void> {
  const url = resolveUrl(baseUrl, options.pollingUrl);
  const headers = buildHeaders({ accessToken: options.accessToken });

  const response = await fetch(url, {
    method: "DELETE",
    headers,
  });

  await checkResponse(response, "Export cancel");
}

/**
 * Downloads an exported file from a bulk export operation.
 *
 * @param baseUrl - The FHIR server base URL (unused but included for consistency).
 * @param options - Download options including file URL.
 * @returns A ReadableStream of the file contents.
 * @throws {UnauthorizedError} When the request receives a 401 response.
 * @throws {Error} For other non-successful responses.
 *
 * @example
 * const stream = await bulkExportDownload("https://example.com/fhir", {
 *   fileUrl: "https://example.com/files/patient.ndjson",
 *   accessToken: "token123"
 * });
 */
export async function bulkExportDownload(
  _baseUrl: string,
  options: BulkExportDownloadOptions,
): Promise<ReadableStream> {
  const headers = buildHeaders({
    accessToken: options.accessToken,
    accept: "application/fhir+ndjson",
  });

  const response = await fetch(options.fileUrl, {
    method: "GET",
    headers,
  });

  await checkResponse(response, "Export download");

  if (!response.body) {
    throw new Error("Export download failed: No response body");
  }

  return response.body;
}
