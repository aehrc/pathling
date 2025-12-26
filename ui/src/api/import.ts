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

import type { Parameters } from "fhir/r4";
import type {
  ImportKickOffOptions,
  ImportResult,
  ImportPnpKickOffOptions,
} from "../types/api";
import {
  buildHeaders,
  buildUrl,
  checkResponse,
  extractJobIdFromUrl,
} from "./utils";

/**
 * Kicks off a bulk import operation.
 *
 * @param baseUrl - The FHIR server base URL.
 * @param options - Import options including input files, format, and mode.
 * @returns The job ID for tracking the import operation.
 * @throws {UnauthorizedError} When the request receives a 401 response.
 * @throws {Error} For other non-successful responses.
 *
 * @example
 * const { jobId } = await importKickOff("https://example.com/fhir", {
 *   input: [{ type: "Patient", url: "s3://bucket/patient.ndjson" }],
 *   inputFormat: "application/fhir+ndjson",
 *   mode: "overwrite",
 *   accessToken: "token123"
 * });
 */
export async function importKickOff(
  baseUrl: string,
  options: ImportKickOffOptions,
): Promise<ImportResult> {
  const url = buildUrl(baseUrl, "/$import");
  const headers = buildHeaders({
    accessToken: options.accessToken,
    contentType: "application/json",
    prefer: "respond-async",
  });

  const body = {
    input: options.input,
    inputFormat: options.inputFormat,
    mode: options.mode,
  };

  const response = await fetch(url, {
    method: "POST",
    headers,
    body: JSON.stringify(body),
  });

  await checkResponse(response, "Import kick-off");

  if (response.status !== 202) {
    const errorBody = await response.text();
    throw new Error(
      `Import kick-off failed: ${response.status} - ${errorBody}`,
    );
  }

  const contentLocation = response.headers.get("Content-Location");
  if (!contentLocation) {
    throw new Error("Import kick-off failed: No Content-Location header");
  }

  const jobId = extractJobIdFromUrl(contentLocation);
  return { jobId };
}

/**
 * Builds a FHIR Parameters resource for the import-pnp operation.
 */
function buildPnpParameters(options: ImportPnpKickOffOptions): Parameters {
  const parameters: Parameters = {
    resourceType: "Parameters",
    parameter: [{ name: "exportUrl", valueUrl: options.exportUrl }],
  };

  if (options.exportType) {
    parameters.parameter!.push({
      name: "exportType",
      valueCode: options.exportType,
    });
  }

  if (options.saveMode) {
    parameters.parameter!.push({
      name: "saveMode",
      valueCode: options.saveMode,
    });
  }

  if (options.inputFormat) {
    parameters.parameter!.push({
      name: "inputFormat",
      valueCode: options.inputFormat,
    });
  }

  return parameters;
}

/**
 * Kicks off a ping-and-pull import operation.
 *
 * @param baseUrl - The FHIR server base URL.
 * @param options - Import options including export URL and optional settings.
 * @returns The job ID for tracking the import operation.
 * @throws {UnauthorizedError} When the request receives a 401 response.
 * @throws {Error} For other non-successful responses.
 *
 * @example
 * const { jobId } = await importPnpKickOff("https://example.com/fhir", {
 *   exportUrl: "https://source.com/$export",
 *   exportType: "dynamic",
 *   saveMode: "merge",
 *   accessToken: "token123"
 * });
 */
export async function importPnpKickOff(
  baseUrl: string,
  options: ImportPnpKickOffOptions,
): Promise<ImportResult> {
  const url = buildUrl(baseUrl, "/$import-pnp");
  const headers = buildHeaders({
    accessToken: options.accessToken,
    contentType: "application/fhir+json",
    prefer: "respond-async",
  });

  const parameters = buildPnpParameters(options);

  const response = await fetch(url, {
    method: "POST",
    headers,
    body: JSON.stringify(parameters),
  });

  await checkResponse(response, "Import PnP kick-off");

  if (response.status !== 202) {
    const errorBody = await response.text();
    throw new Error(
      `Import PnP kick-off failed: ${response.status} - ${errorBody}`,
    );
  }

  const contentLocation = response.headers.get("Content-Location");
  if (!contentLocation) {
    throw new Error("Import PnP kick-off failed: No Content-Location header");
  }

  const jobId = extractJobIdFromUrl(contentLocation);
  return { jobId };
}
