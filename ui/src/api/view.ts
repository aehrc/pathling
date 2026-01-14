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

import { buildHeaders, buildUrl, checkResponse } from "./utils";

import type {
  ViewExportDownloadOptions,
  ViewExportKickOffOptions,
  ViewExportResult,
  ViewRunOptions,
  ViewRunStoredOptions,
} from "../types/api";
import type { Parameters, ParametersParameter } from "fhir/r4";

/**
 * Runs a ViewDefinition and returns the results as a stream.
 *
 * @param baseUrl - The FHIR server base URL.
 * @param options - View run options including the ViewDefinition.
 * @returns A ReadableStream of the view results (NDJSON or CSV).
 * @throws {UnauthorizedError} When the request receives a 401 response.
 * @throws {Error} For other non-successful responses.
 *
 * @example
 * const stream = await viewRun("https://example.com/fhir", {
 *   viewDefinition: { resourceType: "ViewDefinition", ... },
 *   format: "ndjson",
 *   limit: 100,
 *   accessToken: "token123"
 * });
 */
export async function viewRun(
  baseUrl: string,
  options: ViewRunOptions,
): Promise<ReadableStream> {
  const url = buildUrl(baseUrl, "/ViewDefinition/$run");
  const headers = buildHeaders({
    accessToken: options.accessToken,
    contentType: "application/fhir+json",
    accept: "application/x-ndjson",
  });

  const parameter: ParametersParameter[] = [
    {
      name: "viewResource",
      resource:
        options.viewDefinition as Parameters["parameter"] extends (infer T)[]
          ? T extends { resource?: infer R }
            ? R
            : never
          : never,
    },
  ];

  if (options.format) {
    parameter.push({ name: "_format", valueString: options.format });
  }

  if (options.limit !== undefined) {
    parameter.push({ name: "_limit", valueInteger: options.limit });
  }

  if (options.header !== undefined) {
    parameter.push({ name: "_header", valueBoolean: options.header });
  }

  const body: Parameters = {
    resourceType: "Parameters",
    parameter,
  };

  const response = await fetch(url, {
    method: "POST",
    headers,
    body: JSON.stringify(body),
  });

  await checkResponse(response, "View run");

  if (!response.body) {
    throw new Error("View run failed: No response body");
  }

  return response.body;
}

/**
 * Runs a stored ViewDefinition by ID and returns the results as a stream.
 *
 * @param baseUrl - The FHIR server base URL.
 * @param options - View run options including the ViewDefinition ID.
 * @returns A ReadableStream of the view results (NDJSON or CSV).
 * @throws {UnauthorizedError} When the request receives a 401 response.
 * @throws {Error} For other non-successful responses.
 *
 * @example
 * const stream = await viewRunStored("https://example.com/fhir", {
 *   viewDefinitionId: "view-123",
 *   format: "csv",
 *   limit: 50,
 *   accessToken: "token123"
 * });
 */
export async function viewRunStored(
  baseUrl: string,
  options: ViewRunStoredOptions,
): Promise<ReadableStream> {
  const params: Record<string, string> = {};

  if (options.format) {
    params._format = options.format;
  }

  if (options.limit !== undefined) {
    params._limit = String(options.limit);
  }

  if (options.header !== undefined) {
    params._header = String(options.header);
  }

  const url = buildUrl(
    baseUrl,
    `/ViewDefinition/${options.viewDefinitionId}/$run`,
    Object.keys(params).length > 0 ? params : undefined,
  );
  const headers = buildHeaders({
    accessToken: options.accessToken,
    accept: "application/x-ndjson",
  });

  const response = await fetch(url, {
    method: "GET",
    headers,
  });

  await checkResponse(response, "View run");

  if (!response.body) {
    throw new Error("View run failed: No response body");
  }

  return response.body;
}

/**
 * Kicks off an async view export operation.
 *
 * @param baseUrl - The FHIR server base URL.
 * @param options - View export options including views to export.
 * @returns The polling URL for tracking the export operation.
 * @throws {UnauthorizedError} When the request receives a 401 response.
 * @throws {Error} For other non-successful responses.
 *
 * @example
 * const { pollingUrl } = await viewExportKickOff("https://example.com/fhir", {
 *   views: [{ viewDefinition: { ... }, name: "my-view" }],
 *   format: "parquet",
 *   accessToken: "token123"
 * });
 */
export async function viewExportKickOff(
  baseUrl: string,
  options: ViewExportKickOffOptions,
): Promise<ViewExportResult> {
  const url = buildUrl(baseUrl, "/$viewdefinition-export");
  const headers = buildHeaders({
    accessToken: options.accessToken,
    contentType: "application/fhir+json",
    prefer: "respond-async",
  });

  const parameter: ParametersParameter[] = [];

  // Add each view as a parameter.
  for (const view of options.views) {
    const viewParam: ParametersParameter = {
      name: "view",
      part: [
        {
          name: "viewResource",
          resource:
            view.viewDefinition as Parameters["parameter"] extends (infer T)[]
              ? T extends { resource?: infer R }
                ? R
                : never
              : never,
        },
      ],
    };
    if (view.name) {
      viewParam.part!.push({ name: "name", valueString: view.name });
    }
    parameter.push(viewParam);
  }

  if (options.format) {
    parameter.push({ name: "_format", valueString: options.format });
  }

  if (options.header !== undefined) {
    parameter.push({ name: "_header", valueBoolean: options.header });
  }

  const body: Parameters = {
    resourceType: "Parameters",
    parameter,
  };

  const response = await fetch(url, {
    method: "POST",
    headers,
    body: JSON.stringify(body),
  });

  await checkResponse(response, "View export kick-off");

  if (response.status !== 202) {
    const errorBody = await response.text();
    throw new Error(
      `View export kick-off failed: ${response.status} - ${errorBody}`,
    );
  }

  const contentLocation = response.headers.get("Content-Location");
  if (!contentLocation) {
    throw new Error("View export kick-off failed: No Content-Location header");
  }

  return { pollingUrl: contentLocation };
}

/**
 * Downloads a file from a view export operation.
 *
 * @param baseUrl - The FHIR server base URL.
 * @param options - Download options including job ID and file name.
 * @returns A ReadableStream of the file contents.
 * @throws {UnauthorizedError} When the request receives a 401 response.
 * @throws {Error} For other non-successful responses.
 *
 * @example
 * const stream = await viewExportDownload("https://example.com/fhir", {
 *   jobId: "view-export-123",
 *   fileName: "patient-view.parquet",
 *   accessToken: "token123"
 * });
 */
export async function viewExportDownload(
  baseUrl: string,
  options: ViewExportDownloadOptions,
): Promise<ReadableStream> {
  const url = buildUrl(
    baseUrl,
    `/$job-output/${options.jobId}/${options.fileName}`,
  );
  const headers = buildHeaders({ accessToken: options.accessToken });

  const response = await fetch(url, {
    method: "GET",
    headers,
  });

  await checkResponse(response, "View export download");

  if (!response.body) {
    throw new Error("View export download failed: No response body");
  }

  return response.body;
}
