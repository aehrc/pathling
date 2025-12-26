/**
 * Service for FHIR import operations.
 *
 * @author John Grimes
 */

import type { Binary, Parameters } from "fhir/r4";
import { UnauthorizedError } from "../types/errors";
import type { ImportManifest, ImportRequest } from "../types/import";
import type { ImportPnpRequest } from "../types/importPnp";
import type { PollResult } from "../types/polling";

interface KickOffResult {
  jobId: string;
  pollUrl: string;
}

/**
 * Kicks off an import job by posting a manifest to the $import endpoint.
 */
export async function kickOffImport(
  fhirBaseUrl: string,
  accessToken: string | undefined,
  request: ImportRequest,
): Promise<KickOffResult> {
  const url = `${fhirBaseUrl}/$import`;

  const headers: HeadersInit = {
    "Content-Type": "application/json",
    Accept: "application/fhir+json",
    Prefer: "respond-async",
  };
  if (accessToken) {
    headers.Authorization = `Bearer ${accessToken}`;
  }

  const response = await fetch(url, {
    method: "POST",
    headers,
    body: JSON.stringify(request),
  });

  if (response.status === 401) {
    throw new UnauthorizedError();
  }
  if (response.status !== 202) {
    const errorBody = await response.text();
    throw new Error(
      `Import kick-off failed: ${response.status} - ${errorBody}`,
    );
  }

  const contentLocation = response.headers.get("Content-Location");
  if (!contentLocation) {
    throw new Error(
      "Import kick-off failed: No Content-Location header received",
    );
  }

  const jobId = extractJobId(contentLocation);
  return { jobId, pollUrl: contentLocation };
}

/**
 * Polls the import job status endpoint.
 */
export async function pollImportStatus(
  fhirBaseUrl: string,
  accessToken: string | undefined,
  pollUrl: string,
): Promise<PollResult<ImportManifest>> {
  // Handle both absolute and relative URLs.
  const url = pollUrl.startsWith("http") ? pollUrl : `${fhirBaseUrl}${pollUrl}`;

  const headers: HeadersInit = {
    Accept: "application/fhir+json",
  };
  if (accessToken) {
    headers.Authorization = `Bearer ${accessToken}`;
  }

  const response = await fetch(url, { headers });

  if (response.status === 202) {
    const progressHeader = response.headers.get("X-Progress");
    const progress = progressHeader ? parseProgress(progressHeader) : undefined;
    return { status: "in_progress", progress };
  }

  if (response.status === 200) {
    const responseBody: unknown = await response.json();

    // Handle Binary resource wrapper - some servers return the manifest wrapped in a
    // FHIR Binary resource with base64-encoded data.
    let manifest: ImportManifest;
    if (isBinaryResource(responseBody)) {
      const decodedData = atob(responseBody.data);
      manifest = JSON.parse(decodedData) as ImportManifest;
    } else if (isParametersResource(responseBody)) {
      // Parse FHIR Parameters response format.
      manifest = parseParametersManifest(responseBody);
    } else {
      manifest = responseBody as ImportManifest;
    }

    return { status: "completed", manifest };
  }

  if (response.status === 401) {
    throw new UnauthorizedError();
  }
  const errorBody = await response.text();
  throw new Error(`Import poll failed: ${response.status} - ${errorBody}`);
}

/**
 * Cancels an import job.
 */
export async function cancelImport(
  fhirBaseUrl: string,
  accessToken: string | undefined,
  pollUrl: string,
): Promise<void> {
  const url = pollUrl.startsWith("http") ? pollUrl : `${fhirBaseUrl}${pollUrl}`;

  const headers: HeadersInit = {};
  if (accessToken) {
    headers.Authorization = `Bearer ${accessToken}`;
  }

  const response = await fetch(url, {
    method: "DELETE",
    headers,
  });

  if (response.status === 401) {
    throw new UnauthorizedError();
  }
  if (!response.ok) {
    const errorBody = await response.text();
    throw new Error(
      `Import cancellation failed: ${response.status} - ${errorBody}`,
    );
  }
}

/**
 * Kicks off a ping and pull import job by posting parameters to the $import-pnp endpoint.
 */
export async function kickOffImportPnp(
  fhirBaseUrl: string,
  accessToken: string | undefined,
  request: ImportPnpRequest,
): Promise<KickOffResult> {
  const url = `${fhirBaseUrl}/$import-pnp`;

  const headers: HeadersInit = {
    "Content-Type": "application/fhir+json",
    Accept: "application/fhir+json",
    Prefer: "respond-async",
  };
  if (accessToken) {
    headers.Authorization = `Bearer ${accessToken}`;
  }

  const parameters = buildPnpParameters(request);

  const response = await fetch(url, {
    method: "POST",
    headers,
    body: JSON.stringify(parameters),
  });

  if (response.status === 401) {
    throw new UnauthorizedError();
  }
  if (response.status !== 202) {
    const errorBody = await response.text();
    throw new Error(
      `Import PnP kick-off failed: ${response.status} - ${errorBody}`,
    );
  }

  const contentLocation = response.headers.get("Content-Location");
  if (!contentLocation) {
    throw new Error(
      "Import PnP kick-off failed: No Content-Location header received",
    );
  }

  const jobId = extractJobId(contentLocation);
  return { jobId, pollUrl: contentLocation };
}

/**
 * Builds a FHIR Parameters resource from an ImportPnpRequest.
 */
function buildPnpParameters(request: ImportPnpRequest): Parameters {
  return {
    resourceType: "Parameters",
    parameter: [
      { name: "exportUrl", valueUrl: request.exportUrl },
      { name: "exportType", valueCoding: { code: request.exportType } },
      { name: "mode", valueCoding: { code: request.saveMode } },
      { name: "inputFormat", valueCoding: { code: request.inputFormat } },
    ],
  };
}

/**
 * Extracts the job ID from a poll URL.
 */
function extractJobId(pollUrl: string): string {
  const url = new URL(pollUrl, window.location.origin);
  const jobId = url.searchParams.get("id");
  if (!jobId) {
    // Try to extract from path if not in query params.
    const match = pollUrl.match(/job[s]?\/([a-f0-9-]+)/i);
    if (match) {
      return match[1];
    }
    throw new Error("Could not extract job ID from poll URL");
  }
  return jobId;
}

/**
 * Parses the X-Progress header value (e.g., "45%" -> 45).
 */
function parseProgress(progressHeader: string): number {
  const match = progressHeader.match(/(\d+)/);
  if (match) {
    return parseInt(match[1], 10);
  }
  return 0;
}

/**
 * Parses a FHIR Parameters response into an ImportManifest.
 */
function parseParametersManifest(params: Parameters): ImportManifest {
  const manifest: ImportManifest = {
    transactionTime: "",
    request: "",
    output: [],
  };

  for (const param of params.parameter ?? []) {
    if (param.name === "transactionTime" && param.valueInstant) {
      manifest.transactionTime = param.valueInstant;
    } else if (param.name === "request" && param.valueUrl) {
      manifest.request = param.valueUrl;
    } else if (param.name === "output" && param.part) {
      for (const part of param.part) {
        if (part.name === "inputUrl" && part.valueUrl) {
          manifest.output.push({ inputUrl: part.valueUrl });
        }
      }
    }
  }

  return manifest;
}

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
 * Type guard to check if a response is a FHIR Parameters resource.
 */
function isParametersResource(value: unknown): value is Parameters {
  return (
    typeof value === "object" &&
    value !== null &&
    "resourceType" in value &&
    (value as { resourceType: string }).resourceType === "Parameters"
  );
}
