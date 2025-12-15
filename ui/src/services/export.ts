/**
 * Service for FHIR bulk export operations.
 *
 * @author John Grimes
 */

import type { Binary } from "fhir/r4";
import { UnauthorizedError } from "../types/errors";
import type { ExportManifest, ExportRequest } from "../types/export";

interface KickOffResult {
  jobId: string;
  pollUrl: string;
}

interface PollResult {
  status: "in_progress" | "completed";
  progress?: number;
  manifest?: ExportManifest;
}

/**
 * Builds the export URL based on the export request parameters.
 */
function buildExportUrl(request: ExportRequest): string {
  let basePath: string;

  switch (request.level) {
    case "system":
      basePath = "/$export";
      break;
    case "patient-type":
      basePath = "/Patient/$export";
      break;
    case "patient-instance":
      basePath = `/Patient/${request.patientId}/$export`;
      break;
    case "group":
      basePath = `/Group/${request.groupId}/$export`;
      break;
  }

  const params = new URLSearchParams();

  if (request.resourceTypes && request.resourceTypes.length > 0) {
    params.set("_type", request.resourceTypes.join(","));
  }

  if (request.since) {
    params.set("_since", request.since);
  }

  if (request.until) {
    params.set("_until", request.until);
  }

  if (request.elements) {
    params.set("_elements", request.elements);
  }

  const queryString = params.toString();
  return queryString ? `${basePath}?${queryString}` : basePath;
}

/**
 * Kicks off a bulk export job using fetch for better header access.
 */
export async function kickOffExportWithFetch(
  fhirBaseUrl: string,
  accessToken: string | undefined,
  request: ExportRequest,
): Promise<KickOffResult> {
  const url = `${fhirBaseUrl}${buildExportUrl(request)}`;

  const headers: HeadersInit = {
    Accept: "application/fhir+json",
    Prefer: "respond-async",
  };
  if (accessToken) {
    headers.Authorization = `Bearer ${accessToken}`;
  }

  const response = await fetch(url, {
    method: "GET",
    headers,
  });

  if (response.status === 401) {
    throw new UnauthorizedError();
  }
  if (response.status !== 202) {
    const errorBody = await response.text();
    throw new Error(
      `Export kick-off failed: ${response.status} - ${errorBody}`,
    );
  }

  const contentLocation = response.headers.get("Content-Location");
  if (!contentLocation) {
    throw new Error(
      "Export kick-off failed: No Content-Location header received",
    );
  }

  const jobId = extractJobId(contentLocation);
  return { jobId, pollUrl: contentLocation };
}

/**
 * Polls the job status endpoint.
 */
export async function pollJobStatus(
  fhirBaseUrl: string,
  accessToken: string | undefined,
  pollUrl: string,
): Promise<PollResult> {
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

  // Handle 200 (fresh response). Note: 304 responses have no body.
  if (response.status === 200) {
    const responseBody: unknown = await response.json();

    // Handle Binary resource wrapper - some servers return the manifest wrapped in a
    // FHIR Binary resource with base64-encoded data.
    let manifest: ExportManifest;
    if (isBinaryResource(responseBody)) {
      const decodedData = atob(responseBody.data);
      manifest = JSON.parse(decodedData) as ExportManifest;
    } else {
      manifest = responseBody as ExportManifest;
    }

    return { status: "completed", manifest };
  }

  if (response.status === 401) {
    throw new UnauthorizedError();
  }
  const errorBody = await response.text();
  throw new Error(`Job poll failed: ${response.status} - ${errorBody}`);
}

/**
 * Cancels an export job.
 */
export async function cancelJob(
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
      `Job cancellation failed: ${response.status} - ${errorBody}`,
    );
  }
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
