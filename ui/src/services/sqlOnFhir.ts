/**
 * Service for SQL on FHIR operations.
 *
 * @author John Grimes
 */

import type { Binary } from "fhir/r4";
import { UnauthorizedError } from "../types/errors";
import type { PollResult } from "../types/polling";
import type {
  CreateViewDefinitionResult,
  ViewDefinitionResult,
  ViewDefinitionSummary,
} from "../types/sqlOnFhir";
import type { ViewExportManifest, ViewExportRequest } from "../types/viewExport";

interface KickOffResult {
  jobId: string;
  pollUrl: string;
}

/**
 * Parses an NDJSON response into an array of objects.
 */
function parseNdjsonResponse(ndjson: string): Record<string, unknown>[] {
  return ndjson
    .split("\n")
    .filter((line) => line.trim() !== "")
    .map((line) => JSON.parse(line));
}

/**
 * Extracts column names from the first row of results.
 */
function extractColumns(rows: Record<string, unknown>[]): string[] {
  if (rows.length === 0) return [];
  return Object.keys(rows[0]);
}

/**
 * Fetches available ViewDefinitions from the server.
 */
export async function searchViewDefinitions(
  fhirBaseUrl: string,
  accessToken: string | undefined,
): Promise<ViewDefinitionSummary[]> {
  const headers: HeadersInit = {
    Accept: "application/fhir+json",
  };
  if (accessToken) {
    headers.Authorization = `Bearer ${accessToken}`;
  }

  const response = await fetch(`${fhirBaseUrl}/ViewDefinition`, { headers });

  if (response.status === 401) {
    throw new UnauthorizedError();
  }
  if (!response.ok) {
    const errorBody = await response.text();
    throw new Error(`Failed to fetch view definitions: ${response.status} - ${errorBody}`);
  }

  const bundle = await response.json();
  const viewDefinitions: ViewDefinitionSummary[] =
    bundle.entry?.map((e: { resource: { id: string; name?: string } }) => ({
      id: e.resource.id,
      name: e.resource.name || e.resource.id,
      json: JSON.stringify(e.resource, null, 2),
    })) ?? [];

  return viewDefinitions;
}

/**
 * Executes a stored ViewDefinition by ID.
 */
export async function executeStoredViewDefinition(
  fhirBaseUrl: string,
  accessToken: string | undefined,
  viewDefinitionId: string,
): Promise<ViewDefinitionResult> {
  const headers: HeadersInit = {
    Accept: "application/x-ndjson",
  };
  if (accessToken) {
    headers.Authorization = `Bearer ${accessToken}`;
  }

  const response = await fetch(
    `${fhirBaseUrl}/ViewDefinition/${viewDefinitionId}/$run?_limit=10`,
    { headers },
  );

  if (response.status === 401) {
    throw new UnauthorizedError();
  }
  if (!response.ok) {
    const errorBody = await response.text();
    throw new Error(`Failed to execute view definition: ${response.status} - ${errorBody}`);
  }

  const ndjsonText = await response.text();
  const rows = parseNdjsonResponse(ndjsonText);
  const columns = extractColumns(rows);

  return { columns, rows };
}

/**
 * Executes an inline ViewDefinition provided as JSON.
 */
export async function executeInlineViewDefinition(
  fhirBaseUrl: string,
  accessToken: string | undefined,
  viewDefinitionJson: string,
): Promise<ViewDefinitionResult> {
  // Parse the ViewDefinition JSON to validate it.
  let viewResource: unknown;
  try {
    viewResource = JSON.parse(viewDefinitionJson);
  } catch {
    throw new Error("Invalid JSON: Please enter valid view definition JSON");
  }

  const parameters = {
    resourceType: "Parameters",
    parameter: [
      {
        name: "viewResource",
        resource: viewResource,
      },
      {
        name: "_limit",
        valueInteger: 10,
      },
    ],
  };

  const headers: HeadersInit = {
    "Content-Type": "application/fhir+json",
    Accept: "application/x-ndjson",
  };
  if (accessToken) {
    headers.Authorization = `Bearer ${accessToken}`;
  }

  const response = await fetch(`${fhirBaseUrl}/ViewDefinition/$run`, {
    method: "POST",
    headers,
    body: JSON.stringify(parameters),
  });

  if (response.status === 401) {
    throw new UnauthorizedError();
  }
  if (!response.ok) {
    const errorBody = await response.text();
    throw new Error(`Failed to execute view definition: ${response.status} - ${errorBody}`);
  }

  const ndjsonText = await response.text();
  const rows = parseNdjsonResponse(ndjsonText);
  const columns = extractColumns(rows);

  return { columns, rows };
}

/**
 * Creates a ViewDefinition resource on the server.
 */
export async function createViewDefinition(
  fhirBaseUrl: string,
  accessToken: string | undefined,
  viewDefinitionJson: string,
): Promise<CreateViewDefinitionResult> {
  // Parse and validate the JSON.
  let viewResource: { resourceType?: string; name?: string; id?: string };
  try {
    viewResource = JSON.parse(viewDefinitionJson);
  } catch {
    throw new Error("Invalid JSON: Please enter valid view definition JSON");
  }

  // Validate it's a ViewDefinition resource.
  if (viewResource.resourceType !== "ViewDefinition") {
    throw new Error("Invalid resource: resourceType must be 'ViewDefinition'");
  }

  const headers: HeadersInit = {
    "Content-Type": "application/fhir+json",
    Accept: "application/fhir+json",
  };
  if (accessToken) {
    headers.Authorization = `Bearer ${accessToken}`;
  }

  const response = await fetch(`${fhirBaseUrl}/ViewDefinition`, {
    method: "POST",
    headers,
    body: viewDefinitionJson,
  });

  if (response.status === 401) {
    throw new UnauthorizedError();
  }
  if (!response.ok) {
    const errorBody = await response.text();
    throw new Error(`Failed to save view definition: ${response.status} - ${errorBody}`);
  }

  const createdResource = (await response.json()) as {
    id: string;
    name?: string;
  };
  return {
    id: createdResource.id,
    name: createdResource.name || createdResource.id,
  };
}

/**
 * Kicks off a view export job.
 */
export async function kickOffViewExport(
  fhirBaseUrl: string,
  accessToken: string | undefined,
  request: ViewExportRequest,
): Promise<KickOffResult> {
  // Build the Parameters resource.
  let viewResource: unknown;
  if (request.viewDefinitionId) {
    // For stored views, we need to fetch the ViewDefinition first.
    const headers: HeadersInit = {
      Accept: "application/fhir+json",
    };
    if (accessToken) {
      headers.Authorization = `Bearer ${accessToken}`;
    }
    const viewResponse = await fetch(
      `${fhirBaseUrl}/ViewDefinition/${request.viewDefinitionId}`,
      { headers },
    );
    if (viewResponse.status === 401) {
      throw new UnauthorizedError();
    }
    if (!viewResponse.ok) {
      const errorBody = await viewResponse.text();
      throw new Error(`Failed to fetch view definition: ${viewResponse.status} - ${errorBody}`);
    }
    viewResource = await viewResponse.json();
  } else if (request.viewDefinitionJson) {
    try {
      viewResource = JSON.parse(request.viewDefinitionJson);
    } catch {
      throw new Error("Invalid JSON: Please enter valid view definition JSON");
    }
  } else {
    throw new Error("Invalid request: missing view definition ID or JSON");
  }

  const parameters = {
    resourceType: "Parameters",
    parameter: [
      {
        name: "view.viewResource",
        resource: viewResource,
      },
      {
        name: "_format",
        valueString: request.format,
      },
    ],
  };

  const headers: HeadersInit = {
    "Content-Type": "application/fhir+json",
    Accept: "application/fhir+json",
    Prefer: "respond-async",
  };
  if (accessToken) {
    headers.Authorization = `Bearer ${accessToken}`;
  }

  const response = await fetch(`${fhirBaseUrl}/$viewdefinition-export`, {
    method: "POST",
    headers,
    body: JSON.stringify(parameters),
  });

  if (response.status === 401) {
    throw new UnauthorizedError();
  }
  if (response.status !== 202) {
    const errorBody = await response.text();
    throw new Error(`View export kick-off failed: ${response.status} - ${errorBody}`);
  }

  const contentLocation = response.headers.get("Content-Location");
  if (!contentLocation) {
    throw new Error("View export kick-off failed: No Content-Location header received");
  }

  const jobId = extractJobId(contentLocation);
  return { jobId, pollUrl: contentLocation };
}

/**
 * Polls the view export job status endpoint.
 */
export async function pollViewExportJobStatus(
  fhirBaseUrl: string,
  accessToken: string | undefined,
  pollUrl: string,
): Promise<PollResult<ViewExportManifest>> {
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

    // Handle Binary resource wrapper.
    let manifest: ViewExportManifest;
    if (isBinaryResource(responseBody)) {
      const decodedData = atob(responseBody.data);
      manifest = JSON.parse(decodedData) as ViewExportManifest;
    } else {
      manifest = responseBody as ViewExportManifest;
    }

    return { status: "completed", manifest };
  }

  if (response.status === 401) {
    throw new UnauthorizedError();
  }
  const errorBody = await response.text();
  throw new Error(`View export job poll failed: ${response.status} - ${errorBody}`);
}

/**
 * Cancels a view export job.
 */
export async function cancelViewExportJob(
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
    throw new Error(`View export job cancellation failed: ${response.status} - ${errorBody}`);
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
