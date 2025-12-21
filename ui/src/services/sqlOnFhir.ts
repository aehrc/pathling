/**
 * Service for SQL on FHIR operations.
 *
 * @author John Grimes
 */

import { UnauthorizedError } from "../types/errors";
import type { ViewDefinitionResult, ViewDefinitionSummary } from "../types/sqlOnFhir";

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
