/**
 * Service for FHIR resource search operations.
 *
 * @author John Grimes
 */

import type { Bundle, Resource } from "fhir/r4";
import { UnauthorizedError } from "../types/errors";
import type { SearchRequest } from "../types/search";

/**
 * Result of a FHIR search operation.
 */
export interface SearchResult {
  resources: Resource[];
  total: number | undefined;
  bundle: Bundle;
}

/**
 * Executes a FHIR search using the fhirPath query with filter expressions.
 */
export async function searchResources(
  fhirBaseUrl: string,
  accessToken: string | undefined,
  request: SearchRequest,
): Promise<SearchResult> {
  const params = new URLSearchParams();
  params.set("_query", "fhirPath");
  params.set("_count", "10");

  for (const filter of request.filters) {
    const trimmed = filter.trim();
    if (trimmed) {
      params.append("filter", trimmed);
    }
  }

  const url = `${fhirBaseUrl}/${request.resourceType}?${params.toString()}`;

  const headers: HeadersInit = {
    Accept: "application/fhir+json",
  };
  if (accessToken) {
    headers.Authorization = `Bearer ${accessToken}`;
  }

  const response = await fetch(url, { headers });

  if (response.status === 401) {
    throw new UnauthorizedError();
  }
  if (!response.ok) {
    const errorBody = await response.text();
    throw new Error(`Search failed: ${response.status} - ${errorBody}`);
  }

  const bundle: Bundle = await response.json();
  const resources =
    (bundle.entry?.map((e) => e.resource).filter(Boolean) as Resource[]) ?? [];

  return {
    resources,
    total: bundle.total,
    bundle,
  };
}
