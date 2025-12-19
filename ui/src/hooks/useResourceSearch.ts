/**
 * Hook for executing FHIR resource searches with FHIRPath filters.
 *
 * @author John Grimes
 */

import { useQuery } from "@tanstack/react-query";
import { config } from "../config";
import { useAuth } from "../contexts/AuthContext";
import { searchResources, type SearchResult } from "../services/search";
import type { SearchRequest } from "../types/search";

/**
 * Executes a FHIR search operation with the provided request parameters.
 * The query is only enabled when a valid request is provided.
 */
export function useResourceSearch(request: SearchRequest | null) {
  const { client } = useAuth();
  const { fhirBaseUrl } = config;
  const accessToken = client?.state.tokenResponse?.access_token;

  return useQuery<SearchResult, Error>({
    queryKey: ["resourceSearch", request?.resourceType, request?.filters],
    queryFn: () => searchResources(fhirBaseUrl, accessToken, request!),
    enabled: !!request && !!request.resourceType,
    staleTime: 30 * 1000,
    retry: 1,
  });
}
