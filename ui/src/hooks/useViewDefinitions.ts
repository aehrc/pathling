/**
 * Hook for fetching ViewDefinitions from the server.
 *
 * @author John Grimes
 */

import { useQuery } from "@tanstack/react-query";
import { config } from "../config";
import { useAuth } from "../contexts/AuthContext";
import { searchViewDefinitions } from "../services/sqlOnFhir";
import type { ViewDefinitionSummary } from "../types/sqlOnFhir";

/**
 * Fetches available ViewDefinitions from the server.
 * Results are cached for 1 minute.
 */
export function useViewDefinitions() {
  const { client } = useAuth();
  const { fhirBaseUrl } = config;
  const accessToken = client?.state.tokenResponse?.access_token;

  return useQuery<ViewDefinitionSummary[], Error>({
    queryKey: ["viewDefinitions"],
    queryFn: () => searchViewDefinitions(fhirBaseUrl, accessToken),
    staleTime: 60 * 1000,
    retry: 1,
  });
}
