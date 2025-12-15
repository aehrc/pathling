/**
 * Hook for fetching server capabilities using TanStack Query.
 *
 * @author John Grimes
 */

import { useQuery } from "@tanstack/react-query";
import {
  checkServerCapabilities,
  type ServerCapabilities,
} from "../services/auth";

/**
 * Fetches the server's CapabilityStatement and extracts capabilities.
 * Results are cached for 5 minutes since capabilities rarely change.
 */
export function useServerCapabilities(fhirBaseUrl: string | null | undefined) {
  return useQuery<ServerCapabilities, Error>({
    queryKey: ["serverCapabilities", fhirBaseUrl],
    queryFn: () => checkServerCapabilities(fhirBaseUrl!),
    enabled: !!fhirBaseUrl,
    staleTime: 5 * 60 * 1000,
  });
}
