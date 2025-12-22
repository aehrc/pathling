/**
 * Hook for saving a ViewDefinition to the server.
 *
 * @author John Grimes
 */

import { useMutation, useQueryClient } from "@tanstack/react-query";
import { config } from "../config";
import { useAuth } from "../contexts/AuthContext";
import { createViewDefinition } from "../services/sqlOnFhir";
import type { ViewDefinitionSummary } from "../types/sqlOnFhir";

/**
 * Mutation hook for creating a ViewDefinition on the server.
 * Updates the query cache on success.
 */
export function useSaveViewDefinition() {
  const { client } = useAuth();
  const { fhirBaseUrl } = config;
  const queryClient = useQueryClient();

  return useMutation({
    mutationFn: async (json: string) => {
      const accessToken = client?.state.tokenResponse?.access_token;
      return createViewDefinition(fhirBaseUrl, accessToken, json);
    },
    onSuccess: (result, json) => {
      // Update cache with the new ViewDefinition so it's immediately available
      // when the form switches to the stored tab.
      const formattedJson = JSON.stringify(JSON.parse(json), null, 2);
      queryClient.setQueryData<ViewDefinitionSummary[]>(
        ["viewDefinitions"],
        (old) => [
          ...(old ?? []),
          { id: result.id, name: result.name, json: formattedJson },
        ],
      );
      // Invalidate to trigger background refresh for consistency.
      void queryClient.invalidateQueries({ queryKey: ["viewDefinitions"] });
    },
  });
}
