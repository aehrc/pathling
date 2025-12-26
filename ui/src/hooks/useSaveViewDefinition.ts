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

import { useMutation, useQueryClient } from "@tanstack/react-query";
import { create } from "../api";
import { config } from "../config";
import { useAuth } from "../contexts/AuthContext";
import type {
  UseSaveViewDefinitionFn,
  ViewDefinitionSummary,
} from "../types/hooks";

/**
 * Save a ViewDefinition to the server.
 *
 * @param options - Optional callbacks for success and error.
 * @returns Mutation result for saving ViewDefinitions.
 */
export const useSaveViewDefinition: UseSaveViewDefinitionFn = (options) => {
  const { fhirBaseUrl } = config;
  const { client } = useAuth();
  const accessToken = client?.state.tokenResponse?.access_token;
  const queryClient = useQueryClient();

  return useMutation<{ id: string; name: string }, Error, string>({
    mutationFn: async (json: string) => {
      const parsed = JSON.parse(json) as {
        resourceType?: string;
        name?: string;
      };
      if (parsed.resourceType !== "ViewDefinition") {
        throw new Error(
          "Invalid resource: resourceType must be 'ViewDefinition'",
        );
      }
      // Create the resource with explicit resourceType for type safety.
      const result = await create(fhirBaseUrl!, {
        resourceType: "ViewDefinition",
        resource: { ...parsed, resourceType: "ViewDefinition" },
        accessToken,
      });
      const createdResource = result as { id: string; name?: string };
      return {
        id: createdResource.id,
        name: createdResource.name || createdResource.id,
      };
    },
    onSuccess: (result, json) => {
      // Update cache with the new ViewDefinition.
      const formattedJson = JSON.stringify(JSON.parse(json), null, 2);
      queryClient.setQueryData<ViewDefinitionSummary[]>(
        ["viewDefinitions"],
        (old) => [
          ...(old ?? []),
          { id: result.id, name: result.name, json: formattedJson },
        ],
      );
      // Invalidate to trigger background refresh.
      void queryClient.invalidateQueries({ queryKey: ["viewDefinitions"] });
      options?.onSuccess?.(result);
    },
    onError: options?.onError,
  });
};
