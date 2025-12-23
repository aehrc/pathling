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

import { useQuery } from "@tanstack/react-query";
import { viewRun, viewRunStored } from "../api";
import { config } from "../config";
import { useAuth } from "../contexts/AuthContext";
import type { UseViewRunFn } from "../types/hooks";

/**
 * Run a ViewDefinition and return results as a stream.
 *
 * @param options - View run options including definition or ID.
 * @returns Query result with the ReadableStream response.
 */
export const useViewRun: UseViewRunFn = (options) => {
  const { fhirBaseUrl } = config;
  const { client } = useAuth();
  const accessToken = client?.state.tokenResponse?.access_token;

  const hasInlineView = !!options.viewDefinition;
  const hasStoredView = !!options.viewDefinitionId;

  return useQuery<ReadableStream, Error>({
    queryKey: [
      "viewRun",
      options.viewDefinitionId,
      options.viewDefinition?.name,
      options.format,
      options.limit,
    ],
    queryFn: async () => {
      if (hasStoredView) {
        return viewRunStored(fhirBaseUrl!, {
          viewDefinitionId: options.viewDefinitionId!,
          format: options.format,
          limit: options.limit,
          header: options.header,
          accessToken,
        });
      }
      if (hasInlineView) {
        return viewRun(fhirBaseUrl!, {
          viewDefinition: options.viewDefinition!,
          format: options.format,
          limit: options.limit,
          header: options.header,
          accessToken,
        });
      }
      throw new Error("Either viewDefinition or viewDefinitionId is required");
    },
    enabled:
      options.enabled !== false &&
      !!fhirBaseUrl &&
      (hasInlineView || hasStoredView),
  });
}
