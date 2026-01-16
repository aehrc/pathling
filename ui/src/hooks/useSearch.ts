/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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
 */

import { useQuery } from "@tanstack/react-query";

import { search } from "../api";
import { config } from "../config";
import { useAuth } from "../contexts/AuthContext";

import type { UseQueryResult } from "@tanstack/react-query";
import type { Bundle } from "fhir/r4";

/**
 * Options for useSearch hook.
 */
export interface UseSearchOptions {
  /** The FHIR resource type to search. */
  resourceType: string;
  /** Optional search parameters. */
  params?: Record<string, string | string[]>;
  /** Whether to enable the query. */
  enabled?: boolean;
}

/**
 * Result of useSearch hook.
 */
export type UseSearchResult = UseQueryResult<Bundle, Error>;

/**
 * Search for FHIR resources.
 */
export type UseSearchFn = (options: UseSearchOptions) => UseSearchResult;

/**
 * Search for FHIR resources.
 *
 * @param options - Search options including resource type and parameters.
 * @returns Query result with the Bundle response.
 */
export const useSearch: UseSearchFn = (options) => {
  const { fhirBaseUrl } = config;
  const { client } = useAuth();
  const accessToken = client?.state.tokenResponse?.access_token;

  return useQuery<Bundle, Error>({
    queryKey: ["search", options.resourceType, options.params],
    queryFn: () =>
      search(fhirBaseUrl!, {
        resourceType: options.resourceType,
        params: options.params,
        accessToken,
      }),
    enabled: options.enabled !== false && !!fhirBaseUrl,
  });
};
