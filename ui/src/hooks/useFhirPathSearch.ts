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

import type { Bundle, Resource } from "fhir/r4";

/**
 * Options for useFhirPathSearch hook.
 * This is a specialised search that uses FHIRPath filter expressions and
 * standard FHIR search parameters.
 */
export interface UseFhirPathSearchOptions {
  /** The FHIR resource type to search. */
  resourceType: string;
  /** FHIRPath filter expressions. */
  filters: string[];
  /** Standard FHIR search parameters as name-value pairs. */
  searchParams?: Record<string, string[]>;
  /** Whether to enable the query. */
  enabled?: boolean;
}

/**
 * Result of useFhirPathSearch hook.
 */
export interface UseFhirPathSearchResult {
  /** The matching resources. */
  resources: Resource[];
  /** Total count from server (may be undefined). */
  total?: number;
  /** The raw bundle response. */
  bundle?: Bundle;
  /** Loading state. */
  isLoading: boolean;
  /** Error state. */
  isError: boolean;
  /** Error object if failed. */
  error: Error | null;
  /** Refetch function. */
  refetch: () => void;
}

/**
 * Search for resources using FHIRPath filter expressions.
 */
export type UseFhirPathSearchFn = (
  options: UseFhirPathSearchOptions,
) => UseFhirPathSearchResult;

/**
 * Search for resources using FHIRPath filter expressions.
 *
 * @param options - Search options including resource type and filters.
 * @returns Result with resources, total, and bundle.
 */
export const useFhirPathSearch: UseFhirPathSearchFn = (options) => {
  const { fhirBaseUrl } = config;
  const { client } = useAuth();
  const accessToken = client?.state.tokenResponse?.access_token;

  const query = useQuery<
    { resources: Resource[]; total?: number; bundle: Bundle },
    Error
  >({
    queryKey: [
      "fhirPathSearch",
      options.resourceType,
      options.filters,
      options.searchParams,
    ],
    queryFn: async () => {
      const params: Record<string, string | string[]> = {
        _count: "10",
      };

      const filters = options.filters
        .map((f) => f.trim())
        .filter((f) => f.length > 0);

      // Only use the fhirPath named query when FHIRPath filters are present.
      if (filters.length > 0) {
        params._query = "fhirPath";
        params.filter = filters;
      }

      // Merge standard search parameters into the request.
      if (options.searchParams) {
        for (const [key, values] of Object.entries(options.searchParams)) {
          if (values.length > 0) {
            params[key] = values;
          }
        }
      }

      const bundle: Bundle = await search(fhirBaseUrl!, {
        resourceType: options.resourceType,
        params,
        accessToken,
      });

      const resources =
        (bundle.entry?.map((e) => e.resource).filter(Boolean) as Resource[]) ??
        [];

      return {
        resources,
        total: bundle.total,
        bundle,
      };
    },
    enabled:
      options.enabled !== false && !!fhirBaseUrl && !!options.resourceType,
  });

  return {
    resources: query.data?.resources ?? [],
    total: query.data?.total,
    bundle: query.data?.bundle,
    isLoading: query.isLoading,
    isError: query.isError,
    error: query.error,
    refetch: query.refetch,
  };
};
