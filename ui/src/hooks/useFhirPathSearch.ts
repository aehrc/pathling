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
import type { Bundle, Resource } from "fhir/r4";
import { search } from "../api";
import { config } from "../config";
import { useAuth } from "../contexts/AuthContext";
import type { UseFhirPathSearchFn } from "../types/hooks";

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
    queryKey: ["fhirPathSearch", options.resourceType, options.filters],
    queryFn: async () => {
      const params: Record<string, string | string[]> = {
        _query: "fhirPath",
        _count: "10",
      };

      const filters = options.filters
        .map((f) => f.trim())
        .filter((f) => f.length > 0);
      if (filters.length > 0) {
        params.filter = filters;
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
}
