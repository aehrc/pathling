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

import { listStoredLibraries } from "../api";
import { config } from "../config";
import { mapLibraryBundle } from "./sqlQueryHelpers";
import { useAuth } from "../contexts/AuthContext";

import type { SqlQueryLibrarySummary } from "../types/sqlQuery";
import type { UseQueryResult } from "@tanstack/react-query";

/**
 * TanStack Query key for the stored SQLView list. Kept distinct from the
 * SQLQuery list so each caches and invalidates independently.
 */
export const SQL_VIEWS_QUERY_KEY = ["sqlViews"] as const;

/**
 * Options for {@link useSqlViews}.
 */
export interface UseSqlViewsOptions {
  /** Whether to enable the query. Defaults to `true`. */
  enabled?: boolean;
}

/**
 * Hook result type for {@link useSqlViews}.
 */
export type UseSqlViewsResult = UseQueryResult<SqlQueryLibrarySummary[], Error>;

/**
 * Fetches stored SQLView Library resources from the FHIR server.
 *
 * Shares the decoded-summary mapping with {@link useSqlQueryLibraries}; a
 * SQLView simply carries no declared parameters.
 *
 * @param options - Optional query options.
 * @returns Query result with summaries of stored SQLView Libraries.
 */
export function useSqlViews(options?: UseSqlViewsOptions): UseSqlViewsResult {
  const { fhirBaseUrl } = config;
  const { client } = useAuth();
  const accessToken = client?.state.tokenResponse?.access_token;

  return useQuery<SqlQueryLibrarySummary[], Error>({
    queryKey: SQL_VIEWS_QUERY_KEY,
    queryFn: async () => {
      const bundle = await listStoredLibraries(fhirBaseUrl!, {
        typeCode: "sql-view",
        accessToken,
      });
      return mapLibraryBundle(bundle);
    },
    enabled: options?.enabled !== false && !!fhirBaseUrl,
  });
}
