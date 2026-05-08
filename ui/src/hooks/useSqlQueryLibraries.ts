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

import { listSqlQueryLibraries } from "../api";
import { config } from "../config";
import { mapLibraryBundle } from "./sqlQueryHelpers";
import { useAuth } from "../contexts/AuthContext";

import type { SqlQueryLibrarySummary } from "../types/sqlQuery";
import type { UseQueryResult } from "@tanstack/react-query";

/**
 * TanStack Query key shared by readers and mutators of the SQL query
 * Library list.
 */
export const SQL_QUERY_LIBRARIES_QUERY_KEY = ["sqlQueryLibraries"] as const;

/**
 * Options for {@link useSqlQueryLibraries}.
 */
export interface UseSqlQueryLibrariesOptions {
  /** Whether to enable the query. Defaults to `true`. */
  enabled?: boolean;
}

/**
 * Hook result type for {@link useSqlQueryLibraries}.
 */
export type UseSqlQueryLibrariesResult = UseQueryResult<
  SqlQueryLibrarySummary[],
  Error
>;

/**
 * Fetches stored SQLQuery Library resources from the FHIR server.
 *
 * @param options - Optional query options.
 * @returns Query result with summaries of stored SQLQuery Libraries.
 */
export function useSqlQueryLibraries(
  options?: UseSqlQueryLibrariesOptions,
): UseSqlQueryLibrariesResult {
  const { fhirBaseUrl } = config;
  const { client } = useAuth();
  const accessToken = client?.state.tokenResponse?.access_token;

  return useQuery<SqlQueryLibrarySummary[], Error>({
    queryKey: SQL_QUERY_LIBRARIES_QUERY_KEY,
    queryFn: async () => {
      const bundle = await listSqlQueryLibraries(fhirBaseUrl!, {
        accessToken,
      });
      return mapLibraryBundle(bundle);
    },
    enabled: options?.enabled !== false && !!fhirBaseUrl,
  });
}
