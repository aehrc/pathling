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

import { useMutation, useQueryClient } from "@tanstack/react-query";

import { create } from "../api";
import { config } from "../config";
import { libraryToSummary } from "./sqlQueryHelpers";
import { SQL_QUERY_LIBRARIES_QUERY_KEY } from "./useSqlQueryLibraries";
import { useAuth } from "../contexts/AuthContext";

import type {
  SqlQueryLibrary,
  SqlQueryLibrarySummary,
} from "../types/sqlQuery";
import type { UseMutationResult } from "@tanstack/react-query";
import type { Library } from "fhir/r4";

/**
 * Options for {@link useSaveSqlQueryLibrary}.
 */
export interface UseSaveSqlQueryLibraryOptions {
  /** Callback fired on a successful save. */
  onSuccess?: (summary: SqlQueryLibrarySummary) => void;
  /** Callback fired on error. */
  onError?: (error: Error) => void;
}

/**
 * Result type for {@link useSaveSqlQueryLibrary}.
 */
export type UseSaveSqlQueryLibraryResult = UseMutationResult<
  SqlQueryLibrarySummary,
  Error,
  SqlQueryLibrary
>;

/**
 * Saves an inline SQLQuery Library to the server.
 *
 * On success, the summary of the created resource is added to the cached
 * `SQL_QUERY_LIBRARIES_QUERY_KEY` list before the mutation resolves, so a
 * caller can select the new query straight away, and a background refresh is
 * triggered so the list catches up with the server.
 *
 * @param options - Optional callbacks for success and error.
 * @returns Mutation result resolving with the summary of the saved Library.
 */
export function useSaveSqlQueryLibrary(
  options?: UseSaveSqlQueryLibraryOptions,
): UseSaveSqlQueryLibraryResult {
  const { fhirBaseUrl } = config;
  const { client } = useAuth();
  const accessToken = client?.state.tokenResponse?.access_token;
  const queryClient = useQueryClient();

  return useMutation<SqlQueryLibrarySummary, Error, SqlQueryLibrary>({
    mutationFn: async (library) => {
      const created = (await create(fhirBaseUrl!, {
        resourceType: "Library",
        resource: library as unknown as Library,
        accessToken,
      })) as Library;
      const summary = libraryToSummary(created);
      if (!summary) {
        throw new Error(
          "Server returned a Library without an ID or content; cannot reference it.",
        );
      }
      return summary;
    },
    onSuccess: (summary) => {
      // Replace rather than append when the list already holds this ID, so a
      // refetch that landed first does not leave the query listed twice.
      queryClient.setQueryData<SqlQueryLibrarySummary[]>(
        SQL_QUERY_LIBRARIES_QUERY_KEY,
        (cached = []) =>
          cached.some((lib) => lib.id === summary.id)
            ? cached.map((lib) => (lib.id === summary.id ? summary : lib))
            : [...cached, summary],
      );
      void queryClient.invalidateQueries({
        queryKey: SQL_QUERY_LIBRARIES_QUERY_KEY,
      });
      options?.onSuccess?.(summary);
    },
    onError: options?.onError,
  });
}
