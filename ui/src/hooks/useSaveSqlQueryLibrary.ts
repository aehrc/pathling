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
import { SQL_VIEWS_QUERY_KEY } from "./useSqlViews";
import { useAuth } from "../contexts/AuthContext";

import type {
  SaveSqlQueryLibraryResult,
  SqlQueryLibrary,
} from "../types/sqlQuery";
import type { UseMutationResult } from "@tanstack/react-query";
import type { Library } from "fhir/r4";

/**
 * Options for {@link useSaveSqlQueryLibrary}.
 */
export interface UseSaveSqlQueryLibraryOptions {
  /** Callback fired on a successful save. */
  onSuccess?: (result: SaveSqlQueryLibraryResult) => void;
  /** Callback fired on error. */
  onError?: (error: Error) => void;
}

/**
 * Result type for {@link useSaveSqlQueryLibrary}.
 */
export type UseSaveSqlQueryLibraryResult = UseMutationResult<
  SaveSqlQueryLibraryResult,
  Error,
  SqlQueryLibrary
>;

/**
 * Saves an inline SQLQuery or SQLView Library to the server.
 *
 * On success, both the `SQL_QUERY_LIBRARIES_QUERY_KEY` and
 * `SQL_VIEWS_QUERY_KEY` lists are invalidated, since the form saves a
 * SQLView when no parameters are declared and a SQLQuery otherwise.
 *
 * @param options - Optional callbacks for success and error.
 * @returns Mutation result for saving SQLQuery and SQLView Libraries.
 */
export function useSaveSqlQueryLibrary(
  options?: UseSaveSqlQueryLibraryOptions,
): UseSaveSqlQueryLibraryResult {
  const { fhirBaseUrl } = config;
  const { client } = useAuth();
  const accessToken = client?.state.tokenResponse?.access_token;
  const queryClient = useQueryClient();

  return useMutation<SaveSqlQueryLibraryResult, Error, SqlQueryLibrary>({
    mutationFn: async (library) => {
      const created = (await create(fhirBaseUrl!, {
        resourceType: "Library",
        resource: library as unknown as Library,
        accessToken,
      })) as Library;
      const summary = libraryToSummary(created);
      const id = created.id;
      if (!id || !summary) {
        throw new Error(
          "Server returned a Library without an ID or content; cannot reference it.",
        );
      }
      return { id, title: summary.title };
    },
    onSuccess: (result) => {
      void queryClient.invalidateQueries({
        queryKey: SQL_QUERY_LIBRARIES_QUERY_KEY,
      });
      void queryClient.invalidateQueries({ queryKey: SQL_VIEWS_QUERY_KEY });
      options?.onSuccess?.(result);
    },
    onError: options?.onError,
  });
}
