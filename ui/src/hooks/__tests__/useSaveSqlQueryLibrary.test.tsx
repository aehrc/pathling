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

/**
 * Tests for the useSaveSqlQueryLibrary hook.
 *
 * Covers the refresh of the stored lists after a save: the form saves both
 * SQLQuery and SQLView Libraries, so both lists must be refreshed for the new
 * resource to appear.
 *
 * @author John Grimes
 */

import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { act, renderHook } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

vi.mock("../../api", () => ({
  create: vi.fn(),
}));

vi.mock("../../config", () => ({
  config: {
    fhirBaseUrl: "http://localhost:8080/fhir",
  },
}));

vi.mock("../../contexts/AuthContext", () => ({
  useAuth: vi.fn(() => ({
    client: { state: { tokenResponse: { access_token: "test-token" } } },
  })),
}));

import { create } from "../../api";
import { encodeSql } from "../../utils";
import { useSaveSqlQueryLibrary } from "../useSaveSqlQueryLibrary";
import { SQL_QUERY_LIBRARIES_QUERY_KEY } from "../useSqlQueryLibraries";
import { SQL_VIEWS_QUERY_KEY } from "../useSqlViews";

import type { SqlQueryLibrary } from "../../types/sqlQuery";
import type { Resource } from "fhir/r4";
import type { ReactNode } from "react";

describe("useSaveSqlQueryLibrary", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  // A saved Library may be either a SQLQuery or a SQLView, so the hook
  // refreshes both stored lists - otherwise a newly saved SQLView would not
  // appear among the views until the page was reloaded.
  it.each(["sql-query", "sql-view"] as const)(
    "refreshes both stored lists after saving a %s Library",
    async (code) => {
      const submitted: SqlQueryLibrary = {
        resourceType: "Library",
        status: "active",
        title: "Active patients",
        type: {
          coding: [
            {
              system: "https://sql-on-fhir.org/ig/CodeSystem/LibraryTypesCodes",
              code,
            },
          ],
        },
        content: [{ contentType: "application/sql", data: encodeSql("SELECT 1") }],
      };
      vi.mocked(create).mockResolvedValue({
        ...submitted,
        id: "lib-1",
      } as unknown as Resource);
      const queryClient = new QueryClient();
      const invalidate = vi.spyOn(queryClient, "invalidateQueries");
      const wrapper = ({ children }: { children: ReactNode }) => (
        <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>
      );
      const { result } = renderHook(() => useSaveSqlQueryLibrary(), { wrapper });

      let saved: unknown;
      await act(async () => {
        saved = await result.current.mutateAsync(submitted);
      });

      expect(saved).toEqual({ id: "lib-1", title: "Active patients" });
      expect(invalidate).toHaveBeenCalledWith({
        queryKey: SQL_QUERY_LIBRARIES_QUERY_KEY,
      });
      expect(invalidate).toHaveBeenCalledWith({ queryKey: SQL_VIEWS_QUERY_KEY });
    },
  );
});
