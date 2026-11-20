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
 * Tests for the useSaveSqlQueryLibrary hook, focused on the cached list of
 * stored SQL queries that the query picker reads.
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
import { useSaveSqlQueryLibrary } from "../useSaveSqlQueryLibrary";
import { SQL_QUERY_LIBRARIES_QUERY_KEY } from "../useSqlQueryLibraries";

import type { SqlQueryLibrary, SqlQueryLibrarySummary } from "../../types/sqlQuery";
import type { Resource } from "fhir/r4";
import type { ReactNode } from "react";

/**
 * Creates a TanStack Query wrapper and exposes its client for cache
 * inspection.
 *
 * @returns The wrapper component and the query client.
 */
function createWrapperWithClient() {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });
  const wrapper = function Wrapper({ children }: { children: ReactNode }) {
    return <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>;
  };
  return { wrapper, queryClient };
}

// The inline Library the form submits.
const inlineLibrary: SqlQueryLibrary = {
  resourceType: "Library",
  status: "active",
  title: "New query",
  type: {
    coding: [
      {
        system: "https://sql-on-fhir.org/ig/CodeSystem/LibraryTypesCodes",
        code: "sql-query",
      },
    ],
  },
  content: [{ contentType: "application/sql", data: "U0VMRUNUIDE=" }],
  relatedArtifact: [
    {
      type: "depends-on",
      label: "patients",
      resource: "https://pathling.example/ViewDefinition/PatientDemographics",
    },
  ],
  parameter: [{ name: "patient_id", use: "in", type: "string" }],
};

// The same Library as returned by the server, with an assigned ID.
const createdLibrary = { ...inlineLibrary, id: "new-query" };

/**
 * Builds a cached summary for a Library other than the one being saved.
 *
 * @param id - The ID of the existing Library.
 * @param title - The title of the existing Library.
 * @returns A summary as stored in the list cache.
 */
function existingSummary(id: string, title: string): SqlQueryLibrarySummary {
  return {
    id,
    title,
    sql: "SELECT 2",
    relatedArtifacts: [],
    parameters: [],
    resource: { resourceType: "Library", id, title } as SqlQueryLibrary,
  };
}

describe("useSaveSqlQueryLibrary", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  it("adds the saved query to the cached list before the save resolves", async () => {
    // The picker reads the cached list as soon as the save resolves, so the new
    // query must already be in it, with the SQL and parameters the picker
    // previews, rather than waiting on a background refetch.
    vi.mocked(create).mockResolvedValue(createdLibrary as unknown as Resource);
    const { wrapper, queryClient } = createWrapperWithClient();
    const existing = existingSummary("existing", "Existing query");
    queryClient.setQueryData(SQL_QUERY_LIBRARIES_QUERY_KEY, [existing]);
    const { result } = renderHook(() => useSaveSqlQueryLibrary(), { wrapper });

    let cachedOnResolve: SqlQueryLibrarySummary[] | undefined;
    await act(async () => {
      await result.current.mutateAsync(inlineLibrary);
      cachedOnResolve = queryClient.getQueryData(SQL_QUERY_LIBRARIES_QUERY_KEY);
    });

    expect(cachedOnResolve).toEqual([
      existing,
      {
        id: "new-query",
        title: "New query",
        sql: "SELECT 1",
        relatedArtifacts: [
          {
            label: "patients",
            reference: "https://pathling.example/ViewDefinition/PatientDemographics",
          },
        ],
        parameters: [{ name: "patient_id", type: "string" }],
        resource: createdLibrary,
      },
    ]);
  });

  it("resolves with the ID and title of the saved query", async () => {
    // The form selects the saved query by the ID it resolves with.
    vi.mocked(create).mockResolvedValue(createdLibrary as unknown as Resource);
    const { wrapper } = createWrapperWithClient();
    const { result } = renderHook(() => useSaveSqlQueryLibrary(), { wrapper });

    let saved: { id: string; title: string } | undefined;
    await act(async () => {
      saved = await result.current.mutateAsync(inlineLibrary);
    });

    expect(saved).toMatchObject({ id: "new-query", title: "New query" });
  });

  it("seeds the cached list when it has not been fetched", async () => {
    // A save can complete before the list is first loaded; the new query
    // should still be selectable.
    vi.mocked(create).mockResolvedValue(createdLibrary as unknown as Resource);
    const { wrapper, queryClient } = createWrapperWithClient();
    const { result } = renderHook(() => useSaveSqlQueryLibrary(), { wrapper });

    await act(async () => {
      await result.current.mutateAsync(inlineLibrary);
    });

    const cached = queryClient.getQueryData<SqlQueryLibrarySummary[]>(
      SQL_QUERY_LIBRARIES_QUERY_KEY,
    );
    expect(cached?.map((summary) => summary.id)).toEqual(["new-query"]);
  });

  it("replaces a cached entry with the same ID rather than duplicating it", async () => {
    // If a refetch has already brought the new Library into the list, adding
    // it again would show the query twice in the picker.
    vi.mocked(create).mockResolvedValue(createdLibrary as unknown as Resource);
    const { wrapper, queryClient } = createWrapperWithClient();
    const first = existingSummary("first", "First query");
    const stale = existingSummary("new-query", "Stale title");
    const last = existingSummary("last", "Last query");
    queryClient.setQueryData(SQL_QUERY_LIBRARIES_QUERY_KEY, [first, stale, last]);
    const { result } = renderHook(() => useSaveSqlQueryLibrary(), { wrapper });

    await act(async () => {
      await result.current.mutateAsync(inlineLibrary);
    });

    const cached = queryClient.getQueryData<SqlQueryLibrarySummary[]>(
      SQL_QUERY_LIBRARIES_QUERY_KEY,
    );
    expect(cached?.map((summary) => [summary.id, summary.title])).toEqual([
      ["first", "First query"],
      ["new-query", "New query"],
      ["last", "Last query"],
    ]);
  });

  it("marks the cached list stale so it is refreshed from the server", async () => {
    // The optimistic entry is a stopgap; the server's list remains the source
    // of truth.
    vi.mocked(create).mockResolvedValue(createdLibrary as unknown as Resource);
    const { wrapper, queryClient } = createWrapperWithClient();
    queryClient.setQueryData(SQL_QUERY_LIBRARIES_QUERY_KEY, []);
    const { result } = renderHook(() => useSaveSqlQueryLibrary(), { wrapper });

    await act(async () => {
      await result.current.mutateAsync(inlineLibrary);
    });

    expect(queryClient.getQueryState(SQL_QUERY_LIBRARIES_QUERY_KEY)?.isInvalidated).toBe(true);
  });

  it("leaves the cached list untouched when the server returns no SQL content", async () => {
    // A Library the picker cannot render must not be added to it.
    vi.mocked(create).mockResolvedValue({
      ...createdLibrary,
      content: [],
    } as unknown as Resource);
    const { wrapper, queryClient } = createWrapperWithClient();
    const existing = existingSummary("existing", "Existing query");
    queryClient.setQueryData(SQL_QUERY_LIBRARIES_QUERY_KEY, [existing]);
    const { result } = renderHook(() => useSaveSqlQueryLibrary(), { wrapper });

    await act(async () => {
      await expect(result.current.mutateAsync(inlineLibrary)).rejects.toThrow(
        /without an ID or content/,
      );
    });

    expect(queryClient.getQueryData(SQL_QUERY_LIBRARIES_QUERY_KEY)).toEqual([existing]);
  });

  it("leaves the cached list untouched when the create request fails", async () => {
    vi.mocked(create).mockRejectedValue(new Error("Server unavailable"));
    const { wrapper, queryClient } = createWrapperWithClient();
    const existing = existingSummary("existing", "Existing query");
    queryClient.setQueryData(SQL_QUERY_LIBRARIES_QUERY_KEY, [existing]);
    const { result } = renderHook(() => useSaveSqlQueryLibrary(), { wrapper });

    await act(async () => {
      await expect(result.current.mutateAsync(inlineLibrary)).rejects.toThrow("Server unavailable");
    });

    expect(queryClient.getQueryData(SQL_QUERY_LIBRARIES_QUERY_KEY)).toEqual([existing]);
  });
});
