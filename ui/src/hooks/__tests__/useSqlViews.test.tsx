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
 * Tests for the useSqlViews hook.
 *
 * Verifies that the hook fetches stored SQLView Library resources with the
 * `sql-view` type code and maps the bundle to summaries via the shared
 * mapper, with parameters always empty for a SQLView.
 *
 * @author John Grimes
 */

import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

// Mock the API module so the hook's network call is observable.
vi.mock("../../api", () => ({
  listStoredLibraries: vi.fn(),
}));

// Mock the config module.
vi.mock("../../config", () => ({
  config: {
    fhirBaseUrl: "http://localhost:8080/fhir",
  },
}));

// Mock the AuthContext.
vi.mock("../../contexts/AuthContext", () => ({
  useAuth: vi.fn(() => ({
    client: {
      state: {
        tokenResponse: {
          access_token: "test-token",
        },
      },
    },
  })),
}));

import { listStoredLibraries } from "../../api";
import { config } from "../../config";
import { useSqlViews } from "../useSqlViews";

import type { Bundle } from "fhir/r4";
import type { ReactNode } from "react";

/**
 * Creates a wrapper component that provides TanStack Query context.
 *
 * @returns A wrapper function suitable for renderHook.
 */
function createWrapper() {
  const queryClient = new QueryClient({
    defaultOptions: {
      queries: {
        retry: false,
      },
    },
  });
  return function Wrapper({ children }: { children: ReactNode }) {
    return <QueryClientProvider client={queryClient}>{children}</QueryClientProvider>;
  };
}

// A stored SQLView: Base64-encoded SQL, one dependency, no parameters.
const mockSqlViewBundle: Bundle = {
  resourceType: "Bundle",
  type: "searchset",
  total: 1,
  entry: [
    {
      resource: {
        resourceType: "Library",
        id: "view-active-patients",
        title: "Active patients",
        status: "active",
        type: {
          coding: [
            {
              system: "https://sql-on-fhir.org/ig/CodeSystem/LibraryTypesCodes",
              code: "sql-view",
            },
          ],
        },
        content: [{ contentType: "application/sql", data: "U0VMRUNUIDI=" }],
        relatedArtifact: [
          {
            type: "depends-on",
            label: "patients",
            resource: "ViewDefinition/patient-demographics",
          },
        ],
      },
    },
  ],
} as unknown as Bundle;

describe("useSqlViews", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  // The hook scopes its Library search to the sql-view type code.
  it("fetches stored libraries with the sql-view type code", async () => {
    vi.mocked(listStoredLibraries).mockResolvedValue(mockSqlViewBundle);

    const { result } = renderHook(() => useSqlViews(), {
      wrapper: createWrapper(),
    });

    await waitFor(() => {
      expect(result.current.isSuccess).toBe(true);
    });

    expect(listStoredLibraries).toHaveBeenCalledWith("http://localhost:8080/fhir", {
      typeCode: "sql-view",
      accessToken: "test-token",
    });
  });

  // The bundle is mapped to summaries via the shared mapper, decoding the
  // SQL and surfacing the dependency; parameters are empty for a SQLView.
  it("maps the bundle to summaries with empty parameters", async () => {
    vi.mocked(listStoredLibraries).mockResolvedValue(mockSqlViewBundle);

    const { result } = renderHook(() => useSqlViews(), {
      wrapper: createWrapper(),
    });

    await waitFor(() => {
      expect(result.current.isSuccess).toBe(true);
    });

    expect(result.current.data).toHaveLength(1);
    const summary = result.current.data?.[0];
    expect(summary?.id).toBe("view-active-patients");
    expect(summary?.title).toBe("Active patients");
    expect(summary?.sql).toBe("SELECT 2");
    expect(summary?.relatedArtifacts).toEqual([
      { label: "patients", reference: "ViewDefinition/patient-demographics" },
    ]);
    expect(summary?.parameters).toEqual([]);
  });

  // The query is disabled when no FHIR base URL is configured.
  it("does not fetch when fhirBaseUrl is not configured", async () => {
    vi.mocked(config).fhirBaseUrl = undefined as unknown as string;
    vi.mocked(listStoredLibraries).mockResolvedValue(mockSqlViewBundle);

    const { result } = renderHook(() => useSqlViews(), {
      wrapper: createWrapper(),
    });

    await new Promise((resolve) => setTimeout(resolve, 50));

    expect(result.current.fetchStatus).toBe("idle");
    expect(listStoredLibraries).not.toHaveBeenCalled();

    // Restore config for other tests.
    vi.mocked(config).fhirBaseUrl = "http://localhost:8080/fhir";
  });

  // The enabled option gates the query, matching useSqlQueryLibraries.
  it("does not fetch when enabled is false", async () => {
    vi.mocked(listStoredLibraries).mockResolvedValue(mockSqlViewBundle);

    const { result } = renderHook(() => useSqlViews({ enabled: false }), {
      wrapper: createWrapper(),
    });

    await new Promise((resolve) => setTimeout(resolve, 50));

    expect(result.current.fetchStatus).toBe("idle");
    expect(listStoredLibraries).not.toHaveBeenCalled();
  });
});
