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
 * Tests for the useDocumentTitle hook.
 *
 * This test suite verifies that the useDocumentTitle hook correctly sets the
 * document title based on the server name from the CapabilityStatement, falling
 * back to the hostname when server name is not available.
 *
 * @author John Grimes
 */

import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

// Mock the config module.
vi.mock("../../config", () => ({
  config: {
    fhirBaseUrl: "http://localhost:8080/fhir",
  },
}));

// Mock the useServerCapabilities hook.
vi.mock("../useServerCapabilities", () => ({
  useServerCapabilities: vi.fn(),
}));

import { useDocumentTitle } from "../useDocumentTitle";
import { useServerCapabilities } from "../useServerCapabilities";

import type { ServerCapabilities } from "../useServerCapabilities";
import type { UseQueryResult } from "@tanstack/react-query";
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

describe("useDocumentTitle", () => {
  const originalTitle = document.title;
  const originalHostname = window.location.hostname;

  beforeEach(() => {
    vi.clearAllMocks();
    document.title = "Original Title";
  });

  afterEach(() => {
    vi.clearAllMocks();
    document.title = originalTitle;
  });

  describe("with server name", () => {
    it("sets document title to server name", async () => {
      const mockCapabilities: ServerCapabilities = {
        authRequired: false,
        serverName: "Pathling FHIR Server",
        serverVersion: "1.0.0",
        fhirVersion: "4.0.1",
        resourceTypes: ["Patient", "Observation"],
      };

      vi.mocked(useServerCapabilities).mockReturnValue({
        data: mockCapabilities,
        isSuccess: true,
        isLoading: false,
        isError: false,
        error: null,
      } as UseQueryResult<ServerCapabilities, Error>);

      renderHook(() => useDocumentTitle(), {
        wrapper: createWrapper(),
      });

      await waitFor(() => {
        expect(document.title).toBe("Pathling FHIR Server");
      });
    });

    it("updates title when server name changes", async () => {
      const initialCapabilities: ServerCapabilities = {
        authRequired: false,
        serverName: "Initial Server",
        resourceTypes: [],
      };

      const mockReturnValue = {
        data: initialCapabilities,
        isSuccess: true,
        isLoading: false,
        isError: false,
        error: null,
      } as UseQueryResult<ServerCapabilities, Error>;

      vi.mocked(useServerCapabilities).mockReturnValue(mockReturnValue);

      const { rerender } = renderHook(() => useDocumentTitle(), {
        wrapper: createWrapper(),
      });

      await waitFor(() => {
        expect(document.title).toBe("Initial Server");
      });

      // Update the mock to return a different server name.
      const updatedCapabilities: ServerCapabilities = {
        authRequired: false,
        serverName: "Updated Server",
        resourceTypes: [],
      };

      vi.mocked(useServerCapabilities).mockReturnValue({
        data: updatedCapabilities,
        isSuccess: true,
        isLoading: false,
        isError: false,
        error: null,
      } as UseQueryResult<ServerCapabilities, Error>);

      rerender();

      await waitFor(() => {
        expect(document.title).toBe("Updated Server");
      });
    });
  });

  describe("without server name", () => {
    it("sets document title to hostname when server name is undefined", async () => {
      const mockCapabilities: ServerCapabilities = {
        authRequired: false,
        serverName: undefined,
        resourceTypes: [],
      };

      vi.mocked(useServerCapabilities).mockReturnValue({
        data: mockCapabilities,
        isSuccess: true,
        isLoading: false,
        isError: false,
        error: null,
      } as UseQueryResult<ServerCapabilities, Error>);

      renderHook(() => useDocumentTitle(), {
        wrapper: createWrapper(),
      });

      await waitFor(() => {
        expect(document.title).toBe(originalHostname);
      });
    });

    it("sets document title to hostname when capabilities are undefined", async () => {
      vi.mocked(useServerCapabilities).mockReturnValue({
        data: undefined,
        isSuccess: false,
        isLoading: true,
        isError: false,
        error: null,
      } as UseQueryResult<ServerCapabilities, Error>);

      renderHook(() => useDocumentTitle(), {
        wrapper: createWrapper(),
      });

      await waitFor(() => {
        expect(document.title).toBe(originalHostname);
      });
    });

    it("sets document title to hostname when server name is empty string", async () => {
      const mockCapabilities: ServerCapabilities = {
        authRequired: false,
        serverName: "",
        resourceTypes: [],
      };

      vi.mocked(useServerCapabilities).mockReturnValue({
        data: mockCapabilities,
        isSuccess: true,
        isLoading: false,
        isError: false,
        error: null,
      } as UseQueryResult<ServerCapabilities, Error>);

      renderHook(() => useDocumentTitle(), {
        wrapper: createWrapper(),
      });

      await waitFor(() => {
        expect(document.title).toBe(originalHostname);
      });
    });
  });

  describe("error handling", () => {
    it("falls back to hostname when capabilities query fails", async () => {
      vi.mocked(useServerCapabilities).mockReturnValue({
        data: undefined,
        isSuccess: false,
        isLoading: false,
        isError: true,
        error: new Error("Network error"),
      } as UseQueryResult<ServerCapabilities, Error>);

      renderHook(() => useDocumentTitle(), {
        wrapper: createWrapper(),
      });

      await waitFor(() => {
        expect(document.title).toBe(originalHostname);
      });
    });
  });

  describe("hook dependencies", () => {
    it("calls useServerCapabilities with fhirBaseUrl from config", () => {
      vi.mocked(useServerCapabilities).mockReturnValue({
        data: undefined,
        isSuccess: false,
        isLoading: true,
        isError: false,
        error: null,
      } as UseQueryResult<ServerCapabilities, Error>);

      renderHook(() => useDocumentTitle(), {
        wrapper: createWrapper(),
      });

      expect(useServerCapabilities).toHaveBeenCalledWith("http://localhost:8080/fhir");
    });
  });
});
