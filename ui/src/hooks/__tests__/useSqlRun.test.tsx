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
 * Tests for the useSqlRun hook: which wire form each kind of subject takes,
 * how the response is parsed, and how a failure surfaces.
 *
 * @author John Grimes
 */

import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { renderHook, waitFor } from "@testing-library/react";
import React from "react";
import { beforeEach, describe, expect, it, vi } from "vitest";

const mockSqlRun = vi.fn();
const mockSqlRunStored = vi.fn();

vi.mock("../../api", () => ({
  sqlRun: (...args: unknown[]) => mockSqlRun(...args),
  sqlRunStored: (...args: unknown[]) => mockSqlRunStored(...args),
}));

vi.mock("../../config", () => ({
  config: { fhirBaseUrl: "http://localhost:8080/fhir" },
}));

vi.mock("../../contexts/AuthContext", () => ({
  useAuth: vi.fn(() => ({
    client: { state: { tokenResponse: { access_token: "test-token" } } },
  })),
}));

import { useSqlRun } from "../useSqlRun";

/**
 * Wraps the hook in the query client it needs.
 *
 * @param root0 - The wrapper props.
 * @param root0.children - The hook under test.
 * @returns The wrapped children.
 */
function wrapper({ children }: Readonly<{ children: React.ReactNode }>) {
  const queryClient = new QueryClient({
    defaultOptions: { mutations: { retry: false } },
  });
  return React.createElement(QueryClientProvider, { client: queryClient }, children);
}

describe("useSqlRun", () => {
  beforeEach(() => {
    vi.clearAllMocks();
    mockSqlRunStored.mockResolvedValue(new Response('{"id":"p1"}\n', { status: 200 }));
    mockSqlRun.mockResolvedValue(new Response('{"id":"p1"}\n', { status: 200 }));
  });

  // A stored subject with no bindings is a GET, which is the form a user can
  // paste into a browser and the one the specification prefers.
  it("runs a stored subject with no bindings as a GET", async () => {
    const { result } = renderHook(() => useSqlRun(), { wrapper });

    result.current.execute({
      subject: { kind: "reference", reference: "ViewDefinition/v" },
      limit: 10,
    });

    await waitFor(() => expect(result.current.status).toBe("success"));
    expect(mockSqlRunStored).toHaveBeenCalledWith("http://localhost:8080/fhir", {
      reference: "ViewDefinition/v",
      format: undefined,
      limit: 10,
      header: undefined,
      patientIds: undefined,
      groupIds: undefined,
      since: undefined,
      accessToken: "test-token",
    });
    expect(mockSqlRun).not.toHaveBeenCalled();
  });

  // Bindings cannot be expressed in a query string, so they force a POST.
  it("runs a stored subject carrying bindings as a POST", async () => {
    const { result } = renderHook(() => useSqlRun(), { wrapper });

    result.current.execute({
      subject: { kind: "reference", reference: "Library/by-family" },
      bindings: { family: "Smith" },
      parameterTypes: { family: "string" },
    });

    await waitFor(() => expect(result.current.status).toBe("success"));
    expect(mockSqlRunStored).not.toHaveBeenCalled();
    expect(mockSqlRun).toHaveBeenCalledWith(
      "http://localhost:8080/fhir",
      expect.objectContaining({
        subject: { kind: "reference", reference: "Library/by-family" },
        parameters: {
          resourceType: "Parameters",
          parameter: [{ name: "family", valueString: "Smith" }],
        },
      }),
    );
  });

  // An inline subject cannot be expressed in a query string either.
  it("runs an inline subject as a POST", async () => {
    const { result } = renderHook(() => useSqlRun(), { wrapper });
    const view = { resourceType: "ViewDefinition", resource: "Patient" };

    result.current.execute({ subject: { kind: "resource", resource: view } });

    await waitFor(() => expect(result.current.status).toBe("success"));
    expect(mockSqlRun).toHaveBeenCalledWith(
      "http://localhost:8080/fhir",
      expect.objectContaining({ subject: { kind: "resource", resource: view } }),
    );
  });

  it("parses the response in the requested format", async () => {
    mockSqlRunStored.mockResolvedValue(new Response("id,name\np1,Alice\n", { status: 200 }));
    const { result } = renderHook(() => useSqlRun(), { wrapper });

    result.current.execute({
      subject: { kind: "reference", reference: "ViewDefinition/v" },
      format: "csv",
    });

    await waitFor(() => expect(result.current.status).toBe("success"));
    expect(result.current.result).toMatchObject({
      kind: "tabular",
      format: "csv",
      columns: ["id", "name"],
      rows: [{ id: "p1", name: "Alice" }],
    });
  });

  it("surfaces a failed run as an error", async () => {
    mockSqlRunStored.mockRejectedValue(new Error("No ViewDefinition matches"));
    const { result } = renderHook(() => useSqlRun(), { wrapper });

    result.current.execute({
      subject: { kind: "reference", reference: "ViewDefinition/nope" },
    });

    await waitFor(() => expect(result.current.status).toBe("error"));
    expect(result.current.error?.message).toBe("No ViewDefinition matches");
  });

  it("records the request that produced the current state", async () => {
    const { result } = renderHook(() => useSqlRun(), { wrapper });
    const request = {
      subject: { kind: "reference", reference: "ViewDefinition/v" } as const,
    };

    result.current.execute(request);

    await waitFor(() => expect(result.current.status).toBe("success"));
    expect(result.current.lastRequest).toEqual(request);
  });
});
