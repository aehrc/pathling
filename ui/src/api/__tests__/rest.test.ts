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

import { describe, it, expect, vi, beforeEach, afterEach } from "vitest";

import { UnauthorizedError, NotFoundError } from "../../types/errors";
import { search, read, create, update, deleteResource } from "../rest";

import type { Bundle, Patient } from "fhir/r4";

const mockFetch = vi.fn();

beforeEach(() => {
  vi.stubGlobal("fetch", mockFetch);
});

afterEach(() => {
  vi.unstubAllGlobals();
  vi.resetAllMocks();
});

// Sample FHIR resources for testing.
const samplePatient: Patient = {
  resourceType: "Patient",
  id: "123",
  name: [{ family: "Smith", given: ["John"] }],
};

const sampleBundle: Bundle = {
  resourceType: "Bundle",
  type: "searchset",
  total: 1,
  entry: [{ resource: samplePatient }],
};

describe("search", () => {
  it("makes GET request to correct URL with resource type", async () => {
    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify(sampleBundle), { status: 200 }),
    );

    await search("https://example.com/fhir", {
      resourceType: "Patient",
    });

    expect(mockFetch).toHaveBeenCalledWith(
      "https://example.com/fhir/Patient",
      expect.objectContaining({
        method: "GET",
        headers: expect.objectContaining({
          Accept: "application/fhir+json",
        }),
      }),
    );
  });

  it("includes count parameter when provided", async () => {
    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify(sampleBundle), { status: 200 }),
    );

    await search("https://example.com/fhir", {
      resourceType: "Patient",
      count: 20,
    });

    expect(mockFetch).toHaveBeenCalledWith(
      expect.stringContaining("_count=20"),
      expect.any(Object),
    );
  });

  it("includes filter parameters when provided", async () => {
    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify(sampleBundle), { status: 200 }),
    );

    await search("https://example.com/fhir", {
      resourceType: "Patient",
      filters: ["name.family = 'Smith'", "active = true"],
    });

    const url = mockFetch.mock.calls[0][0] as string;
    expect(url).toContain("filter=");
  });

  it("includes Authorization header when access token provided", async () => {
    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify(sampleBundle), { status: 200 }),
    );

    await search("https://example.com/fhir", {
      resourceType: "Patient",
      accessToken: "test-token",
    });

    expect(mockFetch).toHaveBeenCalledWith(
      expect.any(String),
      expect.objectContaining({
        headers: expect.objectContaining({
          Authorization: "Bearer test-token",
        }),
      }),
    );
  });

  it("omits Authorization header when no access token", async () => {
    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify(sampleBundle), { status: 200 }),
    );

    await search("https://example.com/fhir", {
      resourceType: "Patient",
    });

    const headers = mockFetch.mock.calls[0][1].headers;
    expect(headers).not.toHaveProperty("Authorization");
  });

  it("returns Bundle response", async () => {
    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify(sampleBundle), { status: 200 }),
    );

    const result = await search("https://example.com/fhir", {
      resourceType: "Patient",
    });

    expect(result.resourceType).toBe("Bundle");
    expect(result.total).toBe(1);
  });

  it("throws UnauthorizedError on 401 response", async () => {
    mockFetch.mockResolvedValueOnce(
      new Response("Unauthorized", { status: 401 }),
    );

    await expect(
      search("https://example.com/fhir", { resourceType: "Patient" }),
    ).rejects.toThrow(UnauthorizedError);
  });

  it("throws Error with status and body on other errors", async () => {
    mockFetch.mockResolvedValueOnce(
      new Response("Server error", { status: 500 }),
    );

    await expect(
      search("https://example.com/fhir", { resourceType: "Patient" }),
    ).rejects.toThrow("Search failed: 500");
  });
});

describe("read", () => {
  it("makes GET request to correct URL with resource type and ID", async () => {
    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify(samplePatient), { status: 200 }),
    );

    await read("https://example.com/fhir", {
      resourceType: "Patient",
      id: "123",
    });

    expect(mockFetch).toHaveBeenCalledWith(
      "https://example.com/fhir/Patient/123",
      expect.objectContaining({
        method: "GET",
        headers: expect.objectContaining({
          Accept: "application/fhir+json",
        }),
      }),
    );
  });

  it("includes Authorization header when access token provided", async () => {
    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify(samplePatient), { status: 200 }),
    );

    await read("https://example.com/fhir", {
      resourceType: "Patient",
      id: "123",
      accessToken: "test-token",
    });

    expect(mockFetch).toHaveBeenCalledWith(
      expect.any(String),
      expect.objectContaining({
        headers: expect.objectContaining({
          Authorization: "Bearer test-token",
        }),
      }),
    );
  });

  it("returns Resource response", async () => {
    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify(samplePatient), { status: 200 }),
    );

    const result = await read("https://example.com/fhir", {
      resourceType: "Patient",
      id: "123",
    });

    expect(result.resourceType).toBe("Patient");
    expect(result.id).toBe("123");
  });

  it("throws UnauthorizedError on 401 response", async () => {
    mockFetch.mockResolvedValueOnce(
      new Response("Unauthorized", { status: 401 }),
    );

    await expect(
      read("https://example.com/fhir", { resourceType: "Patient", id: "123" }),
    ).rejects.toThrow(UnauthorizedError);
  });

  it("throws NotFoundError on 404 response", async () => {
    mockFetch.mockResolvedValueOnce(new Response("Not found", { status: 404 }));

    await expect(
      read("https://example.com/fhir", { resourceType: "Patient", id: "123" }),
    ).rejects.toThrow(NotFoundError);
  });
});

describe("create", () => {
  it("makes POST request to correct URL with resource type", async () => {
    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify(samplePatient), { status: 201 }),
    );

    await create("https://example.com/fhir", {
      resourceType: "Patient",
      resource: samplePatient,
    });

    expect(mockFetch).toHaveBeenCalledWith(
      "https://example.com/fhir/Patient",
      expect.objectContaining({
        method: "POST",
        headers: expect.objectContaining({
          Accept: "application/fhir+json",
          "Content-Type": "application/fhir+json",
        }),
        body: JSON.stringify(samplePatient),
      }),
    );
  });

  it("includes Authorization header when access token provided", async () => {
    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify(samplePatient), { status: 201 }),
    );

    await create("https://example.com/fhir", {
      resourceType: "Patient",
      resource: samplePatient,
      accessToken: "test-token",
    });

    expect(mockFetch).toHaveBeenCalledWith(
      expect.any(String),
      expect.objectContaining({
        headers: expect.objectContaining({
          Authorization: "Bearer test-token",
        }),
      }),
    );
  });

  it("returns created Resource", async () => {
    const createdPatient = { ...samplePatient, id: "new-id" };
    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify(createdPatient), { status: 201 }),
    );

    const result = await create("https://example.com/fhir", {
      resourceType: "Patient",
      resource: samplePatient,
    });

    expect(result.id).toBe("new-id");
  });

  it("throws UnauthorizedError on 401 response", async () => {
    mockFetch.mockResolvedValueOnce(
      new Response("Unauthorized", { status: 401 }),
    );

    await expect(
      create("https://example.com/fhir", {
        resourceType: "Patient",
        resource: samplePatient,
      }),
    ).rejects.toThrow(UnauthorizedError);
  });
});

describe("update", () => {
  it("makes PUT request to correct URL with resource type and ID", async () => {
    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify(samplePatient), { status: 200 }),
    );

    await update("https://example.com/fhir", {
      resourceType: "Patient",
      id: "123",
      resource: samplePatient,
    });

    expect(mockFetch).toHaveBeenCalledWith(
      "https://example.com/fhir/Patient/123",
      expect.objectContaining({
        method: "PUT",
        headers: expect.objectContaining({
          Accept: "application/fhir+json",
          "Content-Type": "application/fhir+json",
        }),
        body: JSON.stringify(samplePatient),
      }),
    );
  });

  it("includes Authorization header when access token provided", async () => {
    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify(samplePatient), { status: 200 }),
    );

    await update("https://example.com/fhir", {
      resourceType: "Patient",
      id: "123",
      resource: samplePatient,
      accessToken: "test-token",
    });

    expect(mockFetch).toHaveBeenCalledWith(
      expect.any(String),
      expect.objectContaining({
        headers: expect.objectContaining({
          Authorization: "Bearer test-token",
        }),
      }),
    );
  });

  it("returns updated Resource", async () => {
    mockFetch.mockResolvedValueOnce(
      new Response(JSON.stringify(samplePatient), { status: 200 }),
    );

    const result = await update("https://example.com/fhir", {
      resourceType: "Patient",
      id: "123",
      resource: samplePatient,
    });

    expect(result.resourceType).toBe("Patient");
  });

  it("throws UnauthorizedError on 401 response", async () => {
    mockFetch.mockResolvedValueOnce(
      new Response("Unauthorized", { status: 401 }),
    );

    await expect(
      update("https://example.com/fhir", {
        resourceType: "Patient",
        id: "123",
        resource: samplePatient,
      }),
    ).rejects.toThrow(UnauthorizedError);
  });

  it("throws NotFoundError on 404 response", async () => {
    mockFetch.mockResolvedValueOnce(new Response("Not found", { status: 404 }));

    await expect(
      update("https://example.com/fhir", {
        resourceType: "Patient",
        id: "123",
        resource: samplePatient,
      }),
    ).rejects.toThrow(NotFoundError);
  });
});

describe("deleteResource", () => {
  it("makes DELETE request to correct URL with resource type and ID", async () => {
    mockFetch.mockResolvedValueOnce(new Response(null, { status: 204 }));

    await deleteResource("https://example.com/fhir", {
      resourceType: "Patient",
      id: "123",
    });

    expect(mockFetch).toHaveBeenCalledWith(
      "https://example.com/fhir/Patient/123",
      expect.objectContaining({
        method: "DELETE",
        headers: expect.objectContaining({
          Accept: "application/fhir+json",
        }),
      }),
    );
  });

  it("includes Authorization header when access token provided", async () => {
    mockFetch.mockResolvedValueOnce(new Response(null, { status: 204 }));

    await deleteResource("https://example.com/fhir", {
      resourceType: "Patient",
      id: "123",
      accessToken: "test-token",
    });

    expect(mockFetch).toHaveBeenCalledWith(
      expect.any(String),
      expect.objectContaining({
        headers: expect.objectContaining({
          Authorization: "Bearer test-token",
        }),
      }),
    );
  });

  it("resolves without returning value on success", async () => {
    mockFetch.mockResolvedValueOnce(new Response(null, { status: 204 }));

    const result = await deleteResource("https://example.com/fhir", {
      resourceType: "Patient",
      id: "123",
    });

    expect(result).toBeUndefined();
  });

  it("throws UnauthorizedError on 401 response", async () => {
    mockFetch.mockResolvedValueOnce(
      new Response("Unauthorized", { status: 401 }),
    );

    await expect(
      deleteResource("https://example.com/fhir", {
        resourceType: "Patient",
        id: "123",
      }),
    ).rejects.toThrow(UnauthorizedError);
  });

  it("throws NotFoundError on 404 response", async () => {
    mockFetch.mockResolvedValueOnce(new Response("Not found", { status: 404 }));

    await expect(
      deleteResource("https://example.com/fhir", {
        resourceType: "Patient",
        id: "123",
      }),
    ).rejects.toThrow(NotFoundError);
  });
});
