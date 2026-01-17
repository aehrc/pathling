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
 * Tests for the useViewExport hook's deleteJob functionality.
 *
 * @author John Grimes
 */

import { renderHook } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

const mockStartWith = vi.fn();
const mockCancel = vi.fn();
const mockDeleteJob = vi.fn();
const mockReset = vi.fn();

// Mock the useAsyncJob hook.
vi.mock("../useAsyncJob", () => ({
  useAsyncJob: vi.fn(() => ({
    startWith: mockStartWith,
    cancel: mockCancel,
    deleteJob: mockDeleteJob,
    reset: mockReset,
    status: "idle",
    result: undefined,
    error: undefined,
    progress: undefined,
    request: undefined,
  })),
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

// Mock the useAsyncJobCallbacks hook.
vi.mock("../useAsyncJobCallbacks", () => ({
  useAsyncJobCallbacks: vi.fn(() => undefined),
}));

import { useAsyncJob } from "../useAsyncJob";
import { useViewExport } from "../useViewExport";

describe("useViewExport", () => {
  beforeEach(() => {
    vi.clearAllMocks();
  });

  afterEach(() => {
    vi.clearAllMocks();
  });

  describe("deleteJob", () => {
    it("exposes deleteJob function from useAsyncJob", () => {
      const { result } = renderHook(() => useViewExport());

      expect(result.current.deleteJob).toBe(mockDeleteJob);
    });

    it("calls deleteJob when invoked", async () => {
      mockDeleteJob.mockResolvedValue(undefined);

      const { result } = renderHook(() => useViewExport());

      await result.current.deleteJob();

      expect(mockDeleteJob).toHaveBeenCalledTimes(1);
    });

    it("does not expose cancel as deleteJob (they are separate functions)", () => {
      const { result } = renderHook(() => useViewExport());

      expect(result.current.deleteJob).not.toBe(result.current.cancel);
    });

    it("useAsyncJob is called when hook is rendered", () => {
      renderHook(() => useViewExport());

      expect(useAsyncJob).toHaveBeenCalled();
    });
  });
});
