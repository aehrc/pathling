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
 * Tests for the useDownloadFile hook.
 *
 * This test suite verifies that the useDownloadFile hook correctly fetches files
 * with authentication headers and handles errors appropriately. DOM manipulation
 * for blob downloads is tested at a higher level to avoid conflicts with React's
 * internal DOM operations.
 *
 * @author John Grimes
 */

import { renderHook } from "@testing-library/react";
import { afterEach, beforeEach, describe, expect, it, vi } from "vitest";

// Mock the API utils module.
vi.mock("../../api/utils", () => ({
  checkResponse: vi.fn(),
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

import { checkResponse } from "../../api/utils";
import { useAuth } from "../../contexts/AuthContext";
import { useDownloadFile } from "../useDownloadFile";

describe("useDownloadFile", () => {
  beforeEach(() => {
    vi.clearAllMocks();

    // Mock URL object methods.
    vi.spyOn(URL, "createObjectURL").mockReturnValue(
      "blob:http://localhost/mock-blob-url",
    );
    vi.spyOn(URL, "revokeObjectURL").mockImplementation(() => {});
  });

  afterEach(() => {
    vi.clearAllMocks();
    vi.restoreAllMocks();
  });

  describe("successful download", () => {
    it("downloads file with authentication header", async () => {
      const mockBlob = new Blob(["test data"], {
        type: "application/octet-stream",
      });
      vi.spyOn(global, "fetch").mockResolvedValue({
        ok: true,
        blob: () => Promise.resolve(mockBlob),
      } as Response);
      vi.mocked(checkResponse).mockResolvedValue(undefined);

      const { result } = renderHook(() => useDownloadFile());

      await result.current("http://example.com/file.ndjson", "export.ndjson");

      expect(fetch).toHaveBeenCalledWith("http://example.com/file.ndjson", {
        headers: { Authorization: "Bearer test-token" },
      });
    });

    it("creates blob URL for download", async () => {
      const mockBlob = new Blob(["test data"], {
        type: "application/octet-stream",
      });
      vi.spyOn(global, "fetch").mockResolvedValue({
        ok: true,
        blob: () => Promise.resolve(mockBlob),
      } as Response);
      vi.mocked(checkResponse).mockResolvedValue(undefined);

      const { result } = renderHook(() => useDownloadFile());

      await result.current("http://example.com/file.ndjson", "export.ndjson");

      expect(URL.createObjectURL).toHaveBeenCalledWith(mockBlob);
    });

    it("revokes blob URL after download", async () => {
      const mockBlob = new Blob(["test data"], {
        type: "application/octet-stream",
      });
      vi.spyOn(global, "fetch").mockResolvedValue({
        ok: true,
        blob: () => Promise.resolve(mockBlob),
      } as Response);
      vi.mocked(checkResponse).mockResolvedValue(undefined);

      const { result } = renderHook(() => useDownloadFile());

      await result.current("http://example.com/file.ndjson", "export.ndjson");

      expect(URL.revokeObjectURL).toHaveBeenCalledWith(
        "blob:http://localhost/mock-blob-url",
      );
    });
  });

  describe("without authentication", () => {
    it("does not include Authorization header when not authenticated", async () => {
      vi.mocked(useAuth).mockReturnValue({
        client: null,
      } as ReturnType<typeof useAuth>);

      const mockBlob = new Blob(["test data"], {
        type: "application/octet-stream",
      });
      vi.spyOn(global, "fetch").mockResolvedValue({
        ok: true,
        blob: () => Promise.resolve(mockBlob),
      } as Response);
      vi.mocked(checkResponse).mockResolvedValue(undefined);

      const { result } = renderHook(() => useDownloadFile());

      await result.current("http://example.com/file.ndjson", "export.ndjson");

      expect(fetch).toHaveBeenCalledWith("http://example.com/file.ndjson", {
        headers: {},
      });
    });
  });

  describe("error handling", () => {
    it("calls onError callback when response check fails", async () => {
      const testError = new Error("Unauthorized");
      vi.spyOn(global, "fetch").mockResolvedValue({
        ok: false,
        status: 401,
      } as Response);
      vi.mocked(checkResponse).mockRejectedValue(testError);
      const onError = vi.fn();

      const { result } = renderHook(() => useDownloadFile(onError));

      await expect(
        result.current("http://example.com/file.ndjson", "export.ndjson"),
      ).rejects.toThrow("Unauthorized");

      expect(onError).toHaveBeenCalledWith(testError);
    });

    it("calls onError callback when fetch throws", async () => {
      const testError = new Error("Network error");
      vi.spyOn(global, "fetch").mockRejectedValue(testError);
      const onError = vi.fn();

      const { result } = renderHook(() => useDownloadFile(onError));

      await expect(
        result.current("http://example.com/file.ndjson", "export.ndjson"),
      ).rejects.toThrow("Network error");

      expect(onError).toHaveBeenCalledWith(testError);
    });

    it("wraps non-Error objects in Error", async () => {
      vi.spyOn(global, "fetch").mockRejectedValue("string error");
      const onError = vi.fn();

      const { result } = renderHook(() => useDownloadFile(onError));

      await expect(
        result.current("http://example.com/file.ndjson", "export.ndjson"),
      ).rejects.toThrow("Download failed");

      expect(onError).toHaveBeenCalledWith(
        expect.objectContaining({
          message: "Download failed",
        }),
      );
    });

    it("re-throws error for global handling", async () => {
      const testError = new Error("Test error");
      vi.spyOn(global, "fetch").mockRejectedValue(testError);
      const onError = vi.fn();

      const { result } = renderHook(() => useDownloadFile(onError));

      await expect(
        result.current("http://example.com/file.ndjson", "export.ndjson"),
      ).rejects.toThrow("Test error");
    });
  });

  describe("function stability", () => {
    it("returns the same function reference when accessToken is unchanged", () => {
      const { result, rerender } = renderHook(() => useDownloadFile());
      const firstFn = result.current;

      rerender();

      expect(result.current).toBe(firstFn);
    });

    it("returns a new function reference when onError changes", () => {
      const onError1 = vi.fn();
      const onError2 = vi.fn();

      const { result, rerender } = renderHook(
        ({ onError }) => useDownloadFile(onError),
        { initialProps: { onError: onError1 } },
      );
      const firstFn = result.current;

      rerender({ onError: onError2 });

      expect(result.current).not.toBe(firstFn);
    });
  });

  describe("works without onError callback", () => {
    it("completes successfully without onError callback", async () => {
      const mockBlob = new Blob(["test data"], {
        type: "application/octet-stream",
      });
      vi.spyOn(global, "fetch").mockResolvedValue({
        ok: true,
        blob: () => Promise.resolve(mockBlob),
      } as Response);
      vi.mocked(checkResponse).mockResolvedValue(undefined);

      const { result } = renderHook(() => useDownloadFile());

      // Should not throw.
      await result.current("http://example.com/file.ndjson", "export.ndjson");

      expect(fetch).toHaveBeenCalled();
    });

    it("re-throws error without onError callback", async () => {
      const testError = new Error("Test error");
      vi.spyOn(global, "fetch").mockRejectedValue(testError);

      const { result } = renderHook(() => useDownloadFile());

      await expect(
        result.current("http://example.com/file.ndjson", "export.ndjson"),
      ).rejects.toThrow("Test error");
    });
  });
});
