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
 * Tests for the useAsyncJobCallbacks hook.
 *
 * This test suite verifies that the useAsyncJobCallbacks hook correctly
 * memoises callback options for stable reference, extracting onProgress,
 * onComplete, and onError callbacks.
 *
 * @author John Grimes
 */

import { renderHook } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";

import { useAsyncJobCallbacks } from "../useAsyncJobCallbacks";

describe("useAsyncJobCallbacks", () => {
  describe("callback extraction", () => {
    it("extracts onProgress callback from options", () => {
      const onProgress = vi.fn();
      const { result } = renderHook(() => useAsyncJobCallbacks({ onProgress }));

      expect(result.current.onProgress).toBe(onProgress);
    });

    it("extracts onComplete callback from options", () => {
      const onComplete = vi.fn();
      const { result } = renderHook(() => useAsyncJobCallbacks({ onComplete }));

      expect(result.current.onComplete).toBe(onComplete);
    });

    it("extracts onError callback from options", () => {
      const onError = vi.fn();
      const { result } = renderHook(() => useAsyncJobCallbacks({ onError }));

      expect(result.current.onError).toBe(onError);
    });

    it("extracts all callbacks from options", () => {
      const onProgress = vi.fn();
      const onComplete = vi.fn();
      const onError = vi.fn();

      const { result } = renderHook(() =>
        useAsyncJobCallbacks({ onProgress, onComplete, onError }),
      );

      expect(result.current.onProgress).toBe(onProgress);
      expect(result.current.onComplete).toBe(onComplete);
      expect(result.current.onError).toBe(onError);
    });
  });

  describe("undefined handling", () => {
    it("handles undefined options", () => {
      const { result } = renderHook(() => useAsyncJobCallbacks(undefined));

      expect(result.current.onProgress).toBeUndefined();
      expect(result.current.onComplete).toBeUndefined();
      expect(result.current.onError).toBeUndefined();
    });

    it("handles empty options object", () => {
      const { result } = renderHook(() => useAsyncJobCallbacks({}));

      expect(result.current.onProgress).toBeUndefined();
      expect(result.current.onComplete).toBeUndefined();
      expect(result.current.onError).toBeUndefined();
    });

    it("handles partial options", () => {
      const onComplete = vi.fn();
      const { result } = renderHook(() => useAsyncJobCallbacks({ onComplete }));

      expect(result.current.onProgress).toBeUndefined();
      expect(result.current.onComplete).toBe(onComplete);
      expect(result.current.onError).toBeUndefined();
    });
  });

  describe("memoisation", () => {
    it("returns stable reference when callbacks do not change", () => {
      const onProgress = vi.fn();
      const onComplete = vi.fn();
      const onError = vi.fn();

      const { result, rerender } = renderHook(() =>
        useAsyncJobCallbacks({ onProgress, onComplete, onError }),
      );

      const firstResult = result.current;

      rerender();

      expect(result.current).toBe(firstResult);
    });

    it("returns new reference when onProgress changes", () => {
      const onProgress1 = vi.fn();
      const onProgress2 = vi.fn();
      const onComplete = vi.fn();
      const onError = vi.fn();

      const { result, rerender } = renderHook(
        ({ onProgress }) =>
          useAsyncJobCallbacks({ onProgress, onComplete, onError }),
        { initialProps: { onProgress: onProgress1 } },
      );

      const firstResult = result.current;

      rerender({ onProgress: onProgress2 });

      expect(result.current).not.toBe(firstResult);
      expect(result.current.onProgress).toBe(onProgress2);
    });

    it("returns new reference when onComplete changes", () => {
      const onProgress = vi.fn();
      const onComplete1 = vi.fn();
      const onComplete2 = vi.fn();
      const onError = vi.fn();

      const { result, rerender } = renderHook(
        ({ onComplete }) =>
          useAsyncJobCallbacks({ onProgress, onComplete, onError }),
        { initialProps: { onComplete: onComplete1 } },
      );

      const firstResult = result.current;

      rerender({ onComplete: onComplete2 });

      expect(result.current).not.toBe(firstResult);
      expect(result.current.onComplete).toBe(onComplete2);
    });

    it("returns new reference when onError changes", () => {
      const onProgress = vi.fn();
      const onComplete = vi.fn();
      const onError1 = vi.fn();
      const onError2 = vi.fn();

      const { result, rerender } = renderHook(
        ({ onError }) =>
          useAsyncJobCallbacks({ onProgress, onComplete, onError }),
        { initialProps: { onError: onError1 } },
      );

      const firstResult = result.current;

      rerender({ onError: onError2 });

      expect(result.current).not.toBe(firstResult);
      expect(result.current.onError).toBe(onError2);
    });

    it("maintains stable reference when undefined callbacks remain undefined", () => {
      const { result, rerender } = renderHook(() =>
        useAsyncJobCallbacks(undefined),
      );

      const firstResult = result.current;

      rerender();

      expect(result.current).toBe(firstResult);
    });
  });

  describe("return type", () => {
    it("returns object with correct shape", () => {
      const { result } = renderHook(() => useAsyncJobCallbacks({}));

      expect(result.current).toHaveProperty("onProgress");
      expect(result.current).toHaveProperty("onComplete");
      expect(result.current).toHaveProperty("onError");
      expect(Object.keys(result.current)).toHaveLength(3);
    });
  });
});
