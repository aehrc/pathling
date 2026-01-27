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
 * Tests for the useClipboard hook.
 *
 * @author John Grimes
 */

import { renderHook } from "@testing-library/react";
import { beforeEach, describe, expect, it, vi } from "vitest";

import { useClipboard } from "../useClipboard";

describe("useClipboard", () => {
  const mockWriteText = vi.fn();

  beforeEach(() => {
    vi.clearAllMocks();
    Object.assign(navigator, {
      clipboard: { writeText: mockWriteText },
    });
  });

  it("returns a function", () => {
    const { result } = renderHook(() => useClipboard());
    expect(typeof result.current).toBe("function");
  });

  it("copies text to clipboard when called", async () => {
    mockWriteText.mockResolvedValue(undefined);
    const { result } = renderHook(() => useClipboard());

    await result.current("test text");

    expect(mockWriteText).toHaveBeenCalledWith("test text");
    expect(mockWriteText).toHaveBeenCalledTimes(1);
  });

  it("copies JSON to clipboard", async () => {
    mockWriteText.mockResolvedValue(undefined);
    const { result } = renderHook(() => useClipboard());
    const json = JSON.stringify({ foo: "bar" }, null, 2);

    await result.current(json);

    expect(mockWriteText).toHaveBeenCalledWith(json);
  });

  it("returns the same function on re-render", () => {
    const { result, rerender } = renderHook(() => useClipboard());
    const firstFn = result.current;

    rerender();

    expect(result.current).toBe(firstFn);
  });
});
