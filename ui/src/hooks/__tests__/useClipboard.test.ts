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
