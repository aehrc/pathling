/**
 * Hook for copying text to the clipboard.
 *
 * @author John Grimes
 */

import { useCallback } from "react";

/**
 * Returns a function that copies the provided text to the clipboard.
 *
 * @returns A function that accepts text and copies it to the clipboard.
 */
export function useClipboard(): (text: string) => Promise<void> {
  return useCallback(async (text: string) => {
    await navigator.clipboard.writeText(text);
  }, []);
}
