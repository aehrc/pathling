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
