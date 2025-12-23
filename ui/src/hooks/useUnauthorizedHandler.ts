/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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
 *
 * Author: John Grimes
 */

import { useCallback, useEffect, useRef } from "react";
import { useAuth } from "../contexts/AuthContext";

/**
 * Hook that provides a deduplicated handler for unauthorized (401) errors.
 *
 * This hook prevents multiple simultaneous 401 responses from triggering
 * multiple login prompts by tracking whether the error has already been
 * handled. The flag resets when the user re-authenticates.
 *
 * @returns A function that handles unauthorized errors. Can be called with
 * an optional error message - if provided, it checks if the message indicates
 * a 401 error before triggering the login prompt.
 *
 * @example
 * // Basic usage - always triggers login prompt (deduplicated)
 * const handleUnauthorizedError = useUnauthorizedHandler();
 * if (response.status === 401) {
 *   handleUnauthorizedError();
 * }
 *
 * @example
 * // With error message - only triggers if message looks like 401
 * const handleUnauthorizedError = useUnauthorizedHandler();
 * handleUnauthorizedError(errorMessage);
 */
export function useUnauthorizedHandler(): (errorMessage?: string) => void {
  const { isAuthenticated, clearSessionAndPromptLogin } = useAuth();
  const handledRef = useRef(false);

  // Reset the flag when user becomes authenticated.
  useEffect(() => {
    if (isAuthenticated) {
      handledRef.current = false;
    }
  }, [isAuthenticated]);

  return useCallback(
    (errorMessage?: string) => {
      // If called with a message, check if it looks like a 401.
      if (errorMessage !== undefined) {
        const isUnauthorized =
          errorMessage.includes("401") ||
          errorMessage.toLowerCase().includes("unauthorized");
        if (!isUnauthorized) return;
      }

      // Prevent duplicate handling.
      if (handledRef.current) return;
      handledRef.current = true;

      clearSessionAndPromptLogin();
    },
    [clearSessionAndPromptLogin],
  );
}
