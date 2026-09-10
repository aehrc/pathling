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
 * Hook holding the pending and error state of a single user-initiated
 * authorisation attempt. The state belongs to the control that started the
 * attempt, so each login control reports its own progress and its own failure.
 *
 * @author John Grimes
 */

import { useCallback, useRef, useState } from "react";

import { config } from "../config";
import { initiateAuth } from "../services/auth";

interface UseLoginResult {
  /** Starts an authorisation attempt, unless one is already pending. */
  login: () => Promise<void>;
  /** Whether an authorisation attempt is currently under way. */
  isPending: boolean;
  /** The reason the last attempt failed, or null if none has. */
  error: string | null;
}

/**
 * Provides a login action together with its pending and error state.
 *
 * A successful attempt navigates the browser away to the authorisation server,
 * so the pending state is deliberately left set: there is no completion to
 * render. A failed attempt clears the pending state and records the reason, so
 * the control can report it and be retried.
 *
 * @returns The login action and its pending and error state.
 * @example
 * const { login, isPending, error } = useLogin();
 * return (
 *   <>
 *     <Button loading={isPending} onClick={() => void login()}>Log in</Button>
 *     {error && <ErrorCallout message={error} />}
 *   </>
 * );
 */
export function useLogin(): UseLoginResult {
  const [isPending, setIsPending] = useState(false);
  const [error, setError] = useState<string | null>(null);

  // Guards against a second attempt started before the pending state renders.
  const pendingRef = useRef(false);

  const login = useCallback(async () => {
    if (pendingRef.current) return;
    pendingRef.current = true;
    setIsPending(true);
    setError(null);

    try {
      await initiateAuth(config.fhirBaseUrl);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Authentication failed");
      pendingRef.current = false;
      setIsPending(false);
    }
  }, []);

  return { login, isPending, error };
}
