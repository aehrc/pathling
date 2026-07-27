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
 * The bridge between a request that failed with an authorisation error and the
 * authentication state that decides what to do about it. Keeping it here rather
 * than in the application entry point means the path can be exercised without
 * booting the whole application.
 *
 * There is deliberately no deduplication: raising the prompt is idempotent, so
 * concurrent notifications produce a single prompt on their own.
 *
 * @author John Grimes
 */

// The handler registered by the authentication state, if it has mounted.
let handler: (() => void) | null = null;

/**
 * Registers the function to call when a request fails with an authorisation
 * error. Registering again replaces the previous handler.
 *
 * @param fn - The function to call on an authorisation failure.
 */
export function registerSessionExpiryHandler(fn: () => void): void {
  handler = fn;
}

/**
 * Reports that a request failed with an authorisation error. Does nothing when
 * no handler has been registered.
 */
export function notifyUnauthorized(): void {
  handler?.();
}
