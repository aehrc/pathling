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
 * Application configuration.
 *
 * @author John Grimes
 */

/** The default OAuth client ID for the admin UI. */
export const DEFAULT_CLIENT_ID = "pathling-admin-ui";

/** The default OAuth scopes for the admin UI. */
export const DEFAULT_SCOPE = "openid profile user/*.*";

/**
 * Gets the OAuth client ID from build-time environment variable.
 *
 * @returns The VITE_CLIENT_ID environment variable value, or undefined if not set.
 */
export function getEnvClientId(): string | undefined {
  return import.meta.env.VITE_CLIENT_ID;
}

/**
 * Gets the OAuth scope from build-time environment variable.
 *
 * @returns The VITE_SCOPE environment variable value, or the default scope.
 */
export function getScope(): string {
  return import.meta.env.VITE_SCOPE || DEFAULT_SCOPE;
}

export const config = {
  fhirBaseUrl: import.meta.env.VITE_FHIR_BASE_URL || "/fhir",
};
