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
