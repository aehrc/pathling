/**
 * Application configuration.
 *
 * @author John Grimes
 */

/** The default OAuth client ID for the admin UI. */
export const DEFAULT_CLIENT_ID = "pathling-admin-ui";

/**
 * Gets the OAuth client ID from build-time environment variable.
 *
 * @returns The VITE_CLIENT_ID environment variable value, or undefined if not set.
 */
export function getEnvClientId(): string | undefined {
  return import.meta.env.VITE_CLIENT_ID;
}

export const config = {
  fhirBaseUrl: import.meta.env.VITE_FHIR_BASE_URL || "/fhir",
};
