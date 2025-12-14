/**
 * SMART on FHIR authentication service.
 *
 * @author John Grimes
 */

import FHIR from "fhirclient";
import type Client from "fhirclient/lib/Client";

const CLIENT_ID = "pathling-export-ui";

/**
 * Discovers SMART configuration from the FHIR server.
 */
export async function discoverSmartConfig(
  fhirBaseUrl: string
): Promise<boolean> {
  try {
    const response = await fetch(
      `${fhirBaseUrl}/.well-known/smart-configuration`
    );
    if (response.ok) {
      return true;
    }
    // Fall back to checking metadata endpoint.
    const metadataResponse = await fetch(`${fhirBaseUrl}/metadata`);
    return metadataResponse.ok;
  } catch {
    return false;
  }
}

/**
 * Initiates SMART on FHIR standalone launch authorization.
 */
export async function initiateAuth(fhirBaseUrl: string): Promise<void> {
  await FHIR.oauth2.authorize({
    iss: fhirBaseUrl,
    clientId: CLIENT_ID,
    scope: "openid profile user/*.read",
    redirectUri: `${window.location.origin}/callback`,
  });
}

/**
 * Completes the SMART on FHIR authorization flow after redirect.
 */
export async function completeAuth(): Promise<Client> {
  return FHIR.oauth2.ready();
}

/**
 * Checks if there is an existing SMART session.
 */
export function hasExistingSession(): boolean {
  return sessionStorage.getItem("SMART_KEY") !== null;
}
