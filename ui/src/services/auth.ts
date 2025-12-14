/**
 * SMART on FHIR authentication service.
 *
 * @author John Grimes
 */

import FHIR from "fhirclient";
import type Client from "fhirclient/lib/Client";

const CLIENT_ID = "pathling-export-ui";
const SMART_SERVICE_SYSTEM =
  "http://terminology.hl7.org/CodeSystem/restful-security-service";
const SMART_SERVICE_CODE = "SMART-on-FHIR";

export interface ServerCapabilities {
  authRequired: boolean;
  serverName?: string;
}

/**
 * Fetches the CapabilityStatement and determines if authentication is required.
 */
export async function checkServerCapabilities(
  fhirBaseUrl: string
): Promise<ServerCapabilities> {
  try {
    const response = await fetch(`${fhirBaseUrl}/metadata`, {
      headers: { Accept: "application/fhir+json" },
    });
    if (!response.ok) {
      throw new Error(`Failed to fetch metadata: ${response.status}`);
    }

    const capability = await response.json();
    const serverName = capability.software?.name || capability.name;

    // Check if any rest entry has SMART-on-FHIR security configured.
    const restEntries = capability.rest || [];
    for (const rest of restEntries) {
      const security = rest.security;
      if (!security) continue;

      // Check security.service for SMART-on-FHIR.
      const services = security.service || [];
      for (const service of services) {
        const codings = service.coding || [];
        for (const coding of codings) {
          if (
            coding.system === SMART_SERVICE_SYSTEM &&
            coding.code === SMART_SERVICE_CODE
          ) {
            return { authRequired: true, serverName };
          }
        }
      }

      // Also check for OAuth extension as a fallback.
      const extensions = security.extension || [];
      for (const ext of extensions) {
        if (
          ext.url?.includes("oauth-uris") ||
          ext.url?.includes("smart-configuration")
        ) {
          return { authRequired: true, serverName };
        }
      }
    }

    return { authRequired: false, serverName };
  } catch (error) {
    console.error("Failed to check server capabilities:", error);
    // Default to requiring auth if we can't determine.
    return { authRequired: true };
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
