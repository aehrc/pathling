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
  serverVersion?: string;
  fhirVersion?: string;
  publisher?: string;
  description?: string;
  resources?: ResourceCapability[];
  operations?: OperationCapability[];
}

export interface ResourceCapability {
  type: string;
  operations: string[];
}

export interface OperationCapability {
  name: string;
  definition?: string;
}

/**
 * Fetches the CapabilityStatement and extracts server information.
 */
export async function checkServerCapabilities(
  fhirBaseUrl: string,
): Promise<ServerCapabilities> {
  try {
    const response = await fetch(`${fhirBaseUrl}/metadata`, {
      headers: { Accept: "application/fhir+json" },
    });
    if (!response.ok) {
      throw new Error(`Failed to fetch metadata: ${response.status}`);
    }

    const capability = await response.json();

    const serverName =
      capability.implementation?.description || capability.name;
    const serverVersion = capability.software?.version;
    const fhirVersion = capability.fhirVersion;
    const publisher = capability.publisher;
    const description = capability.description;

    // Extract resource capabilities.
    const resources: ResourceCapability[] = [];
    const operations: OperationCapability[] = [];

    const restEntries = capability.rest || [];
    let authRequired = false;

    for (const rest of restEntries) {
      // Check security for auth requirement.
      const security = rest.security;
      if (security) {
        const services = security.service || [];
        for (const service of services) {
          const codings = service.coding || [];
          for (const coding of codings) {
            if (
              coding.system === SMART_SERVICE_SYSTEM &&
              coding.code === SMART_SERVICE_CODE
            ) {
              authRequired = true;
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
            authRequired = true;
          }
        }
      }

      // Extract resources.
      const restResources = rest.resource || [];
      for (const resource of restResources) {
        const ops: string[] = [];

        // Standard interactions.
        const interactions = resource.interaction || [];
        for (const interaction of interactions) {
          if (interaction.code) {
            ops.push(interaction.code);
          }
        }

        // Resource-level operations.
        const resourceOps = resource.operation || [];
        for (const op of resourceOps) {
          if (op.name) {
            ops.push(`$${op.name}`);
          }
        }

        if (resource.type) {
          resources.push({
            type: resource.type,
            operations: ops,
          });
        }
      }

      // Extract system-level operations.
      const systemOps = rest.operation || [];
      for (const op of systemOps) {
        if (op.name) {
          operations.push({
            name: op.name,
            definition: op.definition,
          });
        }
      }
    }

    return {
      authRequired,
      serverName,
      serverVersion,
      fhirVersion,
      publisher,
      description,
      resources,
      operations,
    };
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
