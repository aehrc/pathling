/**
 * SMART on FHIR authentication service.
 *
 * @author John Grimes
 */

import type { CapabilityStatement } from "fhir/r4";
import FHIR from "fhirclient";
import type Client from "fhirclient/lib/Client";
import { DEFAULT_CLIENT_ID, getEnvClientId } from "../config";

export { DEFAULT_CLIENT_ID };

const SMART_SERVICE_SYSTEM =
  "http://terminology.hl7.org/CodeSystem/restful-security-service";
const SMART_SERVICE_CODE = "SMART-on-FHIR";
const RETURN_URL_KEY = "pathling_return_url";

/**
 * Response from the SMART configuration endpoint.
 */
interface SmartConfiguration {
  issuer?: string;
  authorization_endpoint?: string;
  token_endpoint?: string;
  revocation_endpoint?: string;
  admin_ui_client_id?: string;
}

/**
 * Fetches the SMART configuration from the server.
 *
 * @param fhirBaseUrl - The base URL of the FHIR server.
 * @returns The SMART configuration, or null if unavailable.
 */
async function fetchSmartConfiguration(
  fhirBaseUrl: string,
): Promise<SmartConfiguration | null> {
  try {
    const response = await fetch(
      `${fhirBaseUrl}/.well-known/smart-configuration`,
    );
    if (!response.ok) {
      return null;
    }
    return await response.json();
  } catch {
    return null;
  }
}

/**
 * Gets the OAuth client ID to use for authentication.
 *
 * Precedence:
 * 1. admin_ui_client_id from SMART configuration
 * 2. VITE_CLIENT_ID environment variable (set at build time)
 * 3. Default value (pathling-admin-ui)
 *
 * @param fhirBaseUrl - The base URL of the FHIR server.
 * @returns The client ID to use for OAuth flows.
 */
export async function getClientId(fhirBaseUrl: string): Promise<string> {
  // Try to get client ID from SMART configuration.
  const smartConfig = await fetchSmartConfiguration(fhirBaseUrl);
  if (smartConfig?.admin_ui_client_id) {
    return smartConfig.admin_ui_client_id;
  }

  // Fall back to environment variable.
  const envClientId = getEnvClientId();
  if (envClientId) {
    return envClientId;
  }

  // Fall back to default.
  return DEFAULT_CLIENT_ID;
}

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

    const capability: CapabilityStatement = await response.json();

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
 * Stores the current pathname so we can return to it after auth completes.
 *
 * @param fhirBaseUrl - The base URL of the FHIR server.
 */
export async function initiateAuth(fhirBaseUrl: string): Promise<void> {
  // Store the current path to return to after authentication.
  // Strip the /admin basename since React Router will add it back.
  const pathname = window.location.pathname.replace(/^\/admin/, "") || "/";
  sessionStorage.setItem(RETURN_URL_KEY, pathname);

  // Convert relative URLs to absolute URLs for fhirclient compatibility.
  const absoluteUrl = fhirBaseUrl.startsWith("http")
    ? fhirBaseUrl
    : `${window.location.origin}${fhirBaseUrl}`;

  // Get the client ID from server configuration, env var, or default.
  const clientId = await getClientId(fhirBaseUrl);

  await FHIR.oauth2.authorize({
    iss: absoluteUrl,
    clientId,
    scope: "openid profile user/*.read",
    redirectUri: `${window.location.origin}/admin/callback`,
  });
}

/**
 * Gets the stored return URL from before authentication without clearing it.
 * Returns "/" if no URL was stored.
 */
export function getReturnUrl(): string {
  return sessionStorage.getItem(RETURN_URL_KEY) || "/";
}

/**
 * Clears the stored return URL. Call this after successful navigation.
 */
export function clearReturnUrl(): void {
  sessionStorage.removeItem(RETURN_URL_KEY);
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
