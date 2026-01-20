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

import { useQuery } from "@tanstack/react-query";

import type { UseQueryResult } from "@tanstack/react-query";
import type { CapabilityStatement } from "fhir/r4";

/**
 * Resource capability from server.
 */
export interface ResourceCapability {
  type: string;
  operations: string[];
}

/**
 * Operation capability from server.
 */
export interface OperationCapability {
  name: string;
  definition?: string;
}

/**
 * Server capabilities.
 */
export interface ServerCapabilities {
  authRequired: boolean;
  serverName?: string;
  serverVersion?: string;
  fhirVersion?: string;
  publisher?: string;
  description?: string;
  resources?: ResourceCapability[];
  resourceTypes: string[];
  operations?: OperationCapability[];
}

/**
 * Result of useServerCapabilities hook.
 */
export type UseServerCapabilitiesResult = UseQueryResult<
  ServerCapabilities,
  Error
>;

/**
 * Fetch server capabilities from the CapabilityStatement.
 */
export type UseServerCapabilitiesFn = (
  fhirBaseUrl: string | null | undefined,
) => UseServerCapabilitiesResult;

const SMART_SERVICE_SYSTEM =
  "http://terminology.hl7.org/CodeSystem/restful-security-service";
const SMART_SERVICE_CODE = "SMART-on-FHIR";

/**
 * Parses a CapabilityStatement to extract server capabilities.
 *
 * @param capability - The FHIR CapabilityStatement resource.
 * @returns Parsed server capabilities object.
 */
function parseCapabilities(
  capability: CapabilityStatement,
): ServerCapabilities {
  const serverName = capability.implementation?.description || capability.name;
  const serverVersion = capability.software?.version;
  const fhirVersion = capability.fhirVersion;
  const publisher = capability.publisher;
  const description = capability.description;

  const resources: ResourceCapability[] = [];
  const operations: OperationCapability[] = [];
  let authRequired = false;

  for (const rest of capability.rest ?? []) {
    // Check security for auth requirement.
    const security = rest.security;
    if (security) {
      for (const service of security.service ?? []) {
        for (const coding of service.coding ?? []) {
          if (
            coding.system === SMART_SERVICE_SYSTEM &&
            coding.code === SMART_SERVICE_CODE
          ) {
            authRequired = true;
          }
        }
      }

      // Also check for OAuth extension as a fallback.
      for (const ext of security.extension ?? []) {
        if (
          ext.url?.includes("oauth-uris") ||
          ext.url?.includes("smart-configuration")
        ) {
          authRequired = true;
        }
      }
    }

    // Extract resources.
    for (const resource of rest.resource ?? []) {
      const ops: string[] = [];

      for (const interaction of resource.interaction ?? []) {
        if (interaction.code) {
          ops.push(interaction.code);
        }
      }

      for (const op of resource.operation ?? []) {
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
    for (const op of rest.operation ?? []) {
      if (op.name) {
        operations.push({
          name: op.name,
          definition: op.definition,
        });
      }
    }
  }

  // Sorted, deduplicated list of resource type names for use in dropdowns.
  // Using Set to ensure no duplicates even if CapabilityStatement has duplicate entries.
  const resourceTypes = [...new Set(resources.map((r) => r.type))].sort();

  return {
    authRequired,
    serverName,
    serverVersion,
    fhirVersion,
    publisher,
    description,
    resources,
    resourceTypes,
    operations,
  };
}

/**
 * Fetch server capabilities from the CapabilityStatement.
 *
 * @param fhirBaseUrl - The FHIR server base URL.
 * @returns Query result with server capabilities.
 */
export const useServerCapabilities: UseServerCapabilitiesFn = (fhirBaseUrl) => {
  return useQuery<ServerCapabilities, Error>({
    queryKey: ["serverCapabilities", fhirBaseUrl],
    queryFn: async () => {
      const response = await fetch(`${fhirBaseUrl}/metadata`, {
        headers: { Accept: "application/fhir+json" },
      });
      if (!response.ok) {
        throw new Error(`Failed to fetch metadata: ${response.status}`);
      }
      const capability: CapabilityStatement = await response.json();
      return parseCapabilities(capability);
    },
    enabled: !!fhirBaseUrl,
  });
};
