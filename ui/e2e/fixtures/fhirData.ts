/**
 * Mock FHIR data for E2E tests.
 *
 * @author John Grimes
 */

import type { Bundle, CapabilityStatement, Parameters, Patient } from "fhir/r4";

/**
 * Mock CapabilityStatement without authentication required.
 * Used for most functional tests.
 */
export const mockCapabilityStatement: CapabilityStatement = {
  resourceType: "CapabilityStatement",
  status: "active",
  date: "2024-01-01",
  kind: "instance",
  fhirVersion: "4.0.1",
  format: ["json"],
  rest: [
    {
      mode: "server",
      resource: [
        {
          type: "Patient",
          interaction: [
            { code: "read" },
            { code: "search-type" },
            { code: "delete" },
          ],
        },
        {
          type: "Observation",
          interaction: [{ code: "read" }, { code: "search-type" }],
        },
        {
          type: "Condition",
          interaction: [{ code: "read" }, { code: "search-type" }],
        },
      ],
    },
  ],
};

/**
 * Mock CapabilityStatement with SMART-on-FHIR authentication required.
 * Used for testing auth-required behaviour.
 */
export const mockCapabilityStatementWithAuth: CapabilityStatement = {
  resourceType: "CapabilityStatement",
  status: "active",
  date: "2024-01-01",
  kind: "instance",
  fhirVersion: "4.0.1",
  format: ["json"],
  rest: [
    {
      mode: "server",
      security: {
        service: [
          {
            coding: [
              {
                system:
                  "http://terminology.hl7.org/CodeSystem/restful-security-service",
                code: "SMART-on-FHIR",
              },
            ],
          },
        ],
      },
      resource: [
        {
          type: "Patient",
          interaction: [
            { code: "read" },
            { code: "search-type" },
            { code: "delete" },
          ],
        },
      ],
    },
  ],
};

/**
 * Mock Patient resource with name for summary testing.
 */
export const mockPatient: Patient = {
  resourceType: "Patient",
  id: "patient-123",
  name: [
    {
      family: "Smith",
      given: ["John", "William"],
    },
  ],
  gender: "male",
  birthDate: "1980-01-15",
};

/**
 * Second mock Patient for testing multiple results.
 */
export const mockPatient2: Patient = {
  resourceType: "Patient",
  id: "patient-456",
  name: [
    {
      family: "Jones",
      given: ["Jane"],
    },
  ],
  gender: "female",
  birthDate: "1990-05-20",
};

/**
 * Mock Bundle with Patient search results.
 */
export const mockPatientBundle: Bundle = {
  resourceType: "Bundle",
  type: "searchset",
  total: 25,
  entry: [{ resource: mockPatient }, { resource: mockPatient2 }],
};

/**
 * Mock empty Bundle for testing no results state.
 */
export const mockEmptyBundle: Bundle = {
  resourceType: "Bundle",
  type: "searchset",
  total: 0,
  entry: [],
};

/**
 * Mock error response for testing error handling.
 */
export const mockErrorResponse = {
  resourceType: "OperationOutcome",
  issue: [
    {
      severity: "error",
      code: "invalid",
      diagnostics: "Invalid filter expression",
    },
  ],
};

/**
 * Mock job status response indicating job is still in progress.
 */
export const mockJobStatusInProgress = {
  status: "in-progress",
};

/**
 * Mock job status response indicating job completed successfully.
 */
export const mockJobStatusComplete = {
  status: "complete",
  result: {
    transactionTime: "2024-01-01T00:00:00Z",
    request: "/$import",
    output: [],
  },
};

/**
 * Mock job status response indicating job failed.
 */
export const mockJobStatusError = {
  status: "error",
  error: "Import failed: Invalid source URL",
};

/**
 * Mock export manifest for bulk export completion.
 */
export const mockExportManifest: Parameters = {
  resourceType: "Parameters",
  parameter: [
    {
      name: "transactionTime",
      valueInstant: "2024-01-01T12:00:00Z",
    },
    {
      name: "request",
      valueString: "/$export",
    },
    {
      name: "requiresAccessToken",
      valueBoolean: false,
    },
    {
      name: "output",
      part: [
        {
          name: "type",
          valueCode: "Patient",
        },
        {
          name: "url",
          valueUri: "http://localhost:3000/export-files?file=Patient.ndjson",
        },
        {
          name: "count",
          valueInteger: 100,
        },
      ],
    },
    {
      name: "output",
      part: [
        {
          name: "type",
          valueCode: "Observation",
        },
        {
          name: "url",
          valueUri:
            "http://localhost:3000/export-files?file=Observation.ndjson",
        },
        {
          name: "count",
          valueInteger: 250,
        },
      ],
    },
  ],
};

// ============================================================================
// SQL on FHIR / ViewDefinition Mocks
// ============================================================================

/**
 * Mock ViewDefinition resource for patient demographics.
 */
export const mockViewDefinition1 = {
  resourceType: "ViewDefinition",
  id: "patient-demographics",
  name: "Patient Demographics",
  resource: "Patient",
  status: "active",
  select: [
    {
      column: [
        { path: "id", name: "patient_id" },
        { path: "name.first().family", name: "family_name" },
      ],
    },
  ],
};

/**
 * Mock ViewDefinition resource for observation vitals.
 */
export const mockViewDefinition2 = {
  resourceType: "ViewDefinition",
  id: "observation-vitals",
  name: "Observation Vitals",
  resource: "Observation",
  status: "active",
  select: [
    {
      column: [
        { path: "id", name: "obs_id" },
        { path: "code.coding.first().display", name: "observation_type" },
      ],
    },
  ],
};

/**
 * Mock Bundle containing ViewDefinition search results.
 */
export const mockViewDefinitionBundle: Bundle = {
  resourceType: "Bundle",
  type: "searchset",
  total: 2,
  entry: [
    {
      resource: mockViewDefinition1 as Bundle["entry"] extends (infer T)[]
        ? T extends { resource?: infer R }
          ? R
          : never
        : never,
    },
    {
      resource: mockViewDefinition2 as Bundle["entry"] extends (infer T)[]
        ? T extends { resource?: infer R }
          ? R
          : never
        : never,
    },
  ],
};

/**
 * Mock empty ViewDefinition Bundle for testing no definitions state.
 */
export const mockEmptyViewDefinitionBundle: Bundle = {
  resourceType: "Bundle",
  type: "searchset",
  total: 0,
  entry: [],
};

/**
 * Mock NDJSON response for view run with results.
 * Each line is a separate JSON object.
 */
export const mockViewRunNdjson =
  '{"patient_id":"p1","family_name":"Smith"}\n{"patient_id":"p2","family_name":"Jones"}';

/**
 * Mock empty NDJSON response for view run with no results.
 */
export const mockEmptyViewRunNdjson = "";

// ============================================================================
// Authentication Mocks
// ============================================================================

/**
 * Mock SMART configuration endpoint response.
 * Used when testing OAuth flow initiation.
 */
export const mockSmartConfiguration = {
  authorization_endpoint: "https://auth.example.com/authorize",
  token_endpoint: "https://auth.example.com/token",
  capabilities: ["launch-standalone", "client-public"],
};
