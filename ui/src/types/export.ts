/**
 * Type definitions for FHIR bulk export operations.
 *
 * @author John Grimes
 */

export type ExportLevel =
  | "system"
  | "patient-type"
  | "patient-instance"
  | "group";

export interface ExportRequest {
  level: ExportLevel;
  resourceTypes?: string[];
  since?: string;
  until?: string;
  elements?: string;
  patientId?: string;
  groupId?: string;
}

export interface ExportManifestOutput {
  type: string;
  url: string;
  count?: number;
}

export interface ExportManifestError {
  type: string;
  url: string;
}

export interface ExportManifest {
  transactionTime: string;
  request: string;
  requiresAccessToken: boolean;
  output: ExportManifestOutput[];
  deleted?: ExportManifestOutput[];
  error?: ExportManifestError[];
}

// Common FHIR resource types for export selection.
export const RESOURCE_TYPES = [
  "AllergyIntolerance",
  "CarePlan",
  "CareTeam",
  "Claim",
  "Condition",
  "Coverage",
  "Device",
  "DiagnosticReport",
  "DocumentReference",
  "Encounter",
  "ExplanationOfBenefit",
  "Goal",
  "Immunization",
  "Location",
  "Medication",
  "MedicationRequest",
  "MedicationStatement",
  "Observation",
  "Organization",
  "Patient",
  "Practitioner",
  "PractitionerRole",
  "Procedure",
  "Provenance",
  "RelatedPerson",
  "ServiceRequest",
] as const;
