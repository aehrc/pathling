/**
 * Type definitions for FHIR bulk export operations.
 *
 * @author John Grimes
 */

export type ExportLevel =
  | "system"
  | "all-patients"
  | "patient"
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
