/**
 * Type definitions for SQL on FHIR view export operations.
 *
 * @author John Grimes
 */

export type ViewExportFormat = "ndjson" | "csv" | "parquet";

export interface ViewExportRequest {
  viewDefinitionId?: string;
  viewDefinitionJson?: string;
  format: ViewExportFormat;
}

export interface ViewExportManifestOutput {
  name: string;
  url: string;
}

export interface ViewExportManifest {
  transactionTime: string;
  request: string;
  requiresAccessToken: boolean;
  output: ViewExportManifestOutput[];
  error: unknown[];
}
