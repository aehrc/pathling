/**
 * Type definitions for FHIR import operations.
 *
 * @author John Grimes
 */

export type ImportMode = "overwrite" | "merge";

// MIME types for import formats.
export type ImportFormat =
  | "application/fhir+ndjson"
  | "application/x-pathling-parquet"
  | "application/x-pathling-delta+parquet";

export interface ImportInput {
  type: string;
  url: string;
}

export interface ImportRequest {
  inputFormat: ImportFormat;
  input: ImportInput[];
  mode: ImportMode;
}

export interface ImportManifestOutput {
  inputUrl: string;
}

export interface ImportManifest {
  transactionTime: string;
  request: string;
  output: ImportManifestOutput[];
}

// Import format options for the form.
export const IMPORT_FORMATS: { value: ImportFormat; label: string }[] = [
  { value: "application/fhir+ndjson", label: "NDJSON" },
  { value: "application/x-pathling-parquet", label: "Parquet" },
  { value: "application/x-pathling-delta+parquet", label: "Delta" },
];

// Import mode options for the form.
export const IMPORT_MODES: {
  value: ImportMode;
  label: string;
  description: string;
}[] = [
  {
    value: "overwrite",
    label: "Overwrite",
    description: "Replace all existing data for each resource type",
  },
  {
    value: "merge",
    label: "Merge",
    description: "Update existing resources and add new ones",
  },
];
