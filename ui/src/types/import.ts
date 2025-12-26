/**
 * Type definitions for FHIR import operations.
 *
 * @author John Grimes
 */

export type SaveMode = "overwrite" | "merge" | "append" | "ignore" | "error";

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
  saveMode: SaveMode;
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

// Save mode options for import forms.
export const SAVE_MODES: {
  value: SaveMode;
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
  {
    value: "append",
    label: "Append",
    description: "Add new resources without modifying existing",
  },
  {
    value: "ignore",
    label: "Ignore",
    description: "Skip if resources already exist",
  },
  {
    value: "error",
    label: "Error",
    description: "Fail if resources already exist",
  },
];
