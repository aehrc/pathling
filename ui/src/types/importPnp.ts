/**
 * Type definitions for FHIR ping and pull import operations.
 *
 * @author John Grimes
 */

import type { ImportFormat } from "./import";

// Export type: dynamic (initiate new export) or static (fetch pre-generated manifest).
export type ExportType = "dynamic" | "static";

// Save mode options for PnP import.
export type PnpSaveMode = "error" | "overwrite" | "append" | "ignore" | "merge";

export interface ImportPnpRequest {
  exportUrl: string;
  exportType: ExportType;
  inputSource: string;
  saveMode: PnpSaveMode;
  inputFormat: ImportFormat;
}

// Export type options for the form.
export const EXPORT_TYPES: {
  value: ExportType;
  label: string;
  description: string;
}[] = [
  {
    value: "dynamic",
    label: "Dynamic",
    description: "Initiate a new export from the server",
  },
  {
    value: "static",
    label: "Static",
    description: "Fetch a pre-generated export manifest",
  },
];

// Save mode options for PnP import.
export const PNP_SAVE_MODES: {
  value: PnpSaveMode;
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
