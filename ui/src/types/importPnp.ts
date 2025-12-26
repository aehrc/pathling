/**
 * Type definitions for FHIR ping and pull import operations.
 *
 * @author John Grimes
 */

import type { ImportFormat } from "./import";

// Export type for PnP import (only dynamic is currently supported).
export type ExportType = "dynamic";

// Save mode options for PnP import.
export type PnpSaveMode = "error" | "overwrite" | "append" | "ignore" | "merge";

export interface ImportPnpRequest {
  exportUrl: string;
  exportType: ExportType;
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
