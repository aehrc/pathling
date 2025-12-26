/**
 * Type definitions for FHIR ping and pull import operations.
 *
 * @author John Grimes
 */

import type { ImportFormat, SaveMode } from "./import";

// Export type for PnP import (only dynamic is currently supported).
export type ExportType = "dynamic";

export interface ImportPnpRequest {
  exportUrl: string;
  exportType: ExportType;
  saveMode: SaveMode;
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

