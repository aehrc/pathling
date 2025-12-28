/**
 * Type definitions for FHIR bulk export operations.
 *
 * @author John Grimes
 */

import type { Parameters } from "fhir/r4";

export type ExportLevel = "system" | "all-patients" | "patient" | "group";

export interface ExportRequest {
  level: ExportLevel;
  resourceTypes?: string[];
  since?: string;
  until?: string;
  elements?: string;
  patientId?: string;
  groupId?: string;
}

/**
 * Represents an output file entry extracted from the export manifest Parameters.
 */
export interface ExportManifestOutput {
  type: string;
  url: string;
  count?: number;
}

/**
 * Export manifest is a FHIR Parameters resource containing export results.
 */
export type ExportManifest = Parameters;

/**
 * Extracts output file entries from an export manifest Parameters resource.
 *
 * @param manifest - The export manifest Parameters resource.
 * @returns Array of output file entries with type, url, and optional count.
 */
export function getExportOutputFiles(
  manifest: ExportManifest,
): ExportManifestOutput[] {
  if (!manifest.parameter) {
    return [];
  }

  return manifest.parameter
    .filter((param) => param.name === "output" && param.part)
    .map((param) => {
      const parts = param.part!;
      const type = parts.find((p) => p.name === "type")?.valueCode ?? "";
      const url = parts.find((p) => p.name === "url")?.valueUri ?? "";
      const count = parts.find((p) => p.name === "count")?.valueInteger;
      return { type, url, count };
    });
}
