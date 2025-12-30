/**
 * Type definitions for SQL on FHIR view export operations.
 *
 * @author John Grimes
 */

import type { Parameters } from "fhir/r4";

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

/**
 * View export manifest is a FHIR Parameters resource containing export results.
 */
export type ViewExportManifest = Parameters;

/**
 * Extracts output file entries from a view export manifest Parameters resource.
 *
 * @param manifest - The view export manifest Parameters resource.
 * @returns Array of output file entries with name and url.
 */
export function getViewExportOutputFiles(
  manifest: ViewExportManifest,
): ViewExportManifestOutput[] {
  if (!manifest.parameter) {
    return [];
  }

  return manifest.parameter
    .filter((param) => param.name === "output" && param.part)
    .map((param) => {
      const parts = param.part!;
      const name = parts.find((p) => p.name === "name")?.valueString ?? "";
      const url = parts.find((p) => p.name === "url")?.valueUri ?? "";
      return { name, url };
    });
}
