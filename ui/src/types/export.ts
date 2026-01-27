/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
  outputFormat?: string;
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
