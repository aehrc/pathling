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
