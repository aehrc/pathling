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
 * Type definitions for export options used across export and import forms.
 *
 * @author John Grimes
 */

/**
 * Values for configuring bulk export options.
 */
export interface ExportOptionsValues {
  /** Resource types to include in the export. */
  types: string[];
  /** Only resources updated after this time (ISO datetime-local format). */
  since: string;
  /** Only resources updated before this time (ISO datetime-local format). */
  until: string;
  /** Comma-separated list of element names to include. */
  elements: string;
  /** Output format MIME type for the export. */
  outputFormat: string;
  /** Comma-separated FHIR search queries to filter resources. */
  typeFilters: string;
  /** Comma-separated list of pre-defined associated data sets to include. */
  includeAssociatedData: string;
}

/**
 * Default empty values for export options.
 */
export const DEFAULT_EXPORT_OPTIONS: ExportOptionsValues = {
  types: [],
  since: "",
  until: "",
  elements: "",
  outputFormat: "",
  typeFilters: "",
  includeAssociatedData: "",
};

/**
 * Output format options for bulk export.
 */
export const OUTPUT_FORMATS: { value: string; label: string }[] = [
  { value: "application/fhir+ndjson", label: "NDJSON" },
  { value: "application/vnd.apache.parquet", label: "Parquet" },
];
