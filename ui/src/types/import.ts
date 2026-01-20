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
 * Type definitions for FHIR import operations.
 *
 * @author John Grimes
 */

import type { ImportPnpRequest } from "./importPnp";

export type SaveMode = "overwrite" | "merge" | "append" | "ignore" | "error";

// MIME types for import formats.
export type ImportFormat =
  | "application/fhir+ndjson"
  | "application/vnd.apache.parquet";

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
  { value: "application/vnd.apache.parquet", label: "Parquet" },
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

/**
 * Discriminated type for import jobs to determine which hook to use.
 */
export type ImportJobType = "standard" | "pnp";

/**
 * Represents an active or completed import job for tracking in the UI.
 */
export interface ImportJob {
  id: string;
  type: ImportJobType;
  request: ImportRequest | ImportPnpRequest;
  createdAt: Date;
}
