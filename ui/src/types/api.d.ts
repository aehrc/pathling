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

import type { Bundle, Identifier, Parameters, Resource } from "fhir/r4";

/**
 * Pathling API Client Type Definitions
 *
 * Each distinct API call is thinly wrapped in a function with well defined
 * inputs that match the API request JSON, and returning a promise that matches
 * the response JSON. They will each also take an optional authentication token.
 */

// =============================================================================
// Common Types
// =============================================================================

export type ResourceType = Resource["resourceType"] | "ViewDefinition";

export interface AuthOptions {
  accessToken?: string;
}

// =============================================================================
// REST API
// =============================================================================

export interface SearchOptions extends AuthOptions {
  resourceType: ResourceType;
  count?: number;
  filters?: string[];
  /** Generic URL parameters for search queries. */
  params?: Record<string, string | string[]>;
}

export type SearchResult = Bundle;

export interface ReadOptions extends AuthOptions {
  resourceType: ResourceType;
  id: string;
}

export interface CreateOptions extends AuthOptions {
  resourceType: ResourceType;
  resource: Resource;
}

export interface UpdateOptions extends AuthOptions {
  resourceType: ResourceType;
  id: string;
  resource: Resource;
}

export interface DeleteOptions extends AuthOptions {
  resourceType: ResourceType;
  id: string;
}

export type SearchFn = (options: SearchOptions) => Promise<SearchResult>;
export type ReadFn = (options: ReadOptions) => Promise<Resource>;
export type CreateFn = (options: CreateOptions) => Promise<Resource>;
export type UpdateFn = (options: UpdateOptions) => Promise<Resource>;
export type DeleteFn = (options: DeleteOptions) => Promise<void>;

// =============================================================================
// Bulk Export
// =============================================================================

export interface BulkExportBaseOptions extends AuthOptions {
  types?: ResourceType[];
  since?: string;
  until?: string;
  elements?: string;
}

export type SystemExportKickOffOptions = BulkExportBaseOptions;

export type AllPatientsExportKickOffOptions = BulkExportBaseOptions;

export interface PatientExportKickOffOptions extends BulkExportBaseOptions {
  patientId: string;
}

export interface GroupExportKickOffOptions extends BulkExportBaseOptions {
  groupId: string;
}

export interface BulkExportKickOffResult {
  pollingUrl: string;
}

export interface BulkExportStatusOptions extends AuthOptions {
  pollingUrl: string;
}

export interface BulkExportStatusResult {
  status: "in-progress" | "complete";
  progress?: string;
  manifest?: ExportManifest;
}

/**
 * Export manifest is a FHIR Parameters resource containing export results.
 */
export type ExportManifest = Parameters;

export interface BulkExportDownloadOptions extends AuthOptions {
  fileUrl: string;
}

export type SystemExportKickOffFn = (
  options: SystemExportKickOffOptions,
) => Promise<BulkExportKickOffResult>;
export type AllPatientsExportKickOffFn = (
  options: AllPatientsExportKickOffOptions,
) => Promise<BulkExportKickOffResult>;
export type PatientExportKickOffFn = (
  options: PatientExportKickOffOptions,
) => Promise<BulkExportKickOffResult>;
export type GroupExportKickOffFn = (
  options: GroupExportKickOffOptions,
) => Promise<BulkExportKickOffResult>;
export type BulkExportStatusFn = (
  options: BulkExportStatusOptions,
) => Promise<BulkExportStatusResult>;
export type BulkExportDownloadFn = (
  options: BulkExportDownloadOptions,
) => Promise<ReadableStream>;

// =============================================================================
// Import
// =============================================================================

export type ImportFormat =
  | "application/fhir+ndjson"
  | "application/x-pathling-parquet"
  | "application/x-pathling-delta+parquet";

export type ImportMode = "overwrite" | "merge" | "append" | "ignore" | "error";

export interface ImportInput {
  type: ResourceType;
  url: string;
}

export interface ImportKickOffOptions extends AuthOptions {
  input: ImportInput[];
  inputFormat: ImportFormat;
  mode: ImportMode;
}

export interface ImportResult {
  pollingUrl: string;
}

export type ImportKickOffFn = (
  options: ImportKickOffOptions,
) => Promise<ImportResult>;

// =============================================================================
// Import Ping-and-Pull
// =============================================================================

export type ExportType = "dynamic" | "static";
export type SaveMode = "overwrite" | "merge" | "append" | "ignore" | "error";

export interface ImportPnpKickOffOptions extends AuthOptions {
  exportUrl: string;
  exportType?: ExportType;
  saveMode?: SaveMode;
  inputFormat?: ImportFormat;
}

export type ImportPnpKickOffFn = (
  options: ImportPnpKickOffOptions,
) => Promise<ImportResult>;

// =============================================================================
// Bulk Submit
// =============================================================================

export type SubmissionStatus = "in-progress" | "complete" | "aborted";

export interface BulkSubmitOptions extends AuthOptions {
  submitter: Identifier;
  submissionId: string;
  submissionStatus: SubmissionStatus;
  /** Manifest URL. Required unless submissionStatus is "aborted". */
  manifestUrl?: string;
  fhirBaseUrl?: string;
  replacesManifestUrl?: string;
  oauthMetadataUrl?: string;
  metadata?: Record<string, string>;
  fileRequestHeaders?: Record<string, string>;
}

export interface BulkSubmitResult {
  submissionId: string;
  status: string;
}

export interface BulkSubmitStatusOptions extends AuthOptions {
  submitter: Identifier;
  submissionId: string;
}

export interface BulkSubmitStatusResult {
  status: "in-progress" | "completed" | "completed-with-errors" | "aborted";
  progress?: string;
  jobId?: string;
  manifest?: BulkSubmitManifest;
}

export interface BulkSubmitManifest {
  transactionTime: string;
  request: string;
  requiresAccessToken: boolean;
  output: BulkSubmitManifestFile[];
  error: BulkSubmitManifestFile[];
}

export interface BulkSubmitManifestFile {
  type: string;
  url: string;
  manifestUrl?: string;
}

export interface BulkSubmitDownloadOptions extends AuthOptions {
  submissionId: string;
  fileName: string;
}

export type BulkSubmitFn = (
  options: BulkSubmitOptions,
) => Promise<BulkSubmitResult>;
export type BulkSubmitStatusFn = (
  options: BulkSubmitStatusOptions,
) => Promise<BulkSubmitStatusResult>;
export type BulkSubmitDownloadFn = (
  options: BulkSubmitDownloadOptions,
) => Promise<ReadableStream>;

// =============================================================================
// View Run (Synchronous)
// =============================================================================

export type ViewOutputFormat = "ndjson" | "csv";

export interface ViewDefinition {
  resourceType: "ViewDefinition";
  name?: string;
  resource: ResourceType;
  status: string;
  select: unknown[];
  where?: unknown[];
}

export interface ViewRunOptions extends AuthOptions {
  viewDefinition: ViewDefinition;
  format?: ViewOutputFormat;
  limit?: number;
  header?: boolean;
  patientIds?: string[];
  groupIds?: string[];
  since?: string;
}

export interface ViewRunStoredOptions extends AuthOptions {
  viewDefinitionId: string;
  format?: ViewOutputFormat;
  limit?: number;
  header?: boolean;
  patientIds?: string[];
  groupIds?: string[];
  since?: string;
}

export type ViewRunFn = (options: ViewRunOptions) => Promise<ReadableStream>;
export type ViewRunStoredFn = (
  options: ViewRunStoredOptions,
) => Promise<ReadableStream>;

// =============================================================================
// View Export
// =============================================================================

export type ViewExportFormat = "ndjson" | "csv" | "parquet";

export interface ViewExportInput {
  name?: string;
  viewDefinition: ViewDefinition;
}

export interface ViewExportKickOffOptions extends AuthOptions {
  views: ViewExportInput[];
  format?: ViewExportFormat;
  header?: boolean;
  patientIds?: string[];
  groupIds?: string[];
  since?: string;
}

export interface ViewExportResult {
  pollingUrl: string;
}

export interface ViewExportDownloadOptions extends AuthOptions {
  jobId: string;
  fileName: string;
}

export type ViewExportKickOffFn = (
  options: ViewExportKickOffOptions,
) => Promise<ViewExportResult>;
export type ViewExportDownloadFn = (
  options: ViewExportDownloadOptions,
) => Promise<ReadableStream>;

// =============================================================================
// Job (Shared Async Pattern)
// =============================================================================

export interface JobStatusOptions extends AuthOptions {
  pollingUrl: string;
}

export interface JobStatusResult {
  status: "in-progress" | "complete";
  progress?: string;
  result?: unknown;
}

export interface JobCancelOptions extends AuthOptions {
  pollingUrl: string;
}

export type JobStatusFn = (
  options: JobStatusOptions,
) => Promise<JobStatusResult>;
export type JobCancelFn = (options: JobCancelOptions) => Promise<void>;

// =============================================================================
// Async Job Executor
// =============================================================================

/**
 * Options for executing an async job with polling.
 */
export interface AsyncJobExecutorOptions<
  TKickOffResult,
  TStatusResult,
  TFinalResult,
> {
  kickOff: () => Promise<TKickOffResult>;
  getJobId: (kickOffResult: TKickOffResult) => string;
  checkStatus: (jobId: string) => Promise<TStatusResult>;
  isComplete: (statusResult: TStatusResult) => boolean;
  getResult: (statusResult: TStatusResult) => TFinalResult;
  cancel: (jobId: string) => Promise<void>;
  pollingInterval?: number;
  onProgress?: (statusResult: TStatusResult) => void;
}

/**
 * A handle for controlling a running async job.
 */
export interface AsyncJobHandle<TFinalResult> {
  result: Promise<TFinalResult>;
  cancel: () => Promise<void>;
}

/**
 * Executes an async job with polling, handling the kick-off, status checking,
 * and cancellation. Returns a handle with the result promise and a cancel function.
 */
export type AsyncJobExecutorFn = <TKickOffResult, TStatusResult, TFinalResult>(
  options: AsyncJobExecutorOptions<TKickOffResult, TStatusResult, TFinalResult>,
) => AsyncJobHandle<TFinalResult>;
