/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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
 *
 * Author: John Grimes
 */

import type { Bundle, Resource } from "fhir/r4";
import type { UseQueryResult, UseMutationResult } from "@tanstack/react-query";

// ============================================================================
// Common Types
// ============================================================================

/**
 * Status of an async job operation.
 */
export type AsyncJobStatus =
  | "idle"
  | "pending"
  | "in-progress"
  | "complete"
  | "error"
  | "cancelled";

/**
 * Result of an async job hook.
 */
export interface AsyncJobResult<TResult> {
  /** Current status of the job. */
  status: AsyncJobStatus;
  /** Progress percentage (0-100) when available. */
  progress?: number;
  /** The final result when status is "complete". */
  result?: TResult;
  /** Error message when status is "error". */
  error?: string;
  /** Function to cancel the job. */
  cancel: () => Promise<void>;
}

/**
 * Options for starting an async job.
 */
export interface AsyncJobOptions {
  /** Callback when progress updates. */
  onProgress?: (progress: number) => void;
  /** Callback when job completes. */
  onComplete?: () => void;
  /** Callback when job fails. */
  onError?: (error: string) => void;
}

// ============================================================================
// FHIR REST Hooks
// ============================================================================

/**
 * Options for useSearch hook.
 */
export interface UseSearchOptions {
  /** The FHIR resource type to search. */
  resourceType: string;
  /** Optional search parameters. */
  params?: Record<string, string | string[]>;
  /** Whether to enable the query. */
  enabled?: boolean;
}

/**
 * Result of useSearch hook.
 */
export type UseSearchResult = UseQueryResult<Bundle, Error>;

/**
 * Options for useRead hook.
 */
export interface UseReadOptions {
  /** The FHIR resource type. */
  resourceType: string;
  /** The resource ID. */
  id: string;
  /** Whether to enable the query. */
  enabled?: boolean;
}

/**
 * Result of useRead hook.
 */
export type UseReadResult = UseQueryResult<Resource, Error>;

/**
 * Options for useCreate hook.
 */
export interface UseCreateOptions {
  /** Callback on successful creation. */
  onSuccess?: (resource: Resource) => void;
  /** Callback on error. */
  onError?: (error: Error) => void;
}

/**
 * Variables for useCreate mutation.
 */
export interface UseCreateVariables {
  /** The resource to create. */
  resource: Resource;
}

/**
 * Result of useCreate hook.
 */
export type UseCreateResult = UseMutationResult<Resource, Error, UseCreateVariables>;

/**
 * Options for useUpdate hook.
 */
export interface UseUpdateOptions {
  /** Callback on successful update. */
  onSuccess?: (resource: Resource) => void;
  /** Callback on error. */
  onError?: (error: Error) => void;
}

/**
 * Variables for useUpdate mutation.
 */
export interface UseUpdateVariables {
  /** The resource to update (must include id). */
  resource: Resource;
}

/**
 * Result of useUpdate hook.
 */
export type UseUpdateResult = UseMutationResult<Resource, Error, UseUpdateVariables>;

/**
 * Options for useDelete hook.
 */
export interface UseDeleteOptions {
  /** Callback on successful deletion. */
  onSuccess?: () => void;
  /** Callback on error. */
  onError?: (error: Error) => void;
}

/**
 * Variables for useDelete mutation.
 */
export interface UseDeleteVariables {
  /** The FHIR resource type. */
  resourceType: string;
  /** The resource ID. */
  id: string;
}

/**
 * Result of useDelete hook.
 */
export type UseDeleteResult = UseMutationResult<void, Error, UseDeleteVariables>;

// ============================================================================
// Bulk Export Hooks
// ============================================================================

/**
 * Export type for bulk export operations.
 */
export type BulkExportType = "system" | "all-patients" | "patient" | "group";

/**
 * Options for useBulkExport hook.
 */
export interface UseBulkExportOptions extends AsyncJobOptions {
  /** Type of export operation. */
  type: BulkExportType;
  /** Patient ID (required for "patient" type). */
  patientId?: string;
  /** Group ID (required for "group" type). */
  groupId?: string;
  /** Resource types to export (optional filter). */
  resourceTypes?: string[];
  /** Export since date (optional). */
  since?: string;
  /** Output format. */
  outputFormat?: string;
}

/**
 * Manifest entry for a single exported file.
 */
export interface ExportManifestEntry {
  type: string;
  url: string;
  count?: number;
}

/**
 * Complete export manifest.
 */
export interface ExportManifest {
  transactionTime: string;
  request: string;
  requiresAccessToken: boolean;
  output: ExportManifestEntry[];
  error?: ExportManifestEntry[];
}

/**
 * Result of useBulkExport hook.
 */
export interface UseBulkExportResult extends AsyncJobResult<ExportManifest> {
  /** Function to start the export. */
  start: () => void;
  /** Function to download a file from the manifest. */
  download: (fileName: string) => Promise<ReadableStream>;
}

// ============================================================================
// Import Hooks
// ============================================================================

/**
 * Options for useImport hook.
 */
export interface UseImportOptions extends AsyncJobOptions {
  /** Source URLs to import from. */
  sources: string[];
  /** Resource types to import (optional filter). */
  resourceTypes?: string[];
}

/**
 * Result of useImport hook.
 */
export interface UseImportResult extends AsyncJobResult<void> {
  /** Function to start the import. */
  start: () => void;
}

/**
 * Options for useImportPnp hook (passthrough import).
 */
export interface UseImportPnpOptions extends AsyncJobOptions {
  /** Source URLs to import from. */
  sources: string[];
  /** Resource types to import (optional filter). */
  resourceTypes?: string[];
}

/**
 * Result of useImportPnp hook.
 */
export interface UseImportPnpResult extends AsyncJobResult<void> {
  /** Function to start the passthrough import. */
  start: () => void;
}

// ============================================================================
// Bulk Submit Hooks
// ============================================================================

/**
 * Submitter identifier for bulk submit.
 */
export interface SubmitterIdentifier {
  system: string;
  value: string;
}

/**
 * Options for useBulkSubmit hook.
 */
export interface UseBulkSubmitOptions extends AsyncJobOptions {
  /** Unique submission ID. */
  submissionId: string;
  /** Submitter identifier. */
  submitter: SubmitterIdentifier;
  /** URL of the manifest file. */
  manifestUrl: string;
  /** Optional FHIR base URL for the source. */
  fhirBaseUrl?: string;
  /** URL of manifest being replaced (for updates). */
  replacesManifestUrl?: string;
  /** OAuth metadata URL for source authentication. */
  oauthMetadataUrl?: string;
  /** Additional metadata key-value pairs. */
  metadata?: Record<string, string>;
  /** Headers to include when fetching files. */
  fileRequestHeaders?: Record<string, string>;
}

/**
 * Bulk submit manifest entry.
 */
export interface BulkSubmitManifestEntry {
  type: string;
  url: string;
  count?: number;
}

/**
 * Complete bulk submit manifest.
 */
export interface BulkSubmitManifest {
  transactionTime: string;
  request: string;
  output: BulkSubmitManifestEntry[];
  error?: BulkSubmitManifestEntry[];
}

/**
 * Result of useBulkSubmit hook.
 */
export interface UseBulkSubmitResult extends AsyncJobResult<BulkSubmitManifest> {
  /** Function to start the submission. */
  start: () => void;
  /** Function to download a file from the manifest. */
  download: (fileName: string) => Promise<ReadableStream>;
}

// ============================================================================
// View Definition Hooks
// ============================================================================

/**
 * A ViewDefinition resource.
 */
export interface ViewDefinition {
  resourceType: "ViewDefinition";
  id?: string;
  name?: string;
  resource: string;
  status: string;
  select: Array<{
    column?: Array<{ path: string; name: string }>;
    forEach?: string;
    forEachOrNull?: string;
    select?: unknown[];
  }>;
  where?: Array<{ path: string }>;
}

/**
 * Output format for synchronous view run operations.
 */
export type ViewRunOutputFormat = "ndjson" | "csv";

/**
 * Output format for asynchronous view export operations.
 */
export type ViewExportOutputFormat = "ndjson" | "csv" | "parquet";

/**
 * Options for useViewRun hook.
 */
export interface UseViewRunOptions {
  /** The ViewDefinition to run (inline). */
  viewDefinition?: ViewDefinition;
  /** ID of a stored ViewDefinition to run. */
  viewDefinitionId?: string;
  /** Output format. */
  format?: ViewRunOutputFormat;
  /** Maximum rows to return. */
  limit?: number;
  /** Whether to include header row (CSV only). */
  header?: boolean;
  /** Whether to enable the query. */
  enabled?: boolean;
}

/**
 * Result of useViewRun hook.
 */
export type UseViewRunResult = UseQueryResult<ReadableStream, Error>;

/**
 * Options for useViewExport hook.
 */
export interface UseViewExportOptions extends AsyncJobOptions {
  /** Views to export. */
  views: Array<{
    viewDefinition: ViewDefinition;
    name?: string;
  }>;
  /** Output format. */
  format?: ViewExportOutputFormat;
  /** Whether to include header row (CSV only). */
  header?: boolean;
}

/**
 * View export manifest entry.
 */
export interface ViewExportManifestEntry {
  name: string;
  url: string;
}

/**
 * Complete view export manifest.
 */
export interface ViewExportManifest {
  transactionTime: string;
  output: ViewExportManifestEntry[];
}

/**
 * Result of useViewExport hook.
 */
export interface UseViewExportResult extends AsyncJobResult<ViewExportManifest> {
  /** Function to start the export. */
  start: () => void;
  /** Function to download a file from the manifest. */
  download: (fileName: string) => Promise<ReadableStream>;
}

// ============================================================================
// Specialised Query Hooks
// ============================================================================

/**
 * Options for useFhirPathSearch hook.
 * This is a specialised search that uses FHIRPath filter expressions.
 */
export interface UseFhirPathSearchOptions {
  /** The FHIR resource type to search. */
  resourceType: string;
  /** FHIRPath filter expressions. */
  filters: string[];
  /** Whether to enable the query. */
  enabled?: boolean;
}

/**
 * Result of useFhirPathSearch hook.
 */
export interface UseFhirPathSearchResult {
  /** The matching resources. */
  resources: Resource[];
  /** Total count from server (may be undefined). */
  total?: number;
  /** The raw bundle response. */
  bundle?: Bundle;
  /** Loading state. */
  isLoading: boolean;
  /** Error state. */
  isError: boolean;
  /** Error object if failed. */
  error: Error | null;
  /** Refetch function. */
  refetch: () => void;
}

/**
 * Summary of a stored ViewDefinition.
 */
export interface ViewDefinitionSummary {
  id: string;
  name: string;
  json: string;
}

/**
 * Options for useViewDefinitions hook.
 */
export interface UseViewDefinitionsOptions {
  /** Whether to enable the query. */
  enabled?: boolean;
}

/**
 * Result of useViewDefinitions hook.
 */
export type UseViewDefinitionsResult = UseQueryResult<ViewDefinitionSummary[], Error>;

/**
 * Options for useSaveViewDefinition hook.
 */
export interface UseSaveViewDefinitionOptions {
  /** Callback on successful save. */
  onSuccess?: (result: { id: string; name: string }) => void;
  /** Callback on error. */
  onError?: (error: Error) => void;
}

/**
 * Result of useSaveViewDefinition hook.
 */
export type UseSaveViewDefinitionResult = UseMutationResult<
  { id: string; name: string },
  Error,
  string
>;

// ============================================================================
// Server Capabilities Hook
// ============================================================================

/**
 * Resource capability from server.
 */
export interface ResourceCapability {
  type: string;
  operations: string[];
}

/**
 * Operation capability from server.
 */
export interface OperationCapability {
  name: string;
  definition?: string;
}

/**
 * Server capabilities.
 */
export interface ServerCapabilities {
  authRequired: boolean;
  serverName?: string;
  serverVersion?: string;
  fhirVersion?: string;
  publisher?: string;
  description?: string;
  resources?: ResourceCapability[];
  operations?: OperationCapability[];
}

/**
 * Result of useServerCapabilities hook.
 */
export type UseServerCapabilitiesResult = UseQueryResult<ServerCapabilities, Error>;

// ============================================================================
// Hook Function Types
// ============================================================================

/**
 * Search for FHIR resources.
 */
export type UseSearchFn = (options: UseSearchOptions) => UseSearchResult;

/**
 * Read a single FHIR resource by type and ID.
 */
export type UseReadFn = (options: UseReadOptions) => UseReadResult;

/**
 * Create a new FHIR resource.
 */
export type UseCreateFn = (options?: UseCreateOptions) => UseCreateResult;

/**
 * Update an existing FHIR resource.
 */
export type UseUpdateFn = (options?: UseUpdateOptions) => UseUpdateResult;

/**
 * Delete a FHIR resource.
 */
export type UseDeleteFn = (options?: UseDeleteOptions) => UseDeleteResult;

/**
 * Execute a bulk export operation with polling.
 */
export type UseBulkExportFn = (options: UseBulkExportOptions) => UseBulkExportResult;

/**
 * Execute a standard import operation with polling.
 */
export type UseImportFn = (options: UseImportOptions) => UseImportResult;

/**
 * Execute a passthrough (PnP) import operation with polling.
 */
export type UseImportPnpFn = (options: UseImportPnpOptions) => UseImportPnpResult;

/**
 * Execute a bulk submit operation with polling.
 */
export type UseBulkSubmitFn = (options: UseBulkSubmitOptions) => UseBulkSubmitResult;

/**
 * Run a ViewDefinition and return results as a stream.
 */
export type UseViewRunFn = (options: UseViewRunOptions) => UseViewRunResult;

/**
 * Execute a view export operation with polling.
 */
export type UseViewExportFn = (options: UseViewExportOptions) => UseViewExportResult;

/**
 * Search for resources using FHIRPath filter expressions.
 */
export type UseFhirPathSearchFn = (
  options: UseFhirPathSearchOptions,
) => UseFhirPathSearchResult;

/**
 * Fetch available ViewDefinitions from the server.
 */
export type UseViewDefinitionsFn = (
  options?: UseViewDefinitionsOptions,
) => UseViewDefinitionsResult;

/**
 * Save a ViewDefinition to the server.
 */
export type UseSaveViewDefinitionFn = (
  options?: UseSaveViewDefinitionOptions,
) => UseSaveViewDefinitionResult;

/**
 * Fetch server capabilities from the CapabilityStatement.
 */
export type UseServerCapabilitiesFn = (
  fhirBaseUrl: string | null | undefined,
) => UseServerCapabilitiesResult;
