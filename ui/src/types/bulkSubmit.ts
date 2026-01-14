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
 * Type definitions for bulk submit operations.
 *
 * @author John Grimes
 */

// Submission status values sent to the server.
export type BulkSubmitStatus = "in-progress" | "complete" | "aborted";

// Submitter identifier (system/value pair).
export interface SubmitterIdentifier {
  system: string;
  value: string;
}

// File request header for authentication with manifest files.
export interface FileRequestHeader {
  headerName: string;
  headerValue: string;
}

// Optional metadata for submissions.
export interface SubmissionMetadata {
  label?: string;
  description?: string;
}

// Request to create/update a submission via $bulk-submit.
export interface BulkSubmitRequest {
  submissionId: string;
  submitter: SubmitterIdentifier;
  submissionStatus: BulkSubmitStatus;
  manifestUrl?: string;
  fhirBaseUrl?: string;
  replacesManifestUrl?: string;
  fileRequestHeaders?: FileRequestHeader[];
  metadata?: SubmissionMetadata;
}

// Request to check submission status via $bulk-submit-status.
export interface BulkSubmitStatusRequest {
  submissionId: string;
  submitter: SubmitterIdentifier;
}

// Extension element in output file.
export interface OutputExtension {
  url: string;
  valueUrl?: string;
}

// Output file in status manifest.
export interface OutputFile {
  type: string;
  url: string;
  count?: number;
  extension?: OutputExtension[];
}

// Status manifest returned when submission completes.
export interface StatusManifest {
  transactionTime: string;
  request: string;
  requiresAccessToken: boolean;
  output: OutputFile[];
  error?: OutputFile[];
}
