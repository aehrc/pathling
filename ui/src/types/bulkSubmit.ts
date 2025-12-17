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

// Output file in status manifest.
export interface OutputFile {
  type: string;
  url: string;
  count?: number;
}

// Status manifest returned when submission completes.
export interface StatusManifest {
  transactionTime: string;
  request: string;
  requiresAccessToken: boolean;
  output: OutputFile[];
  error?: OutputFile[];
}

// Submission status options for the form.
export const SUBMISSION_STATUSES: {
  value: BulkSubmitStatus;
  label: string;
  description: string;
}[] = [
  {
    value: "in-progress",
    label: "In progress",
    description: "Export is still running on the source server",
  },
  {
    value: "complete",
    label: "Complete",
    description: "Export finished, manifest URL is ready",
  },
];
