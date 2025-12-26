/**
 * Unified type definitions for async job operations (export and import).
 *
 * @author John Grimes
 */

import type {
  BulkSubmitRequest,
  StatusManifest,
  SubmitterIdentifier,
} from "./bulkSubmit";
import type { ExportRequest, ExportManifest } from "./export";
import type { ImportRequest, ImportManifest } from "./import";
import type { ImportPnpRequest } from "./importPnp";
import type { ViewExportRequest, ViewExportManifest } from "./viewExport";

export type JobType =
  | "export"
  | "import"
  | "import-pnp"
  | "bulk-submit"
  | "view-export";

export type JobStatus =
  | "pending"
  | "in_progress"
  | "completed"
  | "failed"
  | "cancelled";

interface BaseJob {
  id: string;
  type: JobType;
  pollUrl: string | null;
  status: JobStatus;
  progress: number | null;
  error: Error | null;
  createdAt: Date;
}

export interface ExportJob extends BaseJob {
  type: "export";
  request: ExportRequest;
  manifest: ExportManifest | null;
}

export interface ImportJob extends BaseJob {
  type: "import";
  request: ImportRequest;
  manifest: ImportManifest | null;
}

export interface ImportPnpJob extends BaseJob {
  type: "import-pnp";
  request: ImportPnpRequest;
  manifest: ImportManifest | null;
}

export interface BulkSubmitJob extends BaseJob {
  type: "bulk-submit";
  request: BulkSubmitRequest;
  submitter: SubmitterIdentifier;
  submissionId: string;
  manifest: StatusManifest | null;
}

export interface ViewExportJob extends BaseJob {
  type: "view-export";
  request: ViewExportRequest;
  manifest: ViewExportManifest | null;
}

export type Job =
  | ExportJob
  | ImportJob
  | ImportPnpJob
  | BulkSubmitJob
  | ViewExportJob;
