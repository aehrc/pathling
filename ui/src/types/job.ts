/**
 * Unified type definitions for async job operations (export and import).
 *
 * @author John Grimes
 */

import type { BulkSubmitRequest, StatusManifest, SubmitterIdentifier } from "./bulkSubmit";
import type { ExportRequest, ExportManifest } from "./export";
import type { ImportRequest, ImportManifest } from "./import";
import type { ImportPnpRequest } from "./importPnp";

export type JobType = "export" | "import" | "import-pnp" | "bulk-submit";

export type JobStatus =
  | "pending"
  | "in_progress"
  | "completed"
  | "failed"
  | "cancelled";

interface BaseJob {
  id: string;
  type: JobType;
  pollUrl: string;
  status: JobStatus;
  progress: number | null;
  error: string | null;
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

export type Job = ExportJob | ImportJob | ImportPnpJob | BulkSubmitJob;
