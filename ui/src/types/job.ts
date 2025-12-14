/**
 * Unified type definitions for async job operations (export and import).
 *
 * @author John Grimes
 */

import type { ExportRequest, ExportManifest } from "./export";
import type { ImportRequest, ImportManifest } from "./import";

export type JobType = "export" | "import";

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

export type Job = ExportJob | ImportJob;
