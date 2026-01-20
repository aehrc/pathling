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
