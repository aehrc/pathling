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

// Async job execution.
export { executeAsyncJob } from "./asyncJob";
export type { AsyncJobExecutorOptions, AsyncJobHandle } from "./asyncJob";

// Bulk export operations.
export {
  systemExportKickOff,
  allPatientsExportKickOff,
  patientExportKickOff,
  groupExportKickOff,
  bulkExportStatus,
  bulkExportDownload,
  buildExportParams,
} from "./bulkExport";

// Bulk submit operations.
export { bulkSubmit, bulkSubmitStatus, bulkSubmitDownload } from "./bulkSubmit";

// Import operations.
export { importKickOff, importPnpKickOff } from "./import";

// Job operations.
export { jobStatus, jobCancel } from "./job";

// Job list operation.
export { listJobs, parseJobsResponse } from "./jobs";
export type { JobSummary, JobSummaryStatus, ListJobsOptions } from "./jobs";

// FHIR REST operations.
export { search, read, create, update, deleteResource } from "./rest";
export type { AuthOptions, ResourceType } from "./rest";

// Utility functions.
export {
  buildUrl,
  resolveUrl,
  parseProgressHeader,
  extractJobIdFromUrl,
} from "./utils";

// SQL on FHIR data operations.
export {
  sqlRun,
  sqlRunStored,
  sqlExportKickOff,
  sqlExportDownload,
  parseSqlExportManifest,
} from "./sqlOnFhirOperations";
export type {
  SqlExportDownloadOptions,
  SqlExportEntry,
  SqlExportFormat,
  SqlExportKickOffOptions,
  SqlExportOutput,
  SqlExportResult,
  SqlRunFormat,
  SqlRunOptions,
  SqlRunStoredOptions,
  SubjectFilters,
  SubjectSource,
} from "./sqlOnFhirOperations";

// ViewDefinition resources.
export type { ViewDefinition } from "./view";

// SQL on FHIR Library resources.
export {
  listSqlQueryLibraries,
  listStoredLibraries,
  SQL_QUERY_LIBRARY_TYPE_SYSTEM,
  SQL_QUERY_LIBRARY_TYPE_FILTER,
  SQL_VIEW_LIBRARY_TYPE_FILTER,
  SQL_QUERY_LIBRARY_PROFILE,
} from "./sqlQuery";
export type { SqlOnFhirLibraryTypeCode } from "./sqlQuery";
