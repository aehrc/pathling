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
} from "./bulkExport";

// Bulk submit operations.
export { bulkSubmit, bulkSubmitStatus, bulkSubmitDownload } from "./bulkSubmit";

// Import operations.
export { importKickOff, importPnpKickOff } from "./import";

// Job operations.
export { jobStatus, jobCancel } from "./job";

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

// ViewDefinition operations.
export {
  viewRun,
  viewRunStored,
  viewExportKickOff,
  viewExportDownload,
} from "./view";
export type { ViewDefinition } from "./view";
