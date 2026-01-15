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

// FHIR REST operations.
export { useSearch } from "./useSearch";
export { useRead } from "./useRead";
export { useCreate } from "./useCreate";
export { useUpdate } from "./useUpdate";
export { useDelete } from "./useDelete";

// Async job operations.
export { useAsyncJob } from "./useAsyncJob";
export { useBulkExport } from "./useBulkExport";
export { useImport } from "./useImport";
export { useImportPnp } from "./useImportPnp";
export { useBulkSubmit } from "./useBulkSubmit";
/**
 * @deprecated Use useBulkSubmit with mode: 'monitor' instead.
 */
export { useBulkSubmit as useBulkSubmitMonitor } from "./useBulkSubmit";
export { useViewExport } from "./useViewExport";

// View operations.
export { useViewRun } from "./useViewRun";
export { useViewDefinitions } from "./useViewDefinitions";
export { useSaveViewDefinition } from "./useSaveViewDefinition";

// Other operations.
export { useClipboard } from "./useClipboard";
export { useDownloadFile } from "./useDownloadFile";
export { useFhirPathSearch } from "./useFhirPathSearch";
export { useServerCapabilities } from "./useServerCapabilities";

// Re-export types from the base hook.
export type { UseAsyncJobResult } from "./useAsyncJob";
