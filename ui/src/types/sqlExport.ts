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
 * Type definitions for the `$sql-export` UI flow.
 *
 * @author John Grimes
 */

import type { Parameters } from "fhir/r4";

/** Output formats the `$sql-export` operation can write. */
export type SqlExportFormat = "ndjson" | "csv" | "parquet";

/** The completion manifest, a FHIR Parameters resource. */
export type SqlExportManifest = Parameters;
