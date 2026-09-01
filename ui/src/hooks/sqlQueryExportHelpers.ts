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
 * Pure helpers for the `$sqlquery-export` UI flow: building the kick-off request body and parsing
 * the completion manifest. Kept as plain functions so they can be unit tested without React.
 *
 * @author John Grimes
 */

import type { SqlQueryExportRequest } from "../types/sqlQuery";
import type { Parameters, ParametersParameter } from "fhir/r4";

/**
 * One downloadable output extracted from a `$sqlquery-export` completion manifest.
 */
export interface SqlQueryExportOutput {
  /** The output name. */
  name: string;
  /** The download URL (a `$result` URL from the manifest's `location`). */
  url: string;
}

/**
 * Builds the `$sqlquery-export` kick-off request body, reusing the same query source (stored or
 * inline) as the synchronous run.
 *
 * @param request - The export request describing the query source, format, and header flag.
 * @returns A FHIR Parameters resource for the kick-off request body.
 *
 * @example
 * const body = buildSqlQueryExportKickOffBody({
 *   mode: "stored",
 *   libraryId: "patient-bp-query",
 *   format: "ndjson",
 * });
 */
export function buildSqlQueryExportKickOffBody(
  request: SqlQueryExportRequest,
): Parameters {
  const queryPart: ParametersParameter =
    request.mode === "stored"
      ? {
          name: "query",
          part: [
            {
              name: "queryReference",
              valueReference: { reference: `Library/${request.libraryId}` },
            },
          ],
        }
      : {
          name: "query",
          part: [
            {
              name: "queryResource",
              resource:
                request.library as Parameters["parameter"] extends (infer T)[]
                  ? T extends { resource?: infer R }
                    ? R
                    : never
                  : never,
            },
          ],
        };

  const parameter: ParametersParameter[] = [
    queryPart,
    { name: "_format", valueCode: request.format },
  ];

  if (request.header !== undefined) {
    parameter.push({ name: "header", valueBoolean: request.header });
  }

  return { resourceType: "Parameters", parameter };
}

/**
 * Extracts the downloadable outputs from a `$sqlquery-export` completion manifest.
 *
 * Each SQL on FHIR `output` parameter carries one `name` part and one or more `location` parts (one
 * per file the query produced). This emits one entry per `location`, all sharing the output's name,
 * so a query that produced several files lists every file.
 *
 * @param manifest - The completion manifest Parameters resource (or null/undefined).
 * @returns The output entries, one per file, in manifest order.
 */
export function parseSqlQueryExportManifest(
  manifest: Parameters | null | undefined,
): SqlQueryExportOutput[] {
  if (!manifest?.parameter) {
    return [];
  }
  return manifest.parameter
    .filter((param) => param.name === "output" && param.part)
    .flatMap((param) => {
      const parts = param.part!;
      const name = parts.find((p) => p.name === "name")?.valueString ?? "";
      return parts
        .filter((p) => p.name === "location" && p.valueUri)
        .map((p) => ({ name, url: p.valueUri! }));
    });
}
