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
 * Pure helpers for the SQL query hooks.
 *
 * Splitting these out keeps the hooks themselves as thin wrappers and lets
 * us unit-test the request/response logic without mounting React.
 *
 * @author John Grimes
 */

import {
  decodeSql,
  flattenFhirParameters,
  parseCsvResponse,
  parseNdjsonResponse,
} from "../utils";

import type {
  SourceOption,
  SqlQueryBinaryResult,
  SqlQueryLibrary,
  SqlQueryLibrarySummary,
  SqlQueryOutputFormat,
  SqlQueryParameterType,
  SqlQueryRelatedArtifact,
  SqlQueryResult,
  SqlQueryTabularResult,
} from "../types/sqlQuery";
import type { Bundle, Library } from "fhir/r4";

/**
 * Allowed FHIR primitive types for declared SQL parameters in the UI.
 */
const SUPPORTED_PARAMETER_TYPES = new Set<SqlQueryParameterType>([
  "string",
  "code",
  "integer",
  "decimal",
  "boolean",
  "date",
  "dateTime",
]);

/**
 * Maps a FHIR Bundle of Library resources into the picker-friendly summary
 * shape consumed by the SQL query form.
 *
 * Resources missing the SQL on FHIR `content` slot or a usable ID are
 * skipped so the picker only ever offers Libraries that the form can
 * actually render.
 *
 * @param bundle - The FHIR search Bundle to map.
 * @returns The library summaries in Bundle order.
 *
 * @example
 * mapLibraryBundle({ resourceType: "Bundle", entry: [{ resource: lib }] });
 */
export function mapLibraryBundle(bundle: Bundle): SqlQueryLibrarySummary[] {
  const summaries: SqlQueryLibrarySummary[] = [];
  for (const entry of bundle.entry ?? []) {
    const resource = entry.resource;
    if (!resource || resource.resourceType !== "Library") {
      continue;
    }
    const summary = libraryToSummary(resource);
    if (summary) {
      summaries.push(summary);
    }
  }
  return summaries;
}

/**
 * Converts a FHIR Library resource into a {@link SqlQueryLibrarySummary},
 * decoding the embedded SQL and surfacing parameter declarations and
 * related artefacts in their form-friendly shape.
 *
 * Returns `undefined` if the resource is missing the data the form
 * requires (an ID or any SQL content).
 *
 * @param library - The Library resource to summarise.
 * @returns The picker-friendly summary, or `undefined` when the resource
 *   is unusable.
 */
export function libraryToSummary(
  library: Library,
): SqlQueryLibrarySummary | undefined {
  const id = library.id;
  if (!id) {
    return undefined;
  }
  const sqlContent = (library.content ?? []).find(
    (c) => c.contentType === "application/sql",
  );
  if (!sqlContent || !sqlContent.data) {
    return undefined;
  }

  const sql = decodeSql(sqlContent.data);
  const title = library.title || library.name || id;
  const relatedArtifacts = (library.relatedArtifact ?? [])
    .filter((ra) => ra.type === "depends-on")
    .map((ra) => ({
      label: ra.label ?? "",
      reference: ra.resource ?? "",
    }));
  const parameters: Array<{ name: string; type: SqlQueryParameterType }> = [];
  for (const declared of library.parameter ?? []) {
    if (!declared.name || !declared.type) {
      continue;
    }
    if (declared.use !== undefined && declared.use !== "in") {
      continue;
    }
    const declaredType = declared.type as SqlQueryParameterType;
    parameters.push({
      name: declared.name,
      type: SUPPORTED_PARAMETER_TYPES.has(declaredType)
        ? declaredType
        : "string",
    });
  }

  return {
    id,
    title,
    url: library.url,
    sql,
    relatedArtifacts,
    parameters,
    resource: library as unknown as SqlQueryLibrary,
  };
}

/**
 * Repopulates inline-form view rows from a stored query's dependency
 * references. Each `relatedArtifact` is a canonical URL, carried verbatim as
 * the row's `referenceUrl`; the picker later matches it back to a known source
 * (see {@link findSourceByUrl}) or surfaces it as an unmatched URL.
 *
 * @param relatedArtifacts - The stored query's decoded dependency references.
 * @returns The view rows, in reference order, keyed by deterministic row ids.
 *
 * @example
 * storedReferencesToViewRows([
 *   { label: "patients", reference: "https://example.org/Patients" },
 * ]);
 * // [{ rowId: "stored-row-0", label: "patients",
 * //    referenceUrl: "https://example.org/Patients" }]
 */
export function storedReferencesToViewRows(
  relatedArtifacts: Array<{ label: string; reference: string }>,
): SqlQueryRelatedArtifact[] {
  return relatedArtifacts.map((artifact, index) => ({
    rowId: `stored-row-${index}`,
    label: artifact.label,
    referenceUrl: artifact.reference,
  }));
}

/**
 * Finds the source whose canonical URL matches the given reference URL.
 *
 * Used to repopulate the picker when editing a stored query: a matched source
 * is shown selected by name, while an unmatched URL (no source carries it) is
 * surfaced verbatim with a "source not found" note.
 *
 * @param sources - The known selectable sources.
 * @param url - The canonical URL to match.
 * @returns The matching source, or `undefined` when none carries that URL.
 *
 * @example
 * findSourceByUrl(
 *   [{ id: "vd1", name: "Patients", url: "https://example.org/Patients" }],
 *   "https://example.org/Patients",
 * );
 * // { id: "vd1", name: "Patients", url: "https://example.org/Patients" }
 */
export function findSourceByUrl(
  sources: SourceOption[],
  url: string,
): SourceOption | undefined {
  if (!url) {
    return undefined;
  }
  return sources.find((source) => source.url === url);
}

/**
 * Reads the body of a `$sqlquery-run` response and assembles it into a
 * format-aware result.
 *
 * Tabular formats (`csv`, `ndjson`, `json`, `fhir`) are parsed into a
 * `{columns, rows}` shape and the original body is preserved as a Blob so
 * the UI can offer a verbatim download. Parquet is not parsed; the body
 * is returned as a Blob only.
 *
 * @param response - The fetch Response from `sqlQueryRun`.
 * @param format - The output format requested with the request.
 * @returns The parsed and/or downloadable result.
 *
 * @example
 * const result = await readSqlQueryResponse(response, "csv");
 */
export async function readSqlQueryResponse(
  response: Response,
  format: SqlQueryOutputFormat,
): Promise<SqlQueryResult> {
  if (format === "parquet") {
    const blob = await response.blob();
    const binary: SqlQueryBinaryResult = {
      kind: "binary",
      format: "parquet",
      blob,
    };
    return binary;
  }

  const text = await response.text();
  const tabular = parseTabularBody(text, format);
  const blob = new Blob([text], { type: tabularContentType(format) });
  const result: SqlQueryTabularResult = {
    kind: "tabular",
    format,
    columns: tabular.columns,
    rows: tabular.rows,
    rawBody: blob,
  };
  return result;
}

/**
 * Parses a tabular body into `{columns, rows}` based on the requested
 * format. Empty bodies produce zero rows.
 *
 * @param body - The response body text.
 * @param format - The format the server was asked to produce.
 * @returns The parsed `{columns, rows}` view.
 */
export function parseTabularBody(
  body: string,
  format: Exclude<SqlQueryOutputFormat, "parquet">,
): { columns: string[]; rows: Record<string, unknown>[] } {
  if (format === "csv") {
    const rows = parseCsvResponse(body);
    return { columns: extractColumns(rows), rows };
  }
  if (format === "ndjson") {
    const rows = parseNdjsonResponse(body);
    return { columns: extractColumns(rows), rows };
  }
  if (format === "json") {
    const trimmed = body.trim();
    if (trimmed.length === 0) {
      return { columns: [], rows: [] };
    }
    const parsed = JSON.parse(trimmed) as Record<string, unknown>[];
    const rows = Array.isArray(parsed) ? parsed : [];
    return { columns: extractColumns(rows), rows };
  }
  // FHIR format: parse a Parameters resource and flatten one row per
  // `row` parameter.
  const trimmed = body.trim();
  if (trimmed.length === 0) {
    return { columns: [], rows: [] };
  }
  const parameters = JSON.parse(trimmed);
  const flattened = flattenFhirParameters(parameters);
  return flattened;
}

/**
 * Extracts column names from the union of keys seen across the rows.
 *
 * Columns appear in first-seen order so the rendered table follows the
 * server's natural column ordering when present.
 *
 * @param rows - The parsed result rows.
 * @returns The column names in first-seen order.
 */
function extractColumns(rows: Record<string, unknown>[]): string[] {
  const columns: string[] = [];
  const seen = new Set<string>();
  for (const row of rows) {
    for (const key of Object.keys(row)) {
      if (!seen.has(key)) {
        seen.add(key);
        columns.push(key);
      }
    }
  }
  return columns;
}

/**
 * Returns the MIME type to attach to the downloadable Blob produced from
 * a tabular response body.
 *
 * @param format - The output format the body was produced with.
 * @returns The MIME type to attach to the Blob.
 */
function tabularContentType(
  format: Exclude<SqlQueryOutputFormat, "parquet">,
): string {
  switch (format) {
    case "csv":
      return "text/csv";
    case "ndjson":
      return "application/x-ndjson";
    case "json":
      return "application/json";
    case "fhir":
      return "application/fhir+json";
  }
}
