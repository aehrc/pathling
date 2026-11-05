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

import { decodeSql, parseNdjsonResponse } from "../utils";

import type { SubjectSource } from "../api";
import type {
  SourceOption,
  SqlQueryLibrary,
  SqlQueryLibrarySummary,
  SqlQueryParameterType,
  SqlQueryRelatedArtifact,
  SqlQueryRequest,
  SqlQueryResult,
  SqlQueryRuntimeBindings,
} from "../types/sqlQuery";
import type { Bundle, Library, Parameters, ParametersParameter } from "fhir/r4";

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
 * Reads the NDJSON body of a `$sql-run` response into a `{columns, rows}`
 * result. Empty bodies produce zero rows.
 *
 * @param response - The fetch Response from `sqlRun` or `sqlRunStored`.
 * @returns The parsed result.
 *
 * @example
 * const { columns, rows } = await readSqlQueryResponse(response);
 */
export async function readSqlQueryResponse(
  response: Response,
): Promise<SqlQueryResult> {
  const rows = parseNdjsonResponse(await response.text());
  return { columns: extractColumns(rows), rows };
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
 * Builds the nested `parameters` Parameters resource carrying runtime
 * bindings, or returns `undefined` when there is nothing to send.
 *
 * A binding whose value cannot be coerced to its declared type is omitted;
 * the form layer is responsible for blocking submission in that case.
 *
 * @param bindings - Runtime values keyed by declared parameter name.
 * @param parameterTypes - Declared FHIR primitive types keyed by name.
 * @returns The bindings resource, or `undefined` when no binding has a value.
 *
 * @example
 * buildBindingsResource({ family: "Smith" }, { family: "string" });
 * // { resourceType: "Parameters", parameter: [{ name: "family", valueString: "Smith" }] }
 */
export function buildBindingsResource(
  bindings: SqlQueryRuntimeBindings | undefined,
  parameterTypes: Record<string, SqlQueryParameterType> | undefined,
): Parameters | undefined {
  if (!bindings) {
    return undefined;
  }

  const entries: ParametersParameter[] = [];
  for (const [name, rawValue] of Object.entries(bindings)) {
    if (rawValue === undefined || rawValue === null || rawValue === "") {
      continue;
    }
    const part = bindingToPart(
      name,
      rawValue,
      parameterTypes?.[name] ?? "string",
    );
    if (part) {
      entries.push(part);
    }
  }
  if (entries.length === 0) {
    return undefined;
  }
  return { resourceType: "Parameters", parameter: entries };
}

/**
 * Maps a single runtime binding to a typed Parameters part.
 *
 * @param name - The parameter name.
 * @param rawValue - The string captured from the form input.
 * @param type - The declared FHIR primitive type.
 * @returns The part with the matching `value[x]` slot, or `undefined` when the
 *   value cannot be parsed.
 */
function bindingToPart(
  name: string,
  rawValue: string,
  type: SqlQueryParameterType,
): ParametersParameter | undefined {
  switch (type) {
    case "string":
      return { name, valueString: rawValue };
    case "code":
      return { name, valueCode: rawValue };
    case "integer": {
      const parsed = Number.parseInt(rawValue, 10);
      return Number.isNaN(parsed) ? undefined : { name, valueInteger: parsed };
    }
    case "decimal": {
      const parsed = Number.parseFloat(rawValue);
      return Number.isNaN(parsed) ? undefined : { name, valueDecimal: parsed };
    }
    case "boolean":
      return { name, valueBoolean: rawValue === "true" };
    case "date":
      return { name, valueDate: rawValue };
    case "dateTime":
      return { name, valueDateTime: rawValue };
  }
}

/**
 * Derives the wire subject form from a SQL query request: a stored Library is
 * named by a typed reference, an inline one is sent whole.
 *
 * @param request - The form-level SQL query request.
 * @returns The subject source to send.
 *
 * @example
 * toSubjectSource({ mode: "stored", libraryId: "bp" });
 * // { kind: "reference", reference: "Library/bp" }
 */
export function toSubjectSource(request: SqlQueryRequest): SubjectSource {
  return request.mode === "stored"
    ? { kind: "reference", reference: `Library/${request.libraryId}` }
    : { kind: "resource", resource: request.library };
}
