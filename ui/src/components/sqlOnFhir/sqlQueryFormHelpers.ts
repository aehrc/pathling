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
 * Pure helpers for the SQL query form: assembling inline Library
 * resources, validating runtime bindings, and shaping derived state.
 *
 * @author John Grimes
 */

import {
  SQL_QUERY_LIBRARY_PROFILE,
  SQL_QUERY_LIBRARY_TYPE_SYSTEM,
} from "../../api";
import { decodeSql, encodeSql } from "../../utils";

import type {
  SqlQueryLibrary,
  SqlQueryParameterDeclaration,
  SqlQueryParameterType,
  SqlQueryRelatedArtifact,
  SqlQueryRequest,
  SqlQueryRuntimeBindings,
} from "../../types/sqlQuery";

/**
 * Extension URL for the plain-text SQL companion of `Library.content[0]`.
 */
const SQL_TEXT_EXTENSION =
  "https://sql-on-fhir.org/ig/StructureDefinition/sql-text";

/**
 * Inputs to {@link buildInlineSqlQueryLibrary}.
 */
export interface BuildInlineLibraryInput {
  /** Optional title; falls back to undefined when blank. */
  title?: string;
  /** Optional canonical URL. */
  url?: string;
  /** Plain-text SQL the user wrote. */
  sql: string;
  /** View rows (related artefacts) configured by the user. */
  tables: SqlQueryRelatedArtifact[];
  /** Declared runtime parameters. */
  parameters: SqlQueryParameterDeclaration[];
}

/**
 * Builds an inline SQLQuery Library resource from the form's authored
 * state.
 *
 * The Library carries the SQL both Base64-encoded (per the profile) and
 * as plain text via the `sql-text` extension, mirroring the canonical
 * examples in the operation definition.
 *
 * @param input - The authored state to assemble.
 * @returns The Library resource ready to send as `queryResource` or
 *   POST to `/Library`.
 *
 * @example
 * buildInlineSqlQueryLibrary({
 *   sql: "SELECT 1",
 *   tables: [],
 *   parameters: [],
 * });
 */
export function buildInlineSqlQueryLibrary(
  input: BuildInlineLibraryInput,
): SqlQueryLibrary {
  const trimmedTitle = input.title?.trim();
  const trimmedUrl = input.url?.trim();
  const library: SqlQueryLibrary = {
    resourceType: "Library",
    status: "active",
    meta: { profile: [SQL_QUERY_LIBRARY_PROFILE] },
    type: {
      coding: [{ system: SQL_QUERY_LIBRARY_TYPE_SYSTEM, code: "sql-query" }],
    },
    content: [
      {
        contentType: "application/sql",
        data: encodeSql(input.sql),
        extension: [{ url: SQL_TEXT_EXTENSION, valueString: input.sql }],
      },
    ],
  };

  if (trimmedTitle) {
    library.title = trimmedTitle;
    library.name = trimmedTitle.replace(/\s+/g, "-").toLowerCase();
  }
  if (trimmedUrl) {
    library.url = trimmedUrl;
  }

  if (input.tables.length > 0) {
    library.relatedArtifact = input.tables.map((table) => ({
      type: "depends-on" as const,
      label: table.label,
      // The source is referenced by its canonical URL, matched against the
      // referenced resource's `url` on the server.
      resource: table.referenceUrl,
    }));
  }

  if (input.parameters.length > 0) {
    library.parameter = input.parameters.map((param) => ({
      name: param.name,
      use: "in" as const,
      type: param.type,
    }));
  }

  return library;
}

/**
 * Recovers the plain SQL text from a `SqlQueryRequest` for display.
 *
 * For stored requests the text comes from the resolved `sql` field, which
 * the form copies from the selected Library. For inline requests it is read
 * from the `sql-text` extension on `Library.content[0]`, falling back to
 * decoding the Base64 `data`. Returns the empty string when no SQL can be
 * recovered.
 *
 * @param request - The request whose SQL should be displayed.
 * @returns The plain SQL text, or an empty string if it cannot be recovered.
 */
export function extractRequestSql(request: SqlQueryRequest): string {
  if (request.mode === "stored") {
    return request.sql ?? "";
  }
  const content = request.library.content?.[0];
  if (!content) {
    return "";
  }
  const ext = content.extension?.find((e) => e.url.endsWith("/sql-text"));
  if (ext?.valueString) {
    return ext.valueString;
  }
  return content.data ? decodeSql(content.data) : "";
}

/**
 * Returns true when the inline form has the minimum data required to
 * execute a query.
 *
 * The server requires a Library with non-empty SQL and at least one
 * related artefact; the form blocks the Execute button otherwise.
 *
 * @param input - The authored state to validate.
 * @returns Whether Execute should be enabled.
 */
export function canExecuteInlineForm(input: BuildInlineLibraryInput): boolean {
  if (input.sql.trim().length === 0) {
    return false;
  }
  if (input.tables.length === 0) {
    return false;
  }
  if (input.tables.some((t) => t.label.trim() === "" || !t.referenceUrl)) {
    return false;
  }
  return true;
}

/**
 * Returns true when the inline form has the minimum data required to
 * save the assembled Library to the server.
 *
 * Save requires what Execute requires, plus a non-blank title (so the
 * Library is referrable in the picker afterwards).
 *
 * @param input - The authored state to validate.
 * @returns Whether Save should be enabled.
 */
export function canSaveInlineForm(input: BuildInlineLibraryInput): boolean {
  if (!canExecuteInlineForm(input)) {
    return false;
  }
  if (!input.title || input.title.trim() === "") {
    return false;
  }
  return true;
}

/**
 * Returns true when each runtime binding parses cleanly to its declared
 * FHIR primitive type.
 *
 * Empty values are allowed and pass validation; the API client will omit
 * them from the request.
 *
 * @param parameters - The declared parameters in the active Library.
 * @param bindings - The runtime values entered by the user.
 * @returns Whether the bindings are submittable.
 */
export function areRuntimeBindingsValid(
  parameters: Array<{ name: string; type: SqlQueryParameterType }>,
  bindings: SqlQueryRuntimeBindings,
): boolean {
  for (const param of parameters) {
    const raw = bindings[param.name];
    if (raw === undefined || raw === "") {
      continue;
    }
    if (!isRuntimeValueValid(raw, param.type)) {
      return false;
    }
  }
  return true;
}

/**
 * Validates a single runtime value against its declared FHIR primitive
 * type. Used for both per-field guidance and overall form validity.
 *
 * @param raw - The string captured from the input.
 * @param type - The declared parameter type.
 * @returns True if the value is parseable as the declared type.
 */
export function isRuntimeValueValid(
  raw: string,
  type: SqlQueryParameterType,
): boolean {
  switch (type) {
    case "string":
    case "code":
      return true;
    case "integer":
      return /^-?\d+$/.test(raw);
    case "decimal":
      return /^-?\d+(\.\d+)?$/.test(raw);
    case "boolean":
      return raw === "true" || raw === "false";
    case "date":
      return /^\d{4}-\d{2}-\d{2}$/.test(raw);
    case "dateTime":
      return /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}(:\d{2}(\.\d+)?)?(Z|[+-]\d{2}:\d{2})?$/.test(
        raw,
      );
  }
}

/**
 * Builds a `parameterTypes` map from a list of declared parameters.
 *
 * @param parameters - The declared parameter list.
 * @returns A name-keyed map of parameter types.
 */
export function buildParameterTypes(
  parameters: Array<{ name: string; type: SqlQueryParameterType }>,
): Record<string, SqlQueryParameterType> {
  const map: Record<string, SqlQueryParameterType> = {};
  for (const param of parameters) {
    if (param.name) {
      map[param.name] = param.type;
    }
  }
  return map;
}
