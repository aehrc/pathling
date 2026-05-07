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
 * Client for the SQL on FHIR `$sqlquery-run` operation and the search
 * endpoint that lists stored SQLQuery Library resources.
 *
 * @author John Grimes
 */

import { buildHeaders, buildUrl, checkResponse } from "./utils";

import type { AuthOptions } from "./rest";
import type {
  SqlQueryLibrary,
  SqlQueryOutputFormat,
  SqlQueryParameterType,
  SqlQueryRuntimeBindings,
} from "../types/sqlQuery";
import type { Bundle, Parameters, ParametersParameter } from "fhir/r4";

/**
 * Code system for the SQL on FHIR Library type vocabulary.
 */
export const SQL_QUERY_LIBRARY_TYPE_SYSTEM =
  "https://sql-on-fhir.org/ig/CodeSystem/LibraryTypesCodes";

/**
 * Token-search filter that scopes a Library search to the SQLQuery profile.
 */
export const SQL_QUERY_LIBRARY_TYPE_FILTER = `${SQL_QUERY_LIBRARY_TYPE_SYSTEM}|sql-query`;

/**
 * Profile URL applied to inline SQLQuery Library resources.
 */
export const SQL_QUERY_LIBRARY_PROFILE =
  "https://sql-on-fhir.org/ig/StructureDefinition/SQLQuery";

/**
 * Maps a {@link SqlQueryOutputFormat} to the matching MIME type, used as
 * the Accept header on `$sqlquery-run` requests.
 */
const FORMAT_TO_MIME: Record<SqlQueryOutputFormat, string> = {
  ndjson: "application/x-ndjson",
  csv: "text/csv",
  json: "application/json",
  parquet: "application/vnd.apache.parquet",
  fhir: "application/fhir+json",
};

/**
 * Common options for the `$sqlquery-run` request.
 */
interface SqlQueryRunCommonOptions extends AuthOptions {
  /** Output format requested via `_format`. */
  format?: SqlQueryOutputFormat;
  /** Maximum rows requested via `_limit`. */
  limit?: number;
  /** Whether to include a CSV header (`_header`). Only meaningful for csv. */
  header?: boolean;
  /** Runtime parameter values, keyed by declared parameter name. */
  bindings?: SqlQueryRuntimeBindings;
  /** Declared parameter types, keyed by name. */
  parameterTypes?: Record<string, SqlQueryParameterType>;
}

/**
 * Stored-mode options: the Library is referenced by ID.
 */
export interface SqlQueryRunStoredOptions extends SqlQueryRunCommonOptions {
  mode: "stored";
  /** ID of a stored SQLQuery Library. */
  libraryId: string;
}

/**
 * Inline-mode options: the Library is supplied with the request.
 */
export interface SqlQueryRunInlineOptions extends SqlQueryRunCommonOptions {
  mode: "inline";
  /** Library to send as the `queryResource` parameter. */
  library: SqlQueryLibrary;
}

/**
 * Discriminated request shape for the `$sqlquery-run` operation.
 */
export type SqlQueryRunOptions =
  | SqlQueryRunStoredOptions
  | SqlQueryRunInlineOptions;

/**
 * Executes the SQL on FHIR `$sqlquery-run` operation.
 *
 * @param baseUrl - The FHIR server base URL.
 * @param options - The request configuration.
 * @returns The HTTP `Response` with body still streaming.
 * @throws {UnauthorizedError} When the request receives a 401 response.
 * @throws {OperationOutcomeError} When the server returns a FHIR
 *   OperationOutcome on a non-2xx response.
 * @throws {Error} For other non-successful responses.
 *
 * @example
 * const response = await sqlQueryRun("https://example.com/fhir", {
 *   mode: "stored",
 *   libraryId: "lib-123",
 *   format: "csv",
 *   limit: 100,
 * });
 */
export async function sqlQueryRun(
  baseUrl: string,
  options: SqlQueryRunOptions,
): Promise<Response> {
  const url = buildUrl(baseUrl, "/$sqlquery-run");
  const accept =
    options.format !== undefined
      ? FORMAT_TO_MIME[options.format]
      : FORMAT_TO_MIME.ndjson;
  const headers = buildHeaders({
    accessToken: options.accessToken,
    contentType: "application/fhir+json",
    accept,
  });

  const parameter: ParametersParameter[] = [];

  if (options.mode === "stored") {
    parameter.push({
      name: "queryReference",
      valueReference: { reference: `Library/${options.libraryId}` },
    });
  } else {
    parameter.push({
      name: "queryResource",
      // The Library type is structurally compatible with the FHIR Library
      // resource carried inside Parameters. Cast through unknown to satisfy
      // the typed `resource` slot without dragging in a deep type.
      resource: options.library as unknown as ParametersParameter["resource"],
    });
  }

  if (options.format !== undefined) {
    parameter.push({ name: "_format", valueString: options.format });
  }
  if (options.limit !== undefined) {
    parameter.push({ name: "_limit", valueInteger: options.limit });
  }
  if (options.header !== undefined) {
    parameter.push({ name: "_header", valueBoolean: options.header });
  }

  const bindingParam = buildBindingParameter(
    options.bindings,
    options.parameterTypes,
  );
  if (bindingParam) {
    parameter.push(bindingParam);
  }

  const body: Parameters = { resourceType: "Parameters", parameter };

  const response = await fetch(url, {
    method: "POST",
    headers,
    body: JSON.stringify(body),
  });

  await checkResponse(response, "SQL query run");

  return response;
}

/**
 * Searches the FHIR server for stored SQLQuery Library resources.
 *
 * Uses a `type` token search scoped to the SQL on FHIR Library type code
 * system and the `sql-query` code, so unrelated Library resources are
 * excluded.
 *
 * @param baseUrl - The FHIR server base URL.
 * @param options - Optional auth configuration.
 * @returns A FHIR Bundle containing the matched Library resources.
 * @throws {UnauthorizedError} When the request receives a 401 response.
 * @throws {Error} For other non-successful responses.
 *
 * @example
 * const bundle = await listSqlQueryLibraries("https://example.com/fhir");
 */
export async function listSqlQueryLibraries(
  baseUrl: string,
  options: AuthOptions = {},
): Promise<Bundle> {
  const url = buildUrl(baseUrl, "/Library", {
    type: SQL_QUERY_LIBRARY_TYPE_FILTER,
  });
  const headers = buildHeaders({ accessToken: options.accessToken });

  const response = await fetch(url, { method: "GET", headers });
  await checkResponse(response, "Library search");
  return (await response.json()) as Bundle;
}

/**
 * Builds the nested `parameters` Parameters resource carrying runtime
 * bindings, or returns `undefined` if no non-empty bindings exist.
 * @param bindings
 * @param parameterTypes
 */
function buildBindingParameter(
  bindings: SqlQueryRuntimeBindings | undefined,
  parameterTypes: Record<string, SqlQueryParameterType> | undefined,
): ParametersParameter | undefined {
  if (!bindings) {
    return undefined;
  }

  const entries: ParametersParameter[] = [];
  for (const [name, rawValue] of Object.entries(bindings)) {
    if (rawValue === undefined || rawValue === null || rawValue === "") {
      continue;
    }
    const declaredType = parameterTypes?.[name] ?? "string";
    const part = bindingToPart(name, rawValue, declaredType);
    if (part) {
      entries.push(part);
    }
  }
  if (entries.length === 0) {
    return undefined;
  }

  return {
    name: "parameters",
    resource: {
      resourceType: "Parameters",
      parameter: entries,
    } as unknown as ParametersParameter["resource"],
  };
}

/**
 * Maps a single runtime binding to a typed Parameters part.
 *
 * Returns `undefined` if the value cannot be coerced (e.g. non-numeric
 * input for an integer parameter); the form layer is responsible for
 * blocking submission in that case.
 * @param name
 * @param rawValue
 * @param type
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
      if (Number.isNaN(parsed)) {
        return undefined;
      }
      return { name, valueInteger: parsed };
    }
    case "decimal": {
      const parsed = Number.parseFloat(rawValue);
      if (Number.isNaN(parsed)) {
        return undefined;
      }
      return { name, valueDecimal: parsed };
    }
    case "boolean":
      return { name, valueBoolean: rawValue === "true" };
    case "date":
      return { name, valueDate: rawValue };
    case "dateTime":
      return { name, valueDateTime: rawValue };
  }
}
