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
 * Client for the search endpoint that lists the stored SQLQuery and SQLView
 * Library resources the UI can run and export.
 *
 * @author John Grimes
 */

import { buildHeaders, buildUrl, checkResponse } from "./utils";

import type { AuthOptions } from "./rest";
import type { Bundle } from "fhir/r4";

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
 * Token-search filter that scopes a Library search to the SQLView profile.
 *
 * Shares the code system with {@link SQL_QUERY_LIBRARY_TYPE_FILTER}; only the
 * type code differs.
 */
export const SQL_VIEW_LIBRARY_TYPE_FILTER = `${SQL_QUERY_LIBRARY_TYPE_SYSTEM}|sql-view`;

/**
 * SQL on FHIR Library type codes that the UI can list and run.
 */
export type SqlOnFhirLibraryTypeCode = "sql-query" | "sql-view";

/**
 * Maps each listable Library type code to its token-search filter.
 */
const LIBRARY_TYPE_FILTERS: Record<SqlOnFhirLibraryTypeCode, string> = {
  "sql-query": SQL_QUERY_LIBRARY_TYPE_FILTER,
  "sql-view": SQL_VIEW_LIBRARY_TYPE_FILTER,
};

/**
 * Profile URL applied to inline SQLQuery Library resources.
 */
export const SQL_QUERY_LIBRARY_PROFILE =
  "https://sql-on-fhir.org/ig/StructureDefinition/SQLQuery";

/**
 * Searches the FHIR server for stored SQL on FHIR Library resources of a
 * given type.
 *
 * Uses a `type` token search scoped to the SQL on FHIR Library type code
 * system and the requested code (`sql-query` or `sql-view`), so unrelated
 * Library resources are excluded. SQLQueries and SQLViews are both Library
 * resources distinguished only by this code, so a single function lists
 * either kind.
 *
 * @param baseUrl - The FHIR server base URL.
 * @param options - The type code to list, plus optional auth configuration.
 * @returns A FHIR Bundle containing the matched Library resources.
 * @throws {UnauthorizedError} When the request receives a 401 response.
 * @throws {Error} For other non-successful responses.
 *
 * @example
 * const bundle = await listStoredLibraries("https://example.com/fhir", {
 *   typeCode: "sql-view",
 * });
 */
export async function listStoredLibraries(
  baseUrl: string,
  options: { typeCode: SqlOnFhirLibraryTypeCode } & AuthOptions,
): Promise<Bundle> {
  const url = buildUrl(baseUrl, "/Library", {
    type: LIBRARY_TYPE_FILTERS[options.typeCode],
  });
  const headers = buildHeaders({ accessToken: options.accessToken });

  const response = await fetch(url, { method: "GET", headers });
  await checkResponse(response, "Library search");
  return (await response.json()) as Bundle;
}

/**
 * Searches the FHIR server for stored SQLQuery Library resources.
 *
 * A thin wrapper over {@link listStoredLibraries} scoped to the `sql-query`
 * type code, retained for the existing callers.
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
  return listStoredLibraries(baseUrl, {
    typeCode: "sql-query",
    accessToken: options.accessToken,
  });
}
