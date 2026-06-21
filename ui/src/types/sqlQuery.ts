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
 * Type definitions for the SQL query (`$sqlquery-run`) UI flow.
 *
 * @author John Grimes
 */

/**
 * Output formats accepted by the `$sqlquery-run` operation.
 */
export type SqlQueryOutputFormat =
  | "ndjson"
  | "csv"
  | "json"
  | "parquet"
  | "fhir";

/**
 * FHIR primitive types supported as runtime parameter values in the UI.
 *
 * The server's request parser handles a wider range of types, but the UI
 * limits authoring to these primitives (see the design doc for rationale).
 */
export type SqlQueryParameterType =
  | "string"
  | "code"
  | "integer"
  | "decimal"
  | "boolean"
  | "date"
  | "dateTime";

/**
 * A `relatedArtifact` entry on a SQLQuery Library, expressed in form-state
 * terms.
 */
export interface SqlQueryRelatedArtifact {
  /** Stable identifier for use as a React `key`. */
  rowId: string;
  /** Table name referenced by the SQL. */
  label: string;
  /** ID of the referenced ViewDefinition (becomes `ViewDefinition/<id>`). */
  viewDefinitionId: string;
}

/**
 * A declared parameter on a SQLQuery Library, expressed in form-state terms.
 */
export interface SqlQueryParameterDeclaration {
  /** Stable identifier for use as a React `key`. */
  rowId: string;
  /** Parameter name, used both in the SQL and to bind a runtime value. */
  name: string;
  /** Declared FHIR primitive type. */
  type: SqlQueryParameterType;
  /** Optional default value supplied as a string; coerced on submit. */
  defaultValue?: string;
}

/**
 * Runtime parameter bindings supplied at execution time.
 *
 * Keyed by declared parameter name. A missing or empty entry causes the
 * parameter to be omitted from the request's nested Parameters resource.
 */
export type SqlQueryRuntimeBindings = Record<string, string>;

/**
 * Decoded summary of a stored SQLQuery Library, surfaced to the form.
 */
export interface SqlQueryLibrarySummary {
  /** Server-assigned ID. */
  id: string;
  /** Human-readable title (`title` or `name`, falling back to the ID). */
  title: string;
  /** Decoded SQL text from `Library.content[0].data`. */
  sql: string;
  /** Related-artifact entries with label and ViewDefinition reference. */
  relatedArtifacts: Array<{ label: string; reference: string }>;
  /** Declared parameters extracted from `Library.parameter`. */
  parameters: Array<{ name: string; type: SqlQueryParameterType }>;
  /** The original Library resource, kept for execution by reference. */
  resource: SqlQueryLibrary;
}

/**
 * Library resource conforming to the SQLQuery profile, in the shape used by
 * the API client and form state. This is a structurally-narrower view of
 * `fhir/r4`'s Library; only the slots used by the UI are typed.
 */
export interface SqlQueryLibrary {
  resourceType: "Library";
  id?: string;
  url?: string;
  name?: string;
  title?: string;
  status: "active" | "draft" | "retired" | "unknown";
  meta?: { profile?: string[] };
  type: {
    coding: Array<{
      system: string;
      code: "sql-query";
    }>;
  };
  content: Array<{
    contentType: "application/sql";
    data: string;
    extension?: Array<{ url: string; valueString?: string }>;
  }>;
  relatedArtifact?: Array<{
    type: "depends-on";
    label?: string;
    resource?: string;
  }>;
  parameter?: Array<{
    name: string;
    use: "in" | "out";
    type: string;
    extension?: Array<{ url: string; [key: string]: unknown }>;
  }>;
}

/**
 * Common execution options that apply regardless of how the Library is
 * supplied.
 */
export interface SqlQueryExecutionOptions {
  /** Output format (defaults to ndjson on the server when omitted). */
  format?: SqlQueryOutputFormat;
  /** Maximum number of rows to return. */
  limit?: number;
  /** Whether to include a header row when format is `csv`. */
  header?: boolean;
  /** Runtime parameter values, keyed by declared parameter name. */
  bindings?: SqlQueryRuntimeBindings;
  /**
   * Declared parameter types, keyed by name. Used to pick the correct
   * `value[x]` slot for each binding.
   */
  parameterTypes?: Record<string, SqlQueryParameterType>;
}

/**
 * A `$sqlquery-run` request, distinguished by how the Library is supplied.
 */
export type SqlQueryRequest =
  | (SqlQueryExecutionOptions & {
      mode: "stored";
      /** ID of a stored Library conforming to the SQLQuery profile. */
      libraryId: string;
    })
  | (SqlQueryExecutionOptions & {
      mode: "inline";
      /** Inline Library to send as the `queryResource` parameter. */
      library: SqlQueryLibrary;
    });

/**
 * Successful tabular result from a `$sqlquery-run` execution.
 */
export interface SqlQueryTabularResult {
  /** Discriminator. */
  kind: "tabular";
  /** Output format that produced this result. */
  format: Exclude<SqlQueryOutputFormat, "parquet">;
  /** Column names in display order. */
  columns: string[];
  /** Parsed result rows. */
  rows: Record<string, unknown>[];
  /** Raw response body, retained for download. */
  rawBody: Blob;
}

/**
 * Successful binary (parquet) result from a `$sqlquery-run` execution.
 */
export interface SqlQueryBinaryResult {
  /** Discriminator. */
  kind: "binary";
  /** Output format that produced this result. */
  format: "parquet";
  /** Response body as a Blob, ready for download. */
  blob: Blob;
}

/**
 * Either form of successful result.
 */
export type SqlQueryResult = SqlQueryTabularResult | SqlQueryBinaryResult;

/**
 * Active or completed SQL query job tracked in the page's job list.
 */
export interface SqlQueryJob {
  /** Stable identifier for the in-page card. */
  id: string;
  /** Whether the Library was supplied by reference or inline. */
  mode: "stored" | "inline";
  /** Snapshot of the request used at execution time. */
  request: SqlQueryRequest;
  /** Original SQL text, displayed in error states and used as a download seed. */
  sql: string;
  /** Wall-clock submission time. */
  createdAt: Date;
}

/**
 * Result of saving an inline SQLQuery Library to the server.
 */
export interface SaveSqlQueryLibraryResult {
  id: string;
  title: string;
}

/**
 * Output formats accepted by the asynchronous `$sqlquery-export` operation.
 *
 * Narrower than {@link SqlQueryOutputFormat}: the export operation supports only the file-friendly
 * formats, mirroring `$viewdefinition-export`.
 */
export type SqlQueryExportFormat = "ndjson" | "csv" | "parquet";

/**
 * A `$sqlquery-export` request, reusing the same query source (stored or inline) as the
 * synchronous run, plus the chosen export format and CSV header flag.
 */
export type SqlQueryExportRequest =
  | {
      mode: "stored";
      /** ID of a stored Library conforming to the SQLQuery profile. */
      libraryId: string;
      format: SqlQueryExportFormat;
      header?: boolean;
    }
  | {
      mode: "inline";
      /** Inline Library to send as the `query.queryResource` part. */
      library: SqlQueryLibrary;
      format: SqlQueryExportFormat;
      header?: boolean;
    };

/**
 * The `$sqlquery-export` completion manifest, a FHIR Parameters resource describing the export
 * outputs. Shares the SQL on FHIR manifest shape with the view export manifest.
 */
export type SqlQueryExportManifest = import("fhir/r4").Parameters;
