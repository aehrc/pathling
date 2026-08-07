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
 * Client for the two SQL on FHIR data operations, `$sql-run` and
 * `$sql-export`. Both act on a polymorphic subject - a ViewDefinition, a
 * SQLQuery or a SQLView - so one module serves every flow in the UI.
 *
 * @author John Grimes
 */

import { buildHeaders, buildUrl, checkResponse } from "./utils";

import type { AuthOptions } from "./rest";
import type { Parameters, ParametersParameter } from "fhir/r4";

/** Output formats the synchronous `$sql-run` operation can return. */
export type SqlRunFormat = "ndjson" | "csv" | "json" | "parquet" | "fhir";

/** Output formats the asynchronous `$sql-export` operation can write. */
export type SqlExportFormat = "ndjson" | "csv" | "parquet";

/** Maps a run format to the media type sent as the `Accept` header. */
const RUN_FORMAT_MIME: Record<SqlRunFormat, string> = {
  ndjson: "application/x-ndjson",
  csv: "text/csv",
  json: "application/json",
  parquet: "application/vnd.apache.parquet",
  fhir: "application/fhir+json",
};

/**
 * How a subject is named on the wire. Exactly one form is sent per subject,
 * matching the operation's exactly-one rule.
 */
export type SubjectSource =
  | { kind: "resource"; resource: object }
  | { kind: "reference"; reference: string }
  | { kind: "canonical"; canonical: string };

/** Filters that narrow the data every subject reads. */
export interface SubjectFilters {
  /** Patient ids, sent as one `patient` parameter each. */
  patientIds?: string[];
  /** Group ids, sent as one `group` parameter each. */
  groupIds?: string[];
  /** Instant sent as `_since`. */
  since?: string;
}

/** Options for a synchronous `$sql-run` request sent as POST. */
export interface SqlRunOptions extends AuthOptions, SubjectFilters {
  /** The subject to run. */
  subject: SubjectSource;
  /** Output format requested via `_format`. */
  format?: SqlRunFormat;
  /** Maximum rows requested via `_limit`. */
  limit?: number;
  /** Whether CSV output carries a header row. */
  header?: boolean;
  /** Runtime bindings, for a SQL subject only. */
  parameters?: Parameters;
}

/** Options for a synchronous `$sql-run` request sent as GET. */
export interface SqlRunStoredOptions extends AuthOptions, SubjectFilters {
  /** A typed relative reference, e.g. `ViewDefinition/abc` or `Library/xyz`. */
  reference: string;
  /** Output format requested via `_format`. */
  format?: SqlRunFormat;
  /** Maximum rows requested via `_limit`. */
  limit?: number;
  /** Whether CSV output carries a header row. */
  header?: boolean;
}

/** One `subject` repetition of an `$sql-export` job. */
export interface SqlExportEntry {
  /** Output name; the server derives one when omitted. */
  name?: string;
  /** How the subject is named. */
  subject: SubjectSource;
  /** Runtime bindings, for a SQL subject only. */
  parameters?: Parameters;
}

/** Options for kicking off an `$sql-export` job. */
export interface SqlExportKickOffOptions extends AuthOptions, SubjectFilters {
  /** The subjects to export, one output each. */
  subjects: SqlExportEntry[];
  /** Output format requested via `_format`. */
  format?: SqlExportFormat;
  /** Whether CSV output carries a header row. */
  header?: boolean;
  /** Tracking identifier echoed in the manifest. */
  clientTrackingId?: string;
}

/** Result of an `$sql-export` kick-off. */
export interface SqlExportResult {
  /** The status URL taken from the `Content-Location` header. */
  pollingUrl: string;
}

/** Options for downloading one output file of a completed export. */
export interface SqlExportDownloadOptions extends AuthOptions {
  /** The fully-qualified URL from the manifest's `output.location`. */
  location: string;
}

/** One downloadable file named by an export manifest. */
export interface SqlExportOutput {
  /** The output name, shared by every file of one subject. */
  name: string;
  /** The download URL. */
  url: string;
}

/**
 * Runs a subject synchronously via `POST [base]/$sql-run`.
 *
 * @param baseUrl - The FHIR server base URL.
 * @param options - The subject, output settings and filters.
 * @returns The HTTP response, with its body still streaming.
 * @throws {UnauthorizedError} When the request receives a 401 response.
 * @throws {OperationOutcomeError} When the server returns an OperationOutcome.
 * @throws {Error} For other non-successful responses.
 *
 * @example
 * const response = await sqlRun("https://example.com/fhir", {
 *   subject: { kind: "reference", reference: "Library/bp-query" },
 *   format: "csv",
 * });
 */
export async function sqlRun(
  baseUrl: string,
  options: SqlRunOptions,
): Promise<Response> {
  const url = buildUrl(baseUrl, "/$sql-run");
  const headers = buildHeaders({
    accessToken: options.accessToken,
    contentType: "application/fhir+json",
    accept: RUN_FORMAT_MIME[options.format ?? "ndjson"],
  });

  const parameter: ParametersParameter[] = [subjectPart(options.subject)];

  if (options.parameters) {
    parameter.push({
      name: "parameters",
      resource: options.parameters as ParametersParameter["resource"],
    });
  }
  if (options.format !== undefined) {
    parameter.push({ name: "_format", valueString: options.format });
  }
  if (options.limit !== undefined) {
    parameter.push({ name: "_limit", valueInteger: options.limit });
  }
  if (options.header !== undefined) {
    parameter.push({ name: "header", valueBoolean: options.header });
  }
  parameter.push(...filterParts(options));

  const body: Parameters = { resourceType: "Parameters", parameter };

  const response = await fetch(url, {
    method: "POST",
    headers,
    body: JSON.stringify(body),
  });

  await checkResponse(response, "SQL run");
  return response;
}

/**
 * Runs a stored subject synchronously via `GET [base]/$sql-run`.
 *
 * Every parameter a GET can carry is a primitive, so this form is available
 * only for a subject already stored on the server.
 *
 * @param baseUrl - The FHIR server base URL.
 * @param options - The subject reference, output settings and filters.
 * @returns The HTTP response, with its body still streaming.
 * @throws {UnauthorizedError} When the request receives a 401 response.
 * @throws {OperationOutcomeError} When the server returns an OperationOutcome.
 * @throws {Error} For other non-successful responses.
 *
 * @example
 * const response = await sqlRunStored("https://example.com/fhir", {
 *   reference: "ViewDefinition/patient-demographics",
 *   format: "csv",
 *   limit: 20,
 * });
 */
export async function sqlRunStored(
  baseUrl: string,
  options: SqlRunStoredOptions,
): Promise<Response> {
  const params = new URLSearchParams();
  params.append("subjectReference", options.reference);
  if (options.format !== undefined) {
    params.append("_format", options.format);
  }
  if (options.limit !== undefined) {
    params.append("_limit", String(options.limit));
  }
  if (options.header !== undefined) {
    params.append("header", String(options.header));
  }
  for (const patientId of options.patientIds ?? []) {
    params.append("patient", `Patient/${patientId}`);
  }
  for (const groupId of options.groupIds ?? []) {
    params.append("group", `Group/${groupId}`);
  }
  if (options.since !== undefined) {
    params.append("_since", options.since);
  }

  const url = buildUrl(baseUrl, "/$sql-run", params);
  const headers = buildHeaders({
    accessToken: options.accessToken,
    accept: RUN_FORMAT_MIME[options.format ?? "ndjson"],
  });

  const response = await fetch(url, { method: "GET", headers });

  await checkResponse(response, "SQL run");
  return response;
}

/**
 * Kicks off an asynchronous `$sql-export` job.
 *
 * @param baseUrl - The FHIR server base URL.
 * @param options - The subjects, output settings and job-wide filters.
 * @returns The status URL for tracking the job.
 * @throws {UnauthorizedError} When the request receives a 401 response.
 * @throws {OperationOutcomeError} When the server rejects the kick-off.
 * @throws {Error} For other non-successful responses, or a missing
 *   `Content-Location` header.
 *
 * @example
 * const { pollingUrl } = await sqlExportKickOff("https://example.com/fhir", {
 *   subjects: [
 *     { name: "demographics", subject: { kind: "canonical", canonical: "https://example.org/v" } },
 *   ],
 *   format: "parquet",
 * });
 */
export async function sqlExportKickOff(
  baseUrl: string,
  options: SqlExportKickOffOptions,
): Promise<SqlExportResult> {
  const url = buildUrl(baseUrl, "/$sql-export");
  const headers = buildHeaders({
    accessToken: options.accessToken,
    contentType: "application/fhir+json",
    prefer: "respond-async",
  });

  const parameter: ParametersParameter[] = options.subjects.map((entry) => {
    const part: ParametersParameter[] = [];
    if (entry.name) {
      part.push({ name: "name", valueString: entry.name });
    }
    part.push(subjectPart(entry.subject));
    if (entry.parameters) {
      part.push({
        name: "parameters",
        resource: entry.parameters as ParametersParameter["resource"],
      });
    }
    return { name: "subject", part };
  });

  if (options.format !== undefined) {
    parameter.push({ name: "_format", valueCode: options.format });
  }
  if (options.header !== undefined) {
    parameter.push({ name: "header", valueBoolean: options.header });
  }
  if (options.clientTrackingId !== undefined) {
    parameter.push({
      name: "clientTrackingId",
      valueString: options.clientTrackingId,
    });
  }
  parameter.push(...filterParts(options));

  const body: Parameters = { resourceType: "Parameters", parameter };

  const response = await fetch(url, {
    method: "POST",
    headers,
    body: JSON.stringify(body),
  });

  await checkResponse(response, "SQL export kick-off");

  if (response.status !== 202) {
    const errorBody = await response.text();
    throw new Error(
      `SQL export kick-off failed: ${response.status} - ${errorBody}`,
    );
  }

  const contentLocation = response.headers.get("Content-Location");
  if (!contentLocation) {
    throw new Error("SQL export kick-off failed: No Content-Location header");
  }

  return { pollingUrl: contentLocation };
}

/**
 * Downloads one output file of a completed export.
 *
 * @param options - The manifest location URL, plus optional auth.
 * @returns A stream of the file contents.
 * @throws {UnauthorizedError} When the request receives a 401 response.
 * @throws {Error} For other non-successful responses, or a missing body.
 *
 * @example
 * const stream = await sqlExportDownload({
 *   location: "https://example.com/fhir/$result?job=abc&file=people.ndjson",
 * });
 */
export async function sqlExportDownload(
  options: SqlExportDownloadOptions,
): Promise<ReadableStream> {
  const headers = buildHeaders({ accessToken: options.accessToken });
  const response = await fetch(options.location, { method: "GET", headers });

  await checkResponse(response, "SQL export download");

  if (!response.body) {
    throw new Error("SQL export download failed: No response body");
  }
  return response.body;
}

/**
 * Extracts the downloadable files from an export completion manifest.
 *
 * Each `output` carries one `name` and one `location` per file the subject
 * produced, so a subject whose result spanned several partitions lists every
 * file under the one name.
 *
 * @param manifest - The completion manifest, or null when none is available.
 * @returns One entry per file, in manifest order.
 *
 * @example
 * const files = parseSqlExportManifest(manifest);
 * // [{ name: "demographics", url: "https://.../$result?..." }]
 */
export function parseSqlExportManifest(
  manifest: Parameters | null | undefined,
): SqlExportOutput[] {
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

/**
 * Builds the Parameters part naming a subject, in whichever of the three
 * mutually exclusive forms the caller chose.
 *
 * @param subject - The subject source.
 * @returns The matching Parameters part.
 */
function subjectPart(subject: SubjectSource): ParametersParameter {
  switch (subject.kind) {
    case "resource":
      return {
        name: "subjectResource",
        resource: subject.resource as ParametersParameter["resource"],
      };
    case "reference":
      return {
        name: "subjectReference",
        valueReference: { reference: subject.reference },
      };
    case "canonical":
      return { name: "subjectCanonical", valueCanonical: subject.canonical };
  }
}

/**
 * Builds the repeating filter parts shared by both operations.
 *
 * @param filters - The patient, group and since filters.
 * @returns The parts to append, empty when no filter was supplied.
 */
function filterParts(filters: SubjectFilters): ParametersParameter[] {
  const parts: ParametersParameter[] = [];
  for (const patientId of filters.patientIds ?? []) {
    parts.push({
      name: "patient",
      valueReference: { reference: `Patient/${patientId}` },
    });
  }
  for (const groupId of filters.groupIds ?? []) {
    parts.push({
      name: "group",
      valueReference: { reference: `Group/${groupId}` },
    });
  }
  if (filters.since !== undefined) {
    parts.push({ name: "_since", valueInstant: filters.since });
  }
  return parts;
}
