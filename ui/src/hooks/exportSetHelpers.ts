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
 * Pure helpers for the export set: the basket of subjects a user composes on
 * the SQL on FHIR page before exporting them as one job.
 *
 * Kept as plain functions so the capture, naming and request-building rules
 * can be tested without mounting React.
 *
 * @author John Grimes
 */

import { buildBindingsResource, toSubjectSource } from "./sqlQueryHelpers";

import type { SqlExportRequest } from "./useSqlExport";
import type { SubjectSource } from "../api";
import type { SqlExportFormat } from "../types/sqlExport";
import type { SqlQueryRequest } from "../types/sqlQuery";
import type { ViewRunRequest } from "../types/viewJob";
import type { Parameters } from "fhir/r4";

/** Which kind of subject an entry captured, shown as a badge. */
export type ExportSetEntryKind = "view" | "query";

/**
 * One captured subject in the export set.
 *
 * The subject and its bindings are frozen at the moment the entry was added,
 * so later edits to the form leave entries already in the set untouched: the
 * user is composing a job out of the things they saw, not out of whatever the
 * form happens to hold when they press Export.
 */
export interface ExportSetEntry {
  /** Stable identifier, used as a React key and for removal. */
  id: string;
  /** The output name, editable in place. */
  name: string;
  /** The kind of subject captured. */
  kind: ExportSetEntryKind;
  /** How the subject is named on the wire, frozen at capture time. */
  subject: SubjectSource;
  /** Runtime bindings, frozen at capture time; absent for a view. */
  parameters?: Parameters;
}

/** The job-wide filters that apply to every subject in the set. */
export interface ExportSetFilters {
  /** Patient ids, one `patient` parameter each. */
  patientIds?: string[];
  /** Group ids, one `group` parameter each. */
  groupIds?: string[];
  /** Restricts to resources updated at or after this instant. */
  since?: string;
}

/**
 * Captures the current ViewDefinition form state as an export set entry.
 *
 * @param id - A stable identifier for the new entry.
 * @param request - The view run request the form would submit.
 * @param existing - The entries already in the set, used to derive a name that
 *   does not collide.
 * @returns The captured entry.
 * @throws {Error} When the request names no view definition at all.
 *
 * @example
 * captureViewEntry("1", { mode: "stored", viewDefinitionId: "demographics" }, []);
 */
export function captureViewEntry(
  id: string,
  request: ViewRunRequest,
  existing: readonly ExportSetEntry[],
): ExportSetEntry {
  if (request.mode === "stored" && request.viewDefinitionId) {
    return {
      id,
      kind: "view",
      name: uniqueName(request.viewDefinitionId, existing),
      subject: {
        kind: "reference",
        reference: `ViewDefinition/${request.viewDefinitionId}`,
      },
    };
  }
  if (request.mode === "inline" && request.viewDefinitionJson) {
    const parsed = JSON.parse(request.viewDefinitionJson) as {
      name?: string;
    };
    return {
      id,
      kind: "view",
      name: uniqueName(parsed.name ?? "view", existing),
      subject: { kind: "resource", resource: parsed },
    };
  }
  throw new Error("Invalid request: missing view definition ID or JSON");
}

/**
 * Captures the current SQL query form state as an export set entry, including
 * any runtime bindings.
 *
 * @param id - A stable identifier for the new entry.
 * @param request - The SQL query request the form would submit.
 * @param existing - The entries already in the set, used to derive a name that
 *   does not collide.
 * @returns The captured entry.
 *
 * @example
 * captureQueryEntry("1", { mode: "stored", libraryId: "bp-summary" }, []);
 */
export function captureQueryEntry(
  id: string,
  request: SqlQueryRequest,
  existing: readonly ExportSetEntry[],
): ExportSetEntry {
  const seed =
    request.mode === "stored"
      ? request.libraryId
      : (request.library.name ?? request.library.title ?? "query");
  return {
    id,
    kind: "query",
    name: uniqueName(seed, existing),
    subject: toSubjectSource(request),
    parameters: buildBindingsResource(request.bindings, request.parameterTypes),
  };
}

/**
 * Renames an entry, leaving the rest of the set untouched.
 *
 * @param entries - The current set.
 * @param id - The id of the entry to rename.
 * @param name - The new name, as typed.
 * @returns A new set with the entry renamed.
 *
 * @example
 * renameEntry(entries, "1", "demographics");
 */
export function renameEntry(
  entries: readonly ExportSetEntry[],
  id: string,
  name: string,
): ExportSetEntry[] {
  return entries.map((entry) => (entry.id === id ? { ...entry, name } : entry));
}

/**
 * Removes an entry from the set.
 *
 * @param entries - The current set.
 * @param id - The id of the entry to remove.
 * @returns A new set without that entry.
 *
 * @example
 * removeEntry(entries, "1");
 */
export function removeEntry(
  entries: readonly ExportSetEntry[],
  id: string,
): ExportSetEntry[] {
  return entries.filter((entry) => entry.id !== id);
}

/**
 * Finds the names shared by more than one entry.
 *
 * The manifest correlates outputs by name, so two entries sharing one would
 * leave the user unable to tell which file is which. The server rejects it
 * too, but the set is checked here so the problem is visible before a job is
 * ever started. A blank name counts as a collision in its own right, since it
 * names nothing.
 *
 * @param entries - The current set.
 * @returns The offending names, in the order they first appear; empty when
 *   every name is distinct and non-blank.
 *
 * @example
 * findNameCollisions([{ name: "a" }, { name: "a" }]); // ["a"]
 */
export function findNameCollisions(
  entries: readonly ExportSetEntry[],
): string[] {
  const seen = new Set<string>();
  const collisions: string[] = [];
  for (const entry of entries) {
    const name = entry.name.trim();
    if (name === "") {
      if (!collisions.includes("")) {
        collisions.push("");
      }
      continue;
    }
    if (seen.has(name) && !collisions.includes(name)) {
      collisions.push(name);
    }
    seen.add(name);
  }
  return collisions;
}

/**
 * Builds the kick-off request for the whole set.
 *
 * @param entries - The captured entries, in set order.
 * @param format - The chosen output format.
 * @param filters - The job-wide filters.
 * @param header - Whether CSV output carries a header row.
 * @returns The request to hand to the export hook.
 *
 * @example
 * buildExportSetRequest(entries, "csv", { patientIds: ["p1"] });
 */
export function buildExportSetRequest(
  entries: readonly ExportSetEntry[],
  format: SqlExportFormat,
  filters: ExportSetFilters = {},
  header = true,
): SqlExportRequest {
  return {
    subjects: entries.map((entry) => ({
      name: entry.name.trim(),
      subject: entry.subject,
      parameters: entry.parameters,
    })),
    format,
    header,
    patientIds: filters.patientIds?.length ? filters.patientIds : undefined,
    groupIds: filters.groupIds?.length ? filters.groupIds : undefined,
    since: filters.since?.trim() ? filters.since.trim() : undefined,
  };
}

/**
 * Parses a comma or whitespace separated list of ids into a clean array.
 *
 * @param value - The raw field value.
 * @returns The ids, with blanks removed; empty when nothing was entered.
 *
 * @example
 * parseIdList("p1, p2  p3"); // ["p1", "p2", "p3"]
 */
export function parseIdList(value: string): string[] {
  return value
    .split(/[\s,]+/)
    .map((id) => id.trim())
    .filter((id) => id !== "");
}

/**
 * Derives a name that does not already appear in the set, appending a numeric
 * suffix on collision so a repeated capture is still usable without renaming.
 *
 * @param seed - The preferred name.
 * @param existing - The entries already in the set.
 * @returns A name not used by any existing entry.
 */
function uniqueName(seed: string, existing: readonly ExportSetEntry[]): string {
  const used = new Set(existing.map((entry) => entry.name));
  if (!used.has(seed)) {
    return seed;
  }
  let suffix = 2;
  while (used.has(`${seed}_${suffix}`)) {
    suffix++;
  }
  return `${seed}_${suffix}`;
}
