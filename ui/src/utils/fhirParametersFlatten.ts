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
 * Flattens the FHIR-format response from the `$sqlquery-run` operation
 * (`_format=fhir`) into a tabular `{columns, rows}` shape.
 *
 * The operation emits a `Parameters` resource with one repeating `row`
 * parameter per result row, and one part per column. Each part carries
 * the value in the appropriate `value[x]` slot.
 *
 * @author John Grimes
 */

import type {
  Parameters,
  ParametersParameter,
  Quantity,
  Coding,
} from "fhir/r4";

/**
 * Result of flattening a FHIR-format `$sqlquery-run` response.
 */
export interface FlattenedFhirResult {
  /** Column names in first-seen order. */
  columns: string[];
  /** Row records keyed by column name, with values rendered as strings. */
  rows: Record<string, string>[];
}

/**
 * Flattens a Parameters resource from `_format=fhir` into a tabular shape.
 *
 * @param parameters - The Parameters response body to flatten.
 * @returns A `{columns, rows}` view of the response.
 *
 * @example
 * flattenFhirParameters({
 *   resourceType: "Parameters",
 *   parameter: [
 *     { name: "row", part: [{ name: "id", valueInteger: 1 }] },
 *   ],
 * });
 * // { columns: ["id"], rows: [{ id: "1" }] }
 */
export function flattenFhirParameters(
  parameters: Parameters,
): FlattenedFhirResult {
  const rowParts = (parameters.parameter ?? []).filter((p) => p.name === "row");

  // First pass: collect column names in first-seen order.
  const seen = new Set<string>();
  const columns: string[] = [];
  for (const rowParam of rowParts) {
    for (const part of rowParam.part ?? []) {
      if (part.name && !seen.has(part.name)) {
        seen.add(part.name);
        columns.push(part.name);
      }
    }
  }

  // Second pass: project each row into a string-keyed record using the
  // canonical column order, filling missing parts with empty strings.
  const rows = rowParts.map<Record<string, string>>((rowParam) => {
    const record: Record<string, string> = {};
    const partsByName = new Map<string, ParametersParameter>();
    for (const part of rowParam.part ?? []) {
      if (part.name) {
        partsByName.set(part.name, part);
      }
    }
    for (const column of columns) {
      const part = partsByName.get(column);
      record[column] = part ? renderPartValue(part) : "";
    }
    return record;
  });

  return { columns, rows };
}

/**
 * Renders a Parameters part's `value[x]` as a display string.
 *
 * Handles the FHIR primitive types emitted by the SQL on FHIR FHIR-format
 * implementation: boolean, integer, decimal, string, code, date, dateTime,
 * instant, quantity and coding. Unknown shapes fall back to a JSON dump.
 *
 * @param part - The Parameters part to render.
 * @returns A display string for the part's `value[x]`.
 */
function renderPartValue(part: ParametersParameter): string {
  if (part.valueBoolean !== undefined) {
    return part.valueBoolean ? "true" : "false";
  }
  if (part.valueInteger !== undefined) {
    return String(part.valueInteger);
  }
  if (part.valueDecimal !== undefined) {
    return String(part.valueDecimal);
  }
  if (part.valueString !== undefined) {
    return part.valueString;
  }
  if (part.valueCode !== undefined) {
    return part.valueCode;
  }
  if (part.valueDate !== undefined) {
    return part.valueDate;
  }
  if (part.valueDateTime !== undefined) {
    return part.valueDateTime;
  }
  if (part.valueInstant !== undefined) {
    return part.valueInstant;
  }
  if (part.valueQuantity !== undefined) {
    return renderQuantity(part.valueQuantity);
  }
  if (part.valueCoding !== undefined) {
    return renderCoding(part.valueCoding);
  }
  return "";
}

/**
 * Renders a FHIR Quantity as `<value> <unit>` or just `<value>`.
 *
 * @param quantity - The Quantity to render.
 * @returns A short display string.
 */
function renderQuantity(quantity: Quantity): string {
  const value = quantity.value !== undefined ? String(quantity.value) : "";
  const unit = quantity.unit ?? quantity.code ?? "";
  return unit ? `${value} ${unit}`.trim() : value;
}

/**
 * Renders a FHIR Coding as its display, falling back to `system|code`.
 *
 * @param coding - The Coding to render.
 * @returns A short display string.
 */
function renderCoding(coding: Coding): string {
  if (coding.display) {
    return coding.display;
  }
  if (coding.system && coding.code) {
    return `${coding.system}|${coding.code}`;
  }
  return coding.code ?? "";
}
