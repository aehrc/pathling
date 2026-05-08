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
 * Minimal CSV parser used to render `$sqlquery-run` results in a table.
 *
 * Mirrors the public surface of `parseNdjsonResponse` from `./ndjson`.
 *
 * @author John Grimes
 */

/**
 * Parses a CSV string with a leading header row into an array of records.
 *
 * Supports double-quoted values containing commas, newlines and doubled
 * quotes. Treats the first non-empty line as the header.
 *
 * @param csv - The CSV text to parse.
 * @returns An array of records keyed by the header row's column names.
 *
 * @example
 * parseCsvResponse("id,name\\n1,Alice");
 * // [{ id: "1", name: "Alice" }]
 */
export function parseCsvResponse(csv: string): Record<string, string>[] {
  const rows = parseCsvRows(csv);
  if (rows.length === 0) {
    return [];
  }
  const [header, ...dataRows] = rows;
  return dataRows.map((row) => {
    const record: Record<string, string> = {};
    for (let i = 0; i < header.length; i++) {
      record[header[i]] = row[i] ?? "";
    }
    return record;
  });
}

/**
 * Parses a CSV string into raw row arrays, without applying a header.
 *
 * @param csv - The CSV text to parse.
 * @returns The parsed rows, each row being an array of cell values.
 */
function parseCsvRows(csv: string): string[][] {
  const rows: string[][] = [];
  let row: string[] = [];
  let cell = "";
  let inQuotes = false;
  let i = 0;

  while (i < csv.length) {
    const ch = csv[i];

    if (inQuotes) {
      if (ch === '"') {
        // A doubled quote inside a quoted value is an escaped literal quote.
        if (i + 1 < csv.length && csv[i + 1] === '"') {
          cell += '"';
          i += 2;
          continue;
        }
        inQuotes = false;
        i++;
        continue;
      }
      cell += ch;
      i++;
      continue;
    }

    if (ch === '"') {
      inQuotes = true;
      i++;
      continue;
    }

    if (ch === ",") {
      row.push(cell);
      cell = "";
      i++;
      continue;
    }

    if (ch === "\r") {
      // Treat \r\n as a single newline; bare \r is also a row break.
      row.push(cell);
      rows.push(row);
      row = [];
      cell = "";
      i++;
      if (i < csv.length && csv[i] === "\n") {
        i++;
      }
      continue;
    }

    if (ch === "\n") {
      row.push(cell);
      rows.push(row);
      row = [];
      cell = "";
      i++;
      continue;
    }

    cell += ch;
    i++;
  }

  // Emit any pending cell unless the string ended cleanly on a newline.
  if (cell.length > 0 || row.length > 0) {
    row.push(cell);
    rows.push(row);
  }

  return rows;
}
