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

import { describe, expect, it } from "vitest";

import { parseCsvResponse } from "../csv";

describe("parseCsvResponse", () => {
  // Basic parsing: a header row drives the column order and a single data
  // row produces a populated record.
  it("parses a header row plus one data row", () => {
    const csv = "id,name\n1,Alice";
    const rows = parseCsvResponse(csv);
    expect(rows).toEqual([{ id: "1", name: "Alice" }]);
  });

  // CRLF line endings are common in CSV exports from Windows tooling, so
  // they must be supported alongside LF.
  it("handles CRLF line endings", () => {
    const csv = "id,name\r\n1,Alice\r\n2,Bob";
    const rows = parseCsvResponse(csv);
    expect(rows).toEqual([
      { id: "1", name: "Alice" },
      { id: "2", name: "Bob" },
    ]);
  });

  // Quoted values may contain commas; the comma should not be treated as
  // a field separator inside a quoted region.
  it("preserves commas inside quoted values", () => {
    const csv = 'id,name\n1,"Smith, John"';
    const rows = parseCsvResponse(csv);
    expect(rows).toEqual([{ id: "1", name: "Smith, John" }]);
  });

  // Quoted values may also contain newlines; these must not split the row.
  it("preserves newlines inside quoted values", () => {
    const csv = 'id,note\n1,"line one\nline two"';
    const rows = parseCsvResponse(csv);
    expect(rows).toEqual([{ id: "1", note: "line one\nline two" }]);
  });

  // Embedded double-quote characters are encoded as a doubled quote; the
  // parser collapses each pair to a single literal quote.
  it("decodes doubled quotes inside a quoted value", () => {
    const csv = 'id,note\n1,"He said ""hi"""';
    const rows = parseCsvResponse(csv);
    expect(rows).toEqual([{ id: "1", note: 'He said "hi"' }]);
  });

  // Empty cells should be retained as empty strings; the column count is
  // governed by the header row.
  it("preserves empty cells", () => {
    const csv = "id,middle,last\n1,,Smith";
    const rows = parseCsvResponse(csv);
    expect(rows).toEqual([{ id: "1", middle: "", last: "Smith" }]);
  });

  // A trailing newline should not produce a phantom row.
  it("ignores a trailing newline", () => {
    const csv = "id,name\n1,Alice\n";
    const rows = parseCsvResponse(csv);
    expect(rows).toEqual([{ id: "1", name: "Alice" }]);
  });

  // A header-only payload yields zero rows.
  it("returns no rows for header-only input", () => {
    const csv = "id,name\n";
    const rows = parseCsvResponse(csv);
    expect(rows).toEqual([]);
  });

  // An entirely empty payload returns no rows.
  it("returns no rows for empty input", () => {
    expect(parseCsvResponse("")).toEqual([]);
  });
});
