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

import { decodeSql, encodeSql } from "../sqlBase64";

describe("encodeSql / decodeSql", () => {
  // ASCII SQL is the common case.
  it("round-trips an ASCII SQL string", () => {
    const sql = "SELECT * FROM patients WHERE id = 1";
    expect(decodeSql(encodeSql(sql))).toBe(sql);
  });

  // Encoding must produce the expected canonical Base64 for a known string,
  // so the server sees the exact bytes a Bash one-liner would produce.
  it("matches the canonical Base64 for an ASCII SQL string", () => {
    expect(encodeSql("SELECT 1")).toBe("U0VMRUNUIDE=");
  });

  // Unicode characters must be UTF-8 encoded before Base64; otherwise the
  // browser's `btoa` raises an error.
  it("round-trips a string containing non-ASCII characters", () => {
    const sql = "SELECT 'café' AS name";
    expect(decodeSql(encodeSql(sql))).toBe(sql);
  });

  // Whitespace, including newlines, should round-trip unchanged.
  it("round-trips multi-line SQL with embedded whitespace", () => {
    const sql = "SELECT a,\n       b\nFROM t\nWHERE x = 1";
    expect(decodeSql(encodeSql(sql))).toBe(sql);
  });

  // Empty SQL must encode to the empty string and round-trip cleanly.
  it("round-trips an empty string", () => {
    expect(encodeSql("")).toBe("");
    expect(decodeSql("")).toBe("");
  });

  // The decoder should be tolerant of base64 strings that lack padding,
  // which is common when servers strip `=` characters.
  it("decodes a Base64 string without padding", () => {
    expect(decodeSql("U0VMRUNUIDE")).toBe("SELECT 1");
  });
});
