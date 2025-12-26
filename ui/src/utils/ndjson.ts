/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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
 *
 * Author: John Grimes
 */

/**
 * Reads a ReadableStream to completion and returns the text content.
 *
 * @param stream - The ReadableStream to read.
 * @returns A promise that resolves to the complete text content of the stream.
 */
export async function streamToText(stream: ReadableStream): Promise<string> {
  const reader = stream.getReader();
  const chunks: Uint8Array[] = [];

  while (true) {
    const { done, value } = await reader.read();
    if (done) break;
    chunks.push(value);
  }

  const decoder = new TextDecoder();
  return (
    chunks.map((chunk) => decoder.decode(chunk, { stream: true })).join("") +
    decoder.decode()
  );
}

/**
 * Parses an NDJSON (Newline Delimited JSON) string into an array of objects.
 *
 * @param ndjson - The NDJSON string to parse.
 * @returns An array of parsed JSON objects.
 */
export function parseNdjsonResponse(ndjson: string): Record<string, unknown>[] {
  return ndjson
    .split("\n")
    .filter((line) => line.trim() !== "")
    .map((line) => JSON.parse(line));
}

/**
 * Extracts column names from the first row of results.
 *
 * @param rows - The array of result rows.
 * @returns An array of column names, or an empty array if no rows exist.
 */
export function extractColumns(rows: Record<string, unknown>[]): string[] {
  if (rows.length === 0) return [];
  return Object.keys(rows[0]);
}
