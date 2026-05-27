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
 * Unicode-safe Base64 helpers for SQL strings carried in
 * `Library.content[0].data`.
 *
 * @author John Grimes
 */

/**
 * Encodes a SQL string as Base64.
 *
 * Wraps `btoa` with a UTF-8 conversion so that strings containing
 * non-ASCII characters (e.g. accented identifiers) encode correctly.
 *
 * @param sql - The SQL text to encode.
 * @returns The Base64-encoded representation of `sql`.
 *
 * @example
 * encodeSql("SELECT 1");
 * // "U0VMRUNUIDE="
 */
export function encodeSql(sql: string): string {
  if (sql.length === 0) {
    return "";
  }
  const bytes = new TextEncoder().encode(sql);
  let binary = "";
  for (const byte of bytes) {
    binary += String.fromCharCode(byte);
  }
  return globalThis.btoa(binary);
}

/**
 * Decodes a Base64-encoded SQL string.
 *
 * Tolerant of unpadded Base64 input (some servers strip trailing `=`
 * characters).
 *
 * @param data - The Base64 text to decode.
 * @returns The decoded UTF-8 string.
 *
 * @example
 * decodeSql("U0VMRUNUIDE=");
 * // "SELECT 1"
 */
export function decodeSql(data: string): string {
  if (data.length === 0) {
    return "";
  }
  const padded = data + "===".slice((data.length + 3) % 4);
  const binary = globalThis.atob(padded);
  const bytes = new Uint8Array(binary.length);
  for (let i = 0; i < binary.length; i++) {
    bytes[i] = binary.charCodeAt(i);
  }
  return new TextDecoder().decode(bytes);
}
