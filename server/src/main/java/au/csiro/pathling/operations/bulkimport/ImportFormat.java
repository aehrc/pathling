/*
 * Copyright 2025 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.operations.bulkimport;

import lombok.Getter;

/**
 * Represents the supported formats for resource import. MIME type variants (e.g.,
 * "application/fhir+ndjson") are mapped to these codes by the ImportOperationValidator.
 */
@Getter
public enum ImportFormat {
  /**
   * Newline-delimited JSON (NDJSON) format. MIME type: application/fhir+ndjson.
   */
  NDJSON("ndjson"),
  /**
   * Parquet format (Pathling extension). MIME type: application/parquet.
   */
  PARQUET("parquet"),
  /**
   * Delta Lake format (Pathling extension). MIME type: application/delta.
   */
  DELTA("delta");

  private final String code;

  ImportFormat(final String code) {
    this.code = code;
  }

  /**
   * Resolve an ImportFormat enum from its string code. Note: MIME type variants (e.g.,
   * "application/fhir+ndjson") should be mapped by the validator before calling this method.
   *
   * @param code The string code to resolve (simple format codes only).
   * @return An ImportFormat if a match is found.
   * @throws IllegalArgumentException if no match can be found.
   */
  public static ImportFormat fromCode(final String code) {
    for (final ImportFormat format : ImportFormat.values()) {
      if (format.getCode().equalsIgnoreCase(code)) {
        return format;
      }
    }
    throw new IllegalArgumentException("Unsupported format: " + code);
  }

}
