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

package au.csiro.pathling.operations.bulkexport;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import lombok.Getter;
import org.jetbrains.annotations.Contract;

/**
 * Represents the output format for bulk export operations.
 *
 * @author Felix Naumann
 * @author John Grimes
 */
@Getter
public enum ExportOutputFormat {
  /** Newline-delimited JSON format. */
  NDJSON("application/fhir+ndjson"),
  /**
   * Parquet format.
   *
   * @see <a href="https://pathling.csiro.au/docs/libraries/io/schema">Pathling Parquet
   *     Specification</a>
   */
  PARQUET("application/vnd.apache.parquet");

  private final String mimeType;

  ExportOutputFormat(final String mimeType) {
    this.mimeType = mimeType;
  }

  /**
   * Parses a format parameter string and returns the corresponding ExportOutputFormat.
   *
   * @param param the format parameter string (e.g., "application/fhir+ndjson", "parquet")
   * @return the corresponding ExportOutputFormat, or null if the format is invalid. Returns NDJSON
   *     if param is null.
   */
  @Nullable
  @Contract(pure = true)
  public static ExportOutputFormat fromParam(@Nullable final String param) {
    if (param == null) {
      return NDJSON;
    }
    return switch (param.toLowerCase()) {
      case "application/fhir+ndjson", "application/ndjson", "ndjson" -> NDJSON;
      case "application/vnd.apache.parquet", "parquet" -> PARQUET;
      default -> null;
    };
  }

  /**
   * Converts the export output format to its parameter string representation.
   *
   * @param exportOutputFormat the export output format to convert
   * @return the string representation used in export parameters
   */
  @Nonnull
  @Contract(pure = true)
  public static String asParam(@Nonnull final ExportOutputFormat exportOutputFormat) {
    return switch (exportOutputFormat) {
      case NDJSON -> "ndjson";
      case PARQUET -> "parquet";
    };
  }
}
