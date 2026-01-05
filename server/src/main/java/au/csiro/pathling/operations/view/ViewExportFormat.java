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

package au.csiro.pathling.operations.view;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Arrays;
import lombok.Getter;

/**
 * Output format options for the ViewDefinition export operation.
 *
 * @author John Grimes
 */
@Getter
public enum ViewExportFormat {

  /** Newline-delimited JSON format. */
  NDJSON("ndjson", "application/x-ndjson", ".ndjson"),

  /** Comma-separated values format. */
  CSV("csv", "text/csv", ".csv"),

  /** Apache Parquet columnar format. */
  PARQUET("parquet", "application/vnd.apache.parquet", ".parquet");

  @Nonnull private final String code;

  @Nonnull private final String contentType;

  @Nonnull private final String fileExtension;

  ViewExportFormat(
      @Nonnull final String code,
      @Nonnull final String contentType,
      @Nonnull final String fileExtension) {
    this.code = code;
    this.contentType = contentType;
    this.fileExtension = fileExtension;
  }

  /**
   * Parses a format string into a ViewExportFormat.
   *
   * @param format the format string to parse, or null for default
   * @return the corresponding ViewExportFormat, defaulting to NDJSON
   */
  @Nonnull
  public static ViewExportFormat fromString(@Nullable final String format) {
    if (format == null || format.isBlank()) {
      return NDJSON;
    }
    final String normalised = format.toLowerCase().trim();
    return Arrays.stream(values())
        .filter(f -> f.code.equals(normalised) || f.contentType.equals(normalised))
        .findFirst()
        .orElse(NDJSON);
  }
}
