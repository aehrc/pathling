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

import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
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
   * Parses an explicit {@code _format} parameter value into a ViewExportFormat. A null or blank
   * value maps to the default (NDJSON); a non-blank value that matches no supported code or media
   * type is rejected.
   *
   * <p>The export operation has a single explicit {@code _format} entry point and no lenient
   * content-negotiation path, so this parser is strict.
   *
   * @param format the explicit {@code _format} value to parse, or null/blank for the default
   * @return the corresponding ViewExportFormat
   * @throws InvalidRequestException if the value is non-blank and not a supported format
   */
  @Nonnull
  public static ViewExportFormat fromString(@Nullable final String format) {
    if (format == null || format.isBlank()) {
      return NDJSON;
    }
    // Strip any media-type parameters (e.g. "text/csv;charset=utf-8" -> "text/csv") so a supported
    // media type carrying parameters is treated as that format.
    final String base = format.split(";", 2)[0].trim().toLowerCase();
    return Arrays.stream(values())
        .filter(f -> f.code.equals(base) || f.contentType.equals(base))
        .findFirst()
        .orElseThrow(
            () ->
                new InvalidRequestException(
                    "Unsupported _format value '%s'. Supported formats: ndjson, csv, parquet."
                        .formatted(format)));
  }
}
