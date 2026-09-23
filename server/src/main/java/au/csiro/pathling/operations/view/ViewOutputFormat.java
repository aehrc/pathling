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
import java.util.Optional;
import lombok.Getter;

/**
 * Output format options for the ViewDefinition run operation.
 *
 * @author John Grimes
 */
@Getter
public enum ViewOutputFormat {

  /** Newline-delimited JSON format. */
  NDJSON("ndjson", "application/x-ndjson"),

  /** Comma-separated values format. */
  CSV("csv", "text/csv"),

  /** JSON format - single document containing an array of objects. */
  JSON("json", "application/json");

  @Nonnull private final String code;

  @Nonnull private final String contentType;

  ViewOutputFormat(@Nonnull final String code, @Nonnull final String contentType) {
    this.code = code;
    this.contentType = contentType;
  }

  /** The default format when no valid format is specified. */
  private static final ViewOutputFormat DEFAULT_FORMAT = NDJSON;

  /**
   * Parses an explicit {@code _format} parameter value strictly. A null or blank value maps to the
   * default (NDJSON); a non-blank value that matches no supported code or media type is rejected.
   *
   * @param format the explicit {@code _format} value to parse, or null/blank for the default
   * @return the corresponding ViewOutputFormat
   * @throws InvalidRequestException if the value is non-blank and not a supported format
   */
  @Nonnull
  public static ViewOutputFormat fromStringStrict(@Nullable final String format) {
    if (isNullOrBlank(format)) {
      return DEFAULT_FORMAT;
    }
    return matchFormat(format)
        .orElseThrow(
            () ->
                new InvalidRequestException(
                    "Unsupported _format value '%s'. Supported formats: ndjson, csv, json."
                        .formatted(format)));
  }

  /**
   * Matches a format string against the supported codes and content types. Any media-type
   * parameters (e.g. {@code text/csv;charset=utf-8}) are stripped before matching, so a supported
   * media type carrying parameters is treated as that format.
   */
  @Nonnull
  private static Optional<ViewOutputFormat> matchFormat(@Nonnull final String format) {
    final String base = format.split(";", 2)[0].trim().toLowerCase();
    return Arrays.stream(values())
        .filter(f -> f.code.equals(base) || f.contentType.equals(base))
        .findFirst();
  }

  /** Checks if a string is null or blank. */
  private static boolean isNullOrBlank(@Nullable final String value) {
    return value == null || value.isBlank();
  }
}
