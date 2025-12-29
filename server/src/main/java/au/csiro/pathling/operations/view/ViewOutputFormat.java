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
 */

package au.csiro.pathling.operations.view;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Arrays;
import java.util.Comparator;
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
   * Parses a format string into a ViewOutputFormat.
   *
   * @param format the format string to parse, or null for default
   * @return the corresponding ViewOutputFormat, defaulting to NDJSON
   */
  @Nonnull
  public static ViewOutputFormat fromString(@Nullable final String format) {
    if (isNullOrBlank(format)) {
      return DEFAULT_FORMAT;
    }
    final String normalised = format.toLowerCase().trim();
    return Arrays.stream(values())
        .filter(f -> f.code.equals(normalised) || f.contentType.equals(normalised))
        .findFirst()
        .orElse(DEFAULT_FORMAT);
  }

  /**
   * Parses an HTTP Accept header into a ViewOutputFormat. Supports quality values (e.g.,
   * "text/csv;q=0.9, application/x-ndjson;q=1.0") and returns the format with the highest quality
   * value that matches a supported content type.
   *
   * @param acceptHeader the Accept header value, or null for default
   * @return the corresponding ViewOutputFormat, defaulting to NDJSON
   */
  @Nonnull
  public static ViewOutputFormat fromAcceptHeader(@Nullable final String acceptHeader) {
    if (isNullOrBlank(acceptHeader)) {
      return DEFAULT_FORMAT;
    }

    // Parse media types with quality values and sort by quality descending.
    return Arrays.stream(acceptHeader.split(","))
        .map(ViewOutputFormat::parseMediaType)
        .sorted(Comparator.comparingDouble(MediaType::quality).reversed())
        .map(mt -> matchContentType(mt.type()))
        .flatMap(Optional::stream)
        .findFirst()
        .orElse(DEFAULT_FORMAT);
  }

  /** Checks if a string is null or blank. */
  private static boolean isNullOrBlank(@Nullable final String value) {
    return value == null || value.isBlank();
  }

  /** Matches a content type string against supported formats. */
  @Nonnull
  private static Optional<ViewOutputFormat> matchContentType(@Nonnull final String contentType) {
    // Handle wildcard.
    if ("*/*".equals(contentType)) {
      return Optional.of(DEFAULT_FORMAT);
    }
    return Arrays.stream(values()).filter(f -> f.contentType.equals(contentType)).findFirst();
  }

  /** Parses a single media type entry from an Accept header (e.g., "text/csv;q=0.9"). */
  @Nonnull
  private static MediaType parseMediaType(@Nonnull final String mediaTypeString) {
    final String[] parts = mediaTypeString.split(";");
    final String type = parts[0].trim().toLowerCase();
    final double quality = extractQualityValue(parts);
    return new MediaType(type, quality);
  }

  /** Extracts the quality value from media type parameters, defaulting to 1.0. */
  private static double extractQualityValue(@Nonnull final String[] parts) {
    for (int i = 1; i < parts.length; i++) {
      final String param = parts[i].trim();
      if (param.startsWith("q=")) {
        try {
          return Double.parseDouble(param.substring(2).trim());
        } catch (final NumberFormatException e) {
          return 1.0;
        }
      }
    }
    return 1.0;
  }

  /** Represents a parsed media type with its quality value. */
  private record MediaType(@Nonnull String type, double quality) {}
}
