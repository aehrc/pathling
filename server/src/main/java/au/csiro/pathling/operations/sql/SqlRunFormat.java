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

package au.csiro.pathling.operations.sql;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Optional;
import java.util.Set;
import lombok.Getter;
import org.hl7.fhir.r4.model.OperationOutcome.IssueType;

/**
 * The output formats {@code $sql-run} offers, and the per-subject-kind rules that decide which of
 * them a given request may ask for.
 *
 * <p>Pathling declares partial support: {@code json}, {@code ndjson} and {@code csv} are available
 * for every subject kind, while {@code parquet} and {@code fhir} are available only for the SQL
 * kinds, because they are produced by the SQL evaluation engine and have no counterpart on the
 * ViewDefinition path. Asking for one of them with a ViewDefinition subject is rejected rather than
 * quietly downgraded, and the constraint is stated in the CapabilityStatement so a client can
 * discover it without provoking a 400.
 *
 * @author John Grimes
 */
@Getter
public enum SqlRunFormat {

  /** JSON format - a single document containing an array of row objects. */
  JSON("json", "application/json", true),

  /** Newline-delimited JSON format. */
  NDJSON("ndjson", "application/x-ndjson", true),

  /** Comma-separated values format. */
  CSV("csv", "text/csv", true),

  /** Apache Parquet columnar format, offered for SQL subjects only. */
  PARQUET("parquet", "application/vnd.apache.parquet", false),

  /**
   * FHIR-native format - a {@code Parameters} resource with one {@code row} parameter per result
   * row. Offered for SQL subjects only.
   */
  FHIR("fhir", "application/fhir+json", false);

  /** The format used when neither {@code _format} nor {@code Accept} selects one. */
  public static final SqlRunFormat DEFAULT_FORMAT = NDJSON;

  /** The {@code expression} value naming the format parameter in error outcomes. */
  public static final String FORMAT_EXPRESSION = "_format";

  @Nonnull private final String code;

  @Nonnull private final String contentType;

  /** Whether the format is offered for every subject kind, rather than the SQL kinds only. */
  private final boolean availableForViewDefinitions;

  SqlRunFormat(
      @Nonnull final String code,
      @Nonnull final String contentType,
      final boolean availableForViewDefinitions) {
    this.code = code;
    this.contentType = contentType;
    this.availableForViewDefinitions = availableForViewDefinitions;
  }

  /**
   * Indicates whether this format is offered for the given subject kind.
   *
   * @param kind the subject kind
   * @return true if the format may be requested for that kind
   */
  public boolean isAvailableFor(@Nonnull final SubjectKind kind) {
    return kind.isSql() || availableForViewDefinitions;
  }

  /**
   * Returns the format codes offered for the given subject kind, in declaration order.
   *
   * @param kind the subject kind
   * @return the supported format codes
   */
  @Nonnull
  public static Set<String> codesFor(@Nonnull final SubjectKind kind) {
    return Arrays.stream(values())
        .filter(format -> format.isAvailableFor(kind))
        .map(SqlRunFormat::getCode)
        .collect(java.util.stream.Collectors.toCollection(java.util.LinkedHashSet::new));
  }

  /**
   * Selects the output format for a request. An explicit {@code _format} takes precedence and is
   * parsed strictly, so an unrecognised value is rejected rather than silently defaulting; {@code
   * Accept} negotiation is lenient and falls back to NDJSON, since a client that expresses only a
   * weak preference should still get a result.
   *
   * @param format the explicit {@code _format} value, or null when not supplied
   * @param acceptHeader the HTTP {@code Accept} header value, or null
   * @param kind the resolved subject kind, which decides the available set
   * @return the selected format
   * @throws ca.uhn.fhir.rest.server.exceptions.InvalidRequestException (400) with {@code issue.code
   *     = not-supported} if the requested format is unrecognised, or is not offered for the subject
   *     kind
   */
  @Nonnull
  public static SqlRunFormat select(
      @Nullable final String format,
      @Nullable final String acceptHeader,
      @Nonnull final SubjectKind kind) {
    if (format == null || format.isBlank()) {
      return fromAcceptHeader(acceptHeader, kind);
    }
    final SqlRunFormat selected =
        match(format)
            .orElseThrow(
                () ->
                    SqlOperationError.badRequest(
                        IssueType.NOTSUPPORTED,
                        FORMAT_EXPRESSION,
                        "Unsupported _format value '%s'. Supported formats for this subject: %s."
                            .formatted(format, String.join(", ", codesFor(kind)))));
    if (!selected.isAvailableFor(kind)) {
      throw SqlOperationError.badRequest(
          IssueType.NOTSUPPORTED,
          FORMAT_EXPRESSION,
          "The '%s' format is not supported for a ViewDefinition subject. Supported formats: %s."
              .formatted(selected.code, String.join(", ", codesFor(kind))));
    }
    return selected;
  }

  /**
   * Derives the format from an {@code Accept} header, ignoring media types the subject kind does
   * not offer, and falling back to the default.
   */
  @Nonnull
  private static SqlRunFormat fromAcceptHeader(
      @Nullable final String acceptHeader, @Nonnull final SubjectKind kind) {
    if (acceptHeader == null || acceptHeader.isBlank()) {
      return DEFAULT_FORMAT;
    }
    return Arrays.stream(acceptHeader.split(","))
        .map(SqlRunFormat::parseMediaType)
        .sorted(Comparator.comparingDouble(AcceptEntry::quality).reversed())
        .map(entry -> matchContentType(entry.type(), kind))
        .flatMap(Optional::stream)
        .findFirst()
        .orElse(DEFAULT_FORMAT);
  }

  /** Matches a {@code _format} value against the supported codes and media types. */
  @Nonnull
  private static Optional<SqlRunFormat> match(@Nonnull final String format) {
    // Strip any media-type parameters, so a supported media type carrying parameters (for example
    // "text/csv;charset=utf-8") is treated as that format.
    final String base = format.split(";", 2)[0].trim().toLowerCase();
    return Arrays.stream(values())
        .filter(f -> f.code.equals(base) || f.contentType.equals(base))
        .findFirst();
  }

  /** Matches an Accept media type against the formats offered for the subject kind. */
  @Nonnull
  private static Optional<SqlRunFormat> matchContentType(
      @Nonnull final String contentType, @Nonnull final SubjectKind kind) {
    if ("*/*".equals(contentType)) {
      return Optional.of(DEFAULT_FORMAT);
    }
    return Arrays.stream(values())
        .filter(f -> f.contentType.equals(contentType) && f.isAvailableFor(kind))
        .findFirst();
  }

  /** Parses a single Accept entry, such as {@code text/csv;q=0.9}. */
  @Nonnull
  private static AcceptEntry parseMediaType(@Nonnull final String entry) {
    final String[] parts = entry.split(";");
    return new AcceptEntry(parts[0].trim().toLowerCase(), quality(parts));
  }

  /** Extracts the quality value from an Accept entry's parameters, defaulting to 1.0. */
  private static double quality(@Nonnull final String[] parts) {
    for (int i = 1; i < parts.length; i++) {
      final String parameter = parts[i].trim();
      if (parameter.startsWith("q=")) {
        try {
          return Double.parseDouble(parameter.substring(2).trim());
        } catch (final NumberFormatException e) {
          return 1.0;
        }
      }
    }
    return 1.0;
  }
}
