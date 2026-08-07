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
import lombok.Getter;
import org.hl7.fhir.r4.model.OperationOutcome.IssueType;

/**
 * The file formats an {@code $sql-export} job can write.
 *
 * <p>The set is narrower than that of {@code $sql-run}, because an export writes bulk files rather
 * than a single streamed response. Two formats the specification lists are refused for distinct
 * reasons, and the distinction is preserved in the issue code: {@code json} is a format this server
 * has not implemented for export ({@code not-supported}), while {@code fhir} is meaningless for a
 * bulk file set and is a client mistake ({@code invalid}).
 *
 * @author John Grimes
 */
@Getter
public enum SqlExportFormat {

  /** Newline-delimited JSON, the default. */
  NDJSON("ndjson", "application/x-ndjson", ".ndjson"),

  /** Comma-separated values. */
  CSV("csv", "text/csv", ".csv"),

  /** Apache Parquet. */
  PARQUET("parquet", "application/vnd.apache.parquet", ".parquet");

  /** The {@code expression} value naming the format parameter in error outcomes. */
  public static final String FORMAT_EXPRESSION = "_format";

  /** The format used when the request names none. */
  public static final SqlExportFormat DEFAULT_FORMAT = NDJSON;

  @Nonnull private final String code;

  @Nonnull private final String contentType;

  @Nonnull private final String fileExtension;

  SqlExportFormat(
      @Nonnull final String code,
      @Nonnull final String contentType,
      @Nonnull final String fileExtension) {
    this.code = code;
    this.contentType = contentType;
    this.fileExtension = fileExtension;
  }

  /**
   * Parses an explicit {@code _format} value. An export has a single explicit entry point and no
   * content negotiation, so parsing is strict.
   *
   * @param format the requested format, or null/blank for the default
   * @return the corresponding format
   * @throws ca.uhn.fhir.rest.server.exceptions.InvalidRequestException (400) {@code invalid} for
   *     {@code fhir}, {@code not-supported} for any other unrecognised value
   */
  @Nonnull
  public static SqlExportFormat fromString(@Nullable final String format) {
    if (format == null || format.isBlank()) {
      return DEFAULT_FORMAT;
    }
    // Strip any media-type parameters, so a supported media type carrying them still matches.
    final String base = format.split(";", 2)[0].trim().toLowerCase();
    if ("fhir".equals(base) || "application/fhir+json".equals(base)) {
      throw SqlOperationError.badRequest(
          IssueType.INVALID,
          FORMAT_EXPRESSION,
          "The 'fhir' format is not applicable to an export, whose result is a set of bulk data"
              + " files. Supported formats: ndjson, csv, parquet.");
    }
    return Arrays.stream(values())
        .filter(f -> f.code.equals(base) || f.contentType.equals(base))
        .findFirst()
        .orElseThrow(
            () ->
                SqlOperationError.badRequest(
                    IssueType.NOTSUPPORTED,
                    FORMAT_EXPRESSION,
                    "The format '%s' is not supported for export. Supported formats: ndjson, csv,"
                            .formatted(format)
                        + " parquet."));
  }
}
