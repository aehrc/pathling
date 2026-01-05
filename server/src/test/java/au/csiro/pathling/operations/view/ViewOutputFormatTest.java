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

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link ViewOutputFormat}.
 *
 * @author John Grimes
 */
class ViewOutputFormatTest {

  // -------------------------------------------------------------------------
  // fromString parsing tests
  // -------------------------------------------------------------------------

  @Test
  void fromStringParsesNdjsonCode() {
    assertThat(ViewOutputFormat.fromString("ndjson")).isEqualTo(ViewOutputFormat.NDJSON);
  }

  @Test
  void fromStringParsesNdjsonContentType() {
    assertThat(ViewOutputFormat.fromString("application/x-ndjson"))
        .isEqualTo(ViewOutputFormat.NDJSON);
  }

  @Test
  void fromStringParsesCsvCode() {
    assertThat(ViewOutputFormat.fromString("csv")).isEqualTo(ViewOutputFormat.CSV);
  }

  @Test
  void fromStringParsesCsvContentType() {
    assertThat(ViewOutputFormat.fromString("text/csv")).isEqualTo(ViewOutputFormat.CSV);
  }

  @Test
  void fromStringParsesJsonCode() {
    assertThat(ViewOutputFormat.fromString("json")).isEqualTo(ViewOutputFormat.JSON);
  }

  @Test
  void fromStringParsesJsonContentType() {
    assertThat(ViewOutputFormat.fromString("application/json")).isEqualTo(ViewOutputFormat.JSON);
  }

  @Test
  void fromStringDefaultsToNdjsonForUnknown() {
    assertThat(ViewOutputFormat.fromString("unknown")).isEqualTo(ViewOutputFormat.NDJSON);
  }

  @Test
  void fromStringDefaultsToNdjsonForEmptyString() {
    assertThat(ViewOutputFormat.fromString("")).isEqualTo(ViewOutputFormat.NDJSON);
  }

  @Test
  void fromStringDefaultsToNdjsonForNull() {
    assertThat(ViewOutputFormat.fromString(null)).isEqualTo(ViewOutputFormat.NDJSON);
  }

  // -------------------------------------------------------------------------
  // fromAcceptHeader parsing tests
  // -------------------------------------------------------------------------

  @Test
  void fromAcceptHeaderReturnsCsvForSingleMediaType() {
    assertThat(ViewOutputFormat.fromAcceptHeader("text/csv")).isEqualTo(ViewOutputFormat.CSV);
  }

  @Test
  void fromAcceptHeaderReturnsNdjsonForSingleMediaType() {
    assertThat(ViewOutputFormat.fromAcceptHeader("application/x-ndjson"))
        .isEqualTo(ViewOutputFormat.NDJSON);
  }

  @Test
  void fromAcceptHeaderReturnsJsonForSingleMediaType() {
    assertThat(ViewOutputFormat.fromAcceptHeader("application/json"))
        .isEqualTo(ViewOutputFormat.JSON);
  }

  @Test
  void fromAcceptHeaderMatchesFirstSupportedMediaType() {
    // First type is unsupported, second is supported.
    assertThat(ViewOutputFormat.fromAcceptHeader("text/html, text/csv"))
        .isEqualTo(ViewOutputFormat.CSV);
  }

  @Test
  void fromAcceptHeaderSelectsHighestQualityMatch() {
    // CSV has lower quality than NDJSON.
    assertThat(ViewOutputFormat.fromAcceptHeader("text/csv;q=0.5, application/x-ndjson;q=1.0"))
        .isEqualTo(ViewOutputFormat.NDJSON);
  }

  @Test
  void fromAcceptHeaderSelectsHighestQualityMatchWhenCsvPreferred() {
    // CSV has higher quality than NDJSON.
    assertThat(ViewOutputFormat.fromAcceptHeader("text/csv;q=1.0, application/x-ndjson;q=0.5"))
        .isEqualTo(ViewOutputFormat.CSV);
  }

  @Test
  void fromAcceptHeaderDefaultsQualityToOne() {
    // Media types without explicit quality default to 1.0.
    assertThat(ViewOutputFormat.fromAcceptHeader("text/csv, application/x-ndjson;q=0.5"))
        .isEqualTo(ViewOutputFormat.CSV);
  }

  @Test
  void fromAcceptHeaderReturnsNdjsonForWildcard() {
    assertThat(ViewOutputFormat.fromAcceptHeader("*/*")).isEqualTo(ViewOutputFormat.NDJSON);
  }

  @Test
  void fromAcceptHeaderReturnsNdjsonForWildcardWithOtherTypes() {
    // Wildcard should match NDJSON as the default format.
    assertThat(ViewOutputFormat.fromAcceptHeader("text/html, */*"))
        .isEqualTo(ViewOutputFormat.NDJSON);
  }

  @Test
  void fromAcceptHeaderReturnsNdjsonWhenNull() {
    assertThat(ViewOutputFormat.fromAcceptHeader(null)).isEqualTo(ViewOutputFormat.NDJSON);
  }

  @Test
  void fromAcceptHeaderReturnsNdjsonWhenEmpty() {
    assertThat(ViewOutputFormat.fromAcceptHeader("")).isEqualTo(ViewOutputFormat.NDJSON);
  }

  @Test
  void fromAcceptHeaderReturnsNdjsonWhenBlank() {
    assertThat(ViewOutputFormat.fromAcceptHeader("   ")).isEqualTo(ViewOutputFormat.NDJSON);
  }

  @Test
  void fromAcceptHeaderReturnsNdjsonForUnknownMediaType() {
    assertThat(ViewOutputFormat.fromAcceptHeader("text/html")).isEqualTo(ViewOutputFormat.NDJSON);
  }

  @Test
  void fromAcceptHeaderHandlesWhitespace() {
    assertThat(ViewOutputFormat.fromAcceptHeader("  text/csv  ;  q=1.0  "))
        .isEqualTo(ViewOutputFormat.CSV);
  }

  @Test
  void fromAcceptHeaderHandlesMediaTypeWithParameters() {
    // Content type with charset parameter should still match.
    assertThat(ViewOutputFormat.fromAcceptHeader("text/csv; charset=utf-8"))
        .isEqualTo(ViewOutputFormat.CSV);
  }

  @Test
  void fromAcceptHeaderHandlesMultipleParametersBeforeQuality() {
    assertThat(ViewOutputFormat.fromAcceptHeader("text/csv; charset=utf-8; q=0.9"))
        .isEqualTo(ViewOutputFormat.CSV);
  }

  // -------------------------------------------------------------------------
  // Content type tests
  // -------------------------------------------------------------------------

  @Test
  void ndjsonHasCorrectContentType() {
    assertThat(ViewOutputFormat.NDJSON.getContentType()).isEqualTo("application/x-ndjson");
  }

  @Test
  void csvHasCorrectContentType() {
    assertThat(ViewOutputFormat.CSV.getContentType()).isEqualTo("text/csv");
  }

  @Test
  void jsonHasCorrectContentType() {
    assertThat(ViewOutputFormat.JSON.getContentType()).isEqualTo("application/json");
  }
}
