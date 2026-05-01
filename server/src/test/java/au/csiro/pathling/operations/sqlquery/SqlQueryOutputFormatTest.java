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

package au.csiro.pathling.operations.sqlquery;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests for {@link SqlQueryOutputFormat} covering both the {@code _format} string and the {@code
 * Accept} header parsing paths, including the q-value precedence used in content negotiation.
 */
class SqlQueryOutputFormatTest {

  @ParameterizedTest(name = "fromString(\"{0}\") returns {1}")
  @CsvSource({
    "ndjson, NDJSON",
    "csv, CSV",
    "json, JSON",
    "parquet, PARQUET",
    "fhir, FHIR",
    "application/x-ndjson, NDJSON",
    "text/csv, CSV",
    "application/json, JSON",
    "application/vnd.apache.parquet, PARQUET",
    "application/fhir+json, FHIR"
  })
  void fromStringMatchesCodeOrContentType(final String input, final SqlQueryOutputFormat expected) {
    assertThat(SqlQueryOutputFormat.fromString(input)).isEqualTo(expected);
  }

  @ParameterizedTest(name = "fromString(\"{0}\") normalises and matches NDJSON")
  @CsvSource({"NDJSON", "  ndjson  ", "Application/X-NDJSON"})
  void fromStringIsCaseInsensitiveAndTrims(final String input) {
    assertThat(SqlQueryOutputFormat.fromString(input)).isEqualTo(SqlQueryOutputFormat.NDJSON);
  }

  @Test
  void fromStringDefaultsToNdjsonForNull() {
    assertThat(SqlQueryOutputFormat.fromString(null)).isEqualTo(SqlQueryOutputFormat.NDJSON);
  }

  @ParameterizedTest
  @ValueSource(strings = {"", "   "})
  void fromStringDefaultsToNdjsonForBlank(final String input) {
    assertThat(SqlQueryOutputFormat.fromString(input)).isEqualTo(SqlQueryOutputFormat.NDJSON);
  }

  @Test
  void fromStringDefaultsToNdjsonForUnknown() {
    assertThat(SqlQueryOutputFormat.fromString("text/xml")).isEqualTo(SqlQueryOutputFormat.NDJSON);
  }

  @Test
  void fromAcceptHeaderDefaultsToNdjsonForNull() {
    assertThat(SqlQueryOutputFormat.fromAcceptHeader(null)).isEqualTo(SqlQueryOutputFormat.NDJSON);
  }

  @Test
  void fromAcceptHeaderDefaultsToNdjsonForBlank() {
    assertThat(SqlQueryOutputFormat.fromAcceptHeader("")).isEqualTo(SqlQueryOutputFormat.NDJSON);
  }

  @Test
  void fromAcceptHeaderResolvesSingleSupportedType() {
    assertThat(SqlQueryOutputFormat.fromAcceptHeader("text/csv"))
        .isEqualTo(SqlQueryOutputFormat.CSV);
  }

  @Test
  void fromAcceptHeaderPicksHigherQualityWinner() {
    assertThat(
            SqlQueryOutputFormat.fromAcceptHeader(
                "text/csv;q=0.5, application/json;q=0.9, application/x-ndjson;q=1.0"))
        .isEqualTo(SqlQueryOutputFormat.NDJSON);
  }

  @Test
  void fromAcceptHeaderDefaultsQualityToOneWhenAbsent() {
    assertThat(SqlQueryOutputFormat.fromAcceptHeader("text/csv, application/json;q=0.5"))
        .isEqualTo(SqlQueryOutputFormat.CSV);
  }

  @Test
  void fromAcceptHeaderTreatsWildcardAsDefault() {
    assertThat(SqlQueryOutputFormat.fromAcceptHeader("*/*")).isEqualTo(SqlQueryOutputFormat.NDJSON);
  }

  @Test
  void fromAcceptHeaderSkipsUnsupportedTypes() {
    assertThat(SqlQueryOutputFormat.fromAcceptHeader("text/xml, text/csv"))
        .isEqualTo(SqlQueryOutputFormat.CSV);
  }

  @Test
  void fromAcceptHeaderDefaultsWhenAllUnsupported() {
    assertThat(SqlQueryOutputFormat.fromAcceptHeader("text/xml, image/png"))
        .isEqualTo(SqlQueryOutputFormat.NDJSON);
  }

  @Test
  void fromAcceptHeaderTreatsMalformedQValueAsOne() {
    assertThat(SqlQueryOutputFormat.fromAcceptHeader("text/csv;q=abc, application/json;q=0.5"))
        .isEqualTo(SqlQueryOutputFormat.CSV);
  }

  @Test
  void fromAcceptHeaderHandlesParametersOtherThanQ() {
    assertThat(SqlQueryOutputFormat.fromAcceptHeader("text/csv;charset=utf-8"))
        .isEqualTo(SqlQueryOutputFormat.CSV);
  }

  @Test
  void getCodeAndContentTypeAreExposed() {
    assertThat(SqlQueryOutputFormat.NDJSON.getCode()).isEqualTo("ndjson");
    assertThat(SqlQueryOutputFormat.NDJSON.getContentType()).isEqualTo("application/x-ndjson");
    assertThat(SqlQueryOutputFormat.PARQUET.getContentType())
        .isEqualTo("application/vnd.apache.parquet");
  }
}
