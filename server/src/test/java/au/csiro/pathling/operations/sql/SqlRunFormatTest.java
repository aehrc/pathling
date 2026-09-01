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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import jakarta.annotation.Nonnull;
import org.hl7.fhir.r4.model.OperationOutcome;
import org.hl7.fhir.r4.model.StringType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Unit tests for {@link SqlRunFormat}, covering the per-subject-kind format sets Pathling declares
 * as partial support, and the {@code _format} / {@code Accept} precedence rules from
 * contracts/sql-run.md.
 *
 * @author John Grimes
 */
class SqlRunFormatTest {

  // ---------------------------------------------------------------------------
  // Formats available for every subject kind.
  // ---------------------------------------------------------------------------

  // json, ndjson and csv are offered whatever the subject resolves to.
  @ParameterizedTest
  @ValueSource(strings = {"json", "ndjson", "csv"})
  void acceptsUniversalFormatsForEveryKind(final String code) {
    for (final SubjectKind kind : SubjectKind.values()) {
      assertThat(SqlRunFormat.select(code, null, kind).getCode()).isEqualTo(code);
    }
  }

  // parquet and fhir come from the SQL evaluation engine, so they are offered for the two SQL
  // kinds.
  @ParameterizedTest
  @CsvSource({"parquet, SQL_QUERY", "parquet, SQL_VIEW", "fhir, SQL_QUERY", "fhir, SQL_VIEW"})
  void acceptsSqlOnlyFormatsForSqlKinds(final String code, final SubjectKind kind) {
    assertThat(SqlRunFormat.select(code, null, kind).getCode()).isEqualTo(code);
  }

  // Asking for a SQL-only format with a ViewDefinition subject is Pathling's declared partial
  // support: it is rejected with not-supported naming _format, rather than quietly downgraded.
  @ParameterizedTest
  @ValueSource(strings = {"parquet", "fhir"})
  void rejectsSqlOnlyFormatsForViewDefinitionSubjects(final String code) {
    final InvalidRequestException exception =
        catchInvalidRequest(() -> SqlRunFormat.select(code, null, SubjectKind.VIEW_DEFINITION));

    assertIssue(exception, OperationOutcome.IssueType.NOTSUPPORTED, "_format");
  }

  // An unrecognised format value cannot be honoured for any kind.
  @Test
  void rejectsUnrecognisedFormat() {
    final InvalidRequestException exception =
        catchInvalidRequest(() -> SqlRunFormat.select("xlsx", null, SubjectKind.SQL_QUERY));

    assertIssue(exception, OperationOutcome.IssueType.NOTSUPPORTED, "_format");
  }

  // The error names the formats that are available, so a client can correct the request without
  // consulting the specification.
  @Test
  void rejectionNamesTheAvailableFormats() {
    assertThatThrownBy(() -> SqlRunFormat.select("parquet", null, SubjectKind.VIEW_DEFINITION))
        .hasMessageContaining("json")
        .hasMessageContaining("ndjson")
        .hasMessageContaining("csv");
  }

  // ---------------------------------------------------------------------------
  // Defaulting and precedence.
  // ---------------------------------------------------------------------------

  // With neither _format nor Accept, the response is ndjson.
  @ParameterizedTest
  @EnumSource(SubjectKind.class)
  void defaultsToNdjson(final SubjectKind kind) {
    assertThat(SqlRunFormat.select(null, null, kind)).isEqualTo(SqlRunFormat.NDJSON);
    assertThat(SqlRunFormat.select("  ", null, kind)).isEqualTo(SqlRunFormat.NDJSON);
  }

  // With no _format, the Accept header selects the format.
  @ParameterizedTest
  @CsvSource({
    "text/csv, csv",
    "application/json, json",
    "application/x-ndjson, ndjson",
    "application/vnd.apache.parquet, parquet"
  })
  void derivesFormatFromAcceptHeader(final String accept, final String expected) {
    assertThat(SqlRunFormat.select(null, accept, SubjectKind.SQL_QUERY).getCode())
        .isEqualTo(expected);
  }

  // A supplied _format overrides whatever Accept asks for.
  @Test
  void formatParameterTakesPrecedenceOverAccept() {
    assertThat(SqlRunFormat.select("csv", "application/json", SubjectKind.SQL_QUERY))
        .isEqualTo(SqlRunFormat.CSV);
  }

  // Accept negotiation respects quality values, picking the client's strongest preference.
  @Test
  void honoursAcceptQualityValues() {
    assertThat(
            SqlRunFormat.select(
                null, "text/csv;q=0.4, application/json;q=0.9", SubjectKind.SQL_QUERY))
        .isEqualTo(SqlRunFormat.JSON);
  }

  // Accept negotiation is lenient: a media type the server does not produce falls back to the
  // default rather than failing, since the client expressed only a preference.
  @Test
  void fallsBackToDefaultForUnsupportedAcceptMediaType() {
    assertThat(SqlRunFormat.select(null, "application/xml", SubjectKind.SQL_QUERY))
        .isEqualTo(SqlRunFormat.NDJSON);
    assertThat(SqlRunFormat.select(null, "*/*", SubjectKind.SQL_QUERY))
        .isEqualTo(SqlRunFormat.NDJSON);
  }

  // Accept must not smuggle in a format the subject kind does not offer; a ViewDefinition subject
  // asking for parquet via Accept falls back to the default rather than being rejected, because
  // Accept expresses a preference rather than a demand.
  @Test
  void ignoresAcceptMediaTypeUnavailableForTheKind() {
    assertThat(
            SqlRunFormat.select(
                null, "application/vnd.apache.parquet", SubjectKind.VIEW_DEFINITION))
        .isEqualTo(SqlRunFormat.NDJSON);
  }

  // A media type carrying parameters still identifies its format.
  @Test
  void stripsMediaTypeParameters() {
    assertThat(SqlRunFormat.select("text/csv;charset=utf-8", null, SubjectKind.SQL_QUERY))
        .isEqualTo(SqlRunFormat.CSV);
  }

  // ---------------------------------------------------------------------------
  // The advertised sets.
  // ---------------------------------------------------------------------------

  // The per-kind sets are what the CapabilityStatement documents, so they are asserted directly.
  @Test
  void advertisesThePerKindFormatSets() {
    assertThat(SqlRunFormat.codesFor(SubjectKind.VIEW_DEFINITION))
        .containsExactly("json", "ndjson", "csv");
    assertThat(SqlRunFormat.codesFor(SubjectKind.SQL_QUERY))
        .containsExactly("json", "ndjson", "csv", "parquet", "fhir");
    assertThat(SqlRunFormat.codesFor(SubjectKind.SQL_VIEW))
        .containsExactly("json", "ndjson", "csv", "parquet", "fhir");
  }

  // ---- helpers ----

  @Nonnull
  private static InvalidRequestException catchInvalidRequest(@Nonnull final Runnable action) {
    try {
      action.run();
    } catch (final InvalidRequestException e) {
      return e;
    }
    throw new AssertionError("Expected an InvalidRequestException to be thrown");
  }

  private static void assertIssue(
      @Nonnull final InvalidRequestException exception,
      @Nonnull final OperationOutcome.IssueType code,
      @Nonnull final String expression) {
    final OperationOutcome outcome = (OperationOutcome) exception.getOperationOutcome();
    assertThat(outcome).isNotNull();
    assertThat(outcome.getIssue())
        .anySatisfy(
            issue -> {
              assertThat(issue.getCode()).isEqualTo(code);
              assertThat(issue.getExpression())
                  .extracting(StringType::getValue)
                  .contains(expression);
            });
  }
}
