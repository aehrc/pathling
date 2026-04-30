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
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import ca.uhn.fhir.rest.server.exceptions.InvalidRequestException;
import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Map;
import org.hl7.fhir.r4.model.Base64BinaryType;
import org.hl7.fhir.r4.model.BooleanType;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.DateTimeType;
import org.hl7.fhir.r4.model.DateType;
import org.hl7.fhir.r4.model.DecimalType;
import org.hl7.fhir.r4.model.Enumerations.PublicationStatus;
import org.hl7.fhir.r4.model.IntegerType;
import org.hl7.fhir.r4.model.Library;
import org.hl7.fhir.r4.model.ParameterDefinition.ParameterUse;
import org.hl7.fhir.r4.model.Parameters;
import org.hl7.fhir.r4.model.StringType;
import org.hl7.fhir.r4.model.TimeType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link SqlQueryRequestParser}, focused on the typed parameter binding path that
 * cross-checks runtime {@code value[x]} against {@code Library.parameter} declarations.
 */
class SqlQueryRequestParserTest {

  private static final String LIBRARY_TYPE_SYSTEM =
      "https://sql-on-fhir.org/ig/CodeSystem/LibraryTypesCodes";

  private SqlQueryRequestParser parser;

  @BeforeEach
  void setUp() {
    parser = new SqlQueryRequestParser(new SqlQueryLibraryParser());
  }

  // ---------------------------------------------------------------------------
  // Empty / null parameter inputs.
  // ---------------------------------------------------------------------------

  @Test
  void returnsEmptyBindingsWhenParametersInputIsNull() {
    final Library library = libraryWithSql("SELECT 1");
    final SqlQueryRequest request = parser.parse(library, null, null, null, null, null);
    assertThat(request.getParameterBindings()).isEmpty();
  }

  @Test
  void returnsEmptyBindingsWhenParametersInputHasNoEntries() {
    final Library library = libraryWithSql("SELECT 1");
    final SqlQueryRequest request = parser.parse(library, null, null, null, null, new Parameters());
    assertThat(request.getParameterBindings()).isEmpty();
  }

  // ---------------------------------------------------------------------------
  // Typed conversion - happy paths.
  // ---------------------------------------------------------------------------

  @Test
  void bindsIntegerParameterAsJavaInteger() {
    final Library library = libraryWithSql("SELECT * FROM t WHERE age > :min_age");
    library.addParameter().setName("min_age").setType("integer").setUse(ParameterUse.IN);
    final Parameters params = new Parameters();
    params.addParameter().setName("min_age").setValue(new IntegerType(42));

    final SqlQueryRequest request = parser.parse(library, null, null, null, null, params);

    assertThat(request.getParameterBindings()).containsExactly(Map.entry("min_age", 42));
    assertThat(request.getParameterBindings().get("min_age")).isInstanceOf(Integer.class);
  }

  @Test
  void bindsDecimalParameterAsBigDecimal() {
    final Library library = libraryWithSql("SELECT * FROM t WHERE x > :threshold");
    library.addParameter().setName("threshold").setType("decimal").setUse(ParameterUse.IN);
    final Parameters params = new Parameters();
    params.addParameter().setName("threshold").setValue(new DecimalType("3.14"));

    final SqlQueryRequest request = parser.parse(library, null, null, null, null, params);

    assertThat(request.getParameterBindings().get("threshold"))
        .isInstanceOf(BigDecimal.class)
        .isEqualTo(new BigDecimal("3.14"));
  }

  @Test
  void bindsBooleanParameterAsJavaBoolean() {
    final Library library = libraryWithSql("SELECT * FROM t WHERE active = :is_active");
    library.addParameter().setName("is_active").setType("boolean").setUse(ParameterUse.IN);
    final Parameters params = new Parameters();
    params.addParameter().setName("is_active").setValue(new BooleanType(true));

    final SqlQueryRequest request = parser.parse(library, null, null, null, null, params);

    assertThat(request.getParameterBindings().get("is_active")).isEqualTo(true);
  }

  @Test
  void bindsStringParameterAsJavaString() {
    final Library library = libraryWithSql("SELECT * FROM t WHERE label = :name");
    library.addParameter().setName("name").setType("string").setUse(ParameterUse.IN);
    final Parameters params = new Parameters();
    params.addParameter().setName("name").setValue(new StringType("alice"));

    final SqlQueryRequest request = parser.parse(library, null, null, null, null, params);

    assertThat(request.getParameterBindings().get("name")).isEqualTo("alice");
  }

  @Test
  void bindsDateParameterAsLocalDate() {
    final Library library = libraryWithSql("SELECT * FROM t WHERE birth_date >= :since");
    library.addParameter().setName("since").setType("date").setUse(ParameterUse.IN);
    final Parameters params = new Parameters();
    params.addParameter().setName("since").setValue(new DateType("1990-05-15"));

    final SqlQueryRequest request = parser.parse(library, null, null, null, null, params);

    assertThat(request.getParameterBindings().get("since"))
        .isInstanceOf(LocalDate.class)
        .isEqualTo(LocalDate.of(1990, 5, 15));
  }

  @Test
  void bindsDateTimeParameterAsInstant() {
    final Library library = libraryWithSql("SELECT * FROM t WHERE created_at >= :since");
    library.addParameter().setName("since").setType("dateTime").setUse(ParameterUse.IN);
    final Parameters params = new Parameters();
    params.addParameter().setName("since").setValue(new DateTimeType("2026-01-15T10:30:00Z"));

    final SqlQueryRequest request = parser.parse(library, null, null, null, null, params);

    assertThat(request.getParameterBindings().get("since"))
        .isInstanceOf(Instant.class)
        .isEqualTo(Instant.parse("2026-01-15T10:30:00Z"));
  }

  @Test
  void bindsTimeParameterAsLocalTime() {
    final Library library = libraryWithSql("SELECT * FROM t WHERE clock = :at");
    library.addParameter().setName("at").setType("time").setUse(ParameterUse.IN);
    final Parameters params = new Parameters();
    params.addParameter().setName("at").setValue(new TimeType("14:30:00"));

    final SqlQueryRequest request = parser.parse(library, null, null, null, null, params);

    assertThat(request.getParameterBindings().get("at"))
        .isInstanceOf(LocalTime.class)
        .isEqualTo(LocalTime.of(14, 30, 0));
  }

  @Test
  void bindsBase64BinaryParameterAsByteArray() {
    final Library library = libraryWithSql("SELECT * FROM t WHERE blob = :payload");
    library.addParameter().setName("payload").setType("base64Binary").setUse(ParameterUse.IN);
    final Parameters params = new Parameters();
    final byte[] bytes = "hello".getBytes(StandardCharsets.UTF_8);
    params.addParameter().setName("payload").setValue(new Base64BinaryType(bytes));

    final SqlQueryRequest request = parser.parse(library, null, null, null, null, params);

    assertThat(request.getParameterBindings().get("payload"))
        .isInstanceOf(byte[].class)
        .isEqualTo(bytes);
  }

  @Test
  void preservesBindingOrder() {
    final Library library = libraryWithSql("SELECT * FROM t WHERE a = :a AND b = :b AND c = :c");
    library.addParameter().setName("a").setType("integer").setUse(ParameterUse.IN);
    library.addParameter().setName("b").setType("integer").setUse(ParameterUse.IN);
    library.addParameter().setName("c").setType("integer").setUse(ParameterUse.IN);
    final Parameters params = new Parameters();
    params.addParameter().setName("c").setValue(new IntegerType(3));
    params.addParameter().setName("a").setValue(new IntegerType(1));
    params.addParameter().setName("b").setValue(new IntegerType(2));

    final SqlQueryRequest request = parser.parse(library, null, null, null, null, params);

    assertThat(request.getParameterBindings().keySet()).containsExactly("c", "a", "b");
  }

  // ---------------------------------------------------------------------------
  // Type-mismatch and undeclared-name rejections.
  // ---------------------------------------------------------------------------

  @Test
  void rejectsTypeMismatchBetweenDeclarationAndRuntimeValue() {
    final Library library = libraryWithSql("SELECT * FROM t WHERE age > :min_age");
    library.addParameter().setName("min_age").setType("integer").setUse(ParameterUse.IN);
    final Parameters params = new Parameters();
    params.addParameter().setName("min_age").setValue(new StringType("42"));

    assertThatThrownBy(() -> parser.parse(library, null, null, null, null, params))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("min_age")
        .hasMessageContaining("integer")
        .hasMessageContaining("string");
  }

  @Test
  void rejectsRuntimeParameterNotDeclaredInLibrary() {
    final Library library = libraryWithSql("SELECT 1");
    final Parameters params = new Parameters();
    params.addParameter().setName("ghost").setValue(new IntegerType(1));

    assertThatThrownBy(() -> parser.parse(library, null, null, null, null, params))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("ghost")
        .hasMessageContaining("not declared");
  }

  @Test
  void rejectsRuntimeParameterWithMissingValue() {
    final Library library = libraryWithSql("SELECT 1");
    library.addParameter().setName("min_age").setType("integer").setUse(ParameterUse.IN);
    final Parameters params = new Parameters();
    params.addParameter().setName("min_age");

    assertThatThrownBy(() -> parser.parse(library, null, null, null, null, params))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("min_age")
        .hasMessageContaining("value");
  }

  @Test
  void rejectsRuntimeParameterWithMissingName() {
    final Library library = libraryWithSql("SELECT 1");
    final Parameters params = new Parameters();
    params.addParameter().setValue(new IntegerType(1));

    assertThatThrownBy(() -> parser.parse(library, null, null, null, null, params))
        .isInstanceOf(InvalidRequestException.class)
        .hasMessageContaining("name");
  }

  /** Builds a minimal SQLQuery-typed Library carrying the given SQL. */
  private static Library libraryWithSql(final String sql) {
    final Library library = new Library();
    library.setStatus(PublicationStatus.ACTIVE);
    library.setType(
        new CodeableConcept()
            .addCoding(new Coding().setSystem(LIBRARY_TYPE_SYSTEM).setCode("sql-query")));
    library
        .addContent()
        .setContentType("application/sql")
        .setData(sql.getBytes(StandardCharsets.UTF_8));
    return library;
  }
}
