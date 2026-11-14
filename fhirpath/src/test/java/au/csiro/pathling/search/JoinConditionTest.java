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

package au.csiro.pathling.search;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.layout.PofDatasets;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.stream.Stream;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Tests a column built from a FHIRPath expression or a search used directly in a join condition
 * (T100k, `evidence/join-condition-limit.md`).
 *
 * <p>Such a column refers to the columns of the resource tolerantly, with no reference to any
 * dataset (decision 75). In a join condition it reads the column from whichever side has it, and
 * treats a column neither side has as absent, giving what joining first and then filtering gives,
 * which is the control. A name both sides have is ambiguous, as a plain reference to it is.
 *
 * @author Piotr Szul
 */
@SpringBootUnitTest
class JoinConditionTest {

  private static final List<String> PATIENTS =
      List.of(
          "{\"resourceType\":\"Patient\",\"id\":\"1\",\"gender\":\"male\","
              + "\"birthDate\":\"1940-01-01\",\"name\":[{\"family\":\"A\"}]}",
          "{\"resourceType\":\"Patient\",\"id\":\"2\",\"gender\":\"female\","
              + "\"birthDate\":\"1960-01-01\"}",
          "{\"resourceType\":\"Patient\",\"id\":\"3\",\"gender\":\"male\","
              + "\"birthDate\":\"1970-01-01\",\"name\":[{\"family\":\"C\"}]}");

  private static final List<String> ENCOUNTERS =
      List.of(
          encounter("e1", "1"), encounter("e2", "1"), encounter("e3", "2"), encounter("e4", "3"));

  @Autowired SparkSession spark;

  @Autowired FhirEncoders encoders;

  @Nonnull
  private static String encounter(@Nonnull final String id, @Nonnull final String patient) {
    return "{\"resourceType\":\"Encounter\",\"id\":\""
        + id
        + "\",\"status\":\"finished\",\"class\":{\"code\":\"AMB\"},"
        + "\"subject\":{\"reference\":\"Patient/"
        + patient
        + "\"}}";
  }

  @Nonnull
  static Stream<Arguments> cases() {
    return Stream.of(
        // An element that is present, of the patient side alone.
        arguments("gender = 'male'", 3L),
        arguments("name.family.exists()", 3L),
        arguments("birthDate > @1950-01-01", 2L),
        // An element absent from the data, which neither side has.
        arguments("deceasedBoolean.empty()", 4L),
        arguments("photo.exists()", 0L),
        // An expression that reads no column.
        arguments("true", 4L));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("cases")
  void expressionInJoinConditionMatchesJoinThenFilter(
      @Nonnull final String expression, final long expected) {
    final Dataset<Row> patients = patients();
    final Dataset<Row> encounters = encounters();
    final Column column = builder().fromExpression(ResourceType.PATIENT, expression);
    final Column key = key(encounters, patients);

    assertThat(encounters.join(patients, column.and(key)).count()).isEqualTo(expected);
    // The control: joining first and then filtering.
    assertThat(encounters.join(patients, key).filter(column).count()).isEqualTo(expected);
  }

  @Test
  void searchInJoinConditionMatchesJoinThenFilter() {
    final Dataset<Row> patients = patients();
    final Dataset<Row> encounters = encounters();
    final Column search = builder().fromQueryString(ResourceType.PATIENT, "gender=male");
    final Column key = key(encounters, patients);

    assertThat(encounters.join(patients, search.and(key)).count()).isEqualTo(3L);
    assertThat(encounters.join(patients, key).filter(search).count()).isEqualTo(3L);
  }

  @Test
  void nameOnBothSidesIsAmbiguous() {
    final Dataset<Row> patients = patients();
    final Dataset<Row> encounters = encounters();
    final Column column = builder().fromExpression(ResourceType.PATIENT, "id.exists()");
    final Column key = key(encounters, patients);

    // The control: a plain reference to the name is ambiguous.
    assertAmbiguous(() -> encounters.join(patients, functions.col("id").isNotNull()).count());
    assertAmbiguous(() -> encounters.join(patients, column.and(key)).count());
  }

  @Nonnull
  private SearchColumnBuilder builder() {
    return SearchColumnBuilder.withDefaultRegistry(encoders.getContext());
  }

  @Nonnull
  private Dataset<Row> patients() {
    return PofDatasets.fromJson(spark, encoders.getContext(), "Patient", PATIENTS);
  }

  /** The encounters, keeping only their id, status and subject, as a caller selecting would. */
  @Nonnull
  private Dataset<Row> encounters() {
    return PofDatasets.fromJson(spark, encoders.getContext(), "Encounter", ENCOUNTERS)
        .select("id", "status", "subject");
  }

  @Nonnull
  private static Column key(
      @Nonnull final Dataset<Row> encounters, @Nonnull final Dataset<Row> patients) {
    return encounters
        .col("subject.reference")
        .equalTo(functions.concat(functions.lit("Patient/"), patients.col("id")));
  }

  private static void assertAmbiguous(@Nonnull final Runnable action) {
    assertThatThrownBy(action::run)
        .isInstanceOf(AnalysisException.class)
        .extracting(error -> ((AnalysisException) error).getCondition())
        .isEqualTo("AMBIGUOUS_REFERENCE");
  }
}
