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
import au.csiro.pathling.test.layout.LayoutDatasets;
import au.csiro.pathling.test.layout.TestLayout;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Tests that a column built from a FHIRPath expression or a search, applied to a dataset in which a
 * column it references is ambiguous, fails as a plain reference to that column does.
 *
 * <p>A column built without reference to any dataset refers to a column of the resource tolerantly,
 * and answers a null where the column is absent (decision 75). After a self-join every column of
 * the resource is present twice, and the reference must report the ambiguity rather than take it
 * for an absence, which would filter out every row and select nulls without an error.
 *
 * @author Piotr Szul
 */
@SpringBootUnitTest
class SelfJoinAmbiguityTest {

  private static final List<String> PATIENTS =
      List.of(
          "{\"resourceType\":\"Patient\",\"id\":\"1\",\"gender\":\"male\","
              + "\"name\":[{\"family\":\"A\"}]}",
          "{\"resourceType\":\"Patient\",\"id\":\"2\",\"gender\":\"female\","
              + "\"name\":[{\"family\":\"B\"}]}");

  @Autowired SparkSession spark;

  @Autowired FhirEncoders encoders;

  @Nonnull
  static Stream<Arguments> cases() {
    final Function<Dataset<Row>, Dataset<Row>> aliased =
        ds -> ds.as("l").join(ds.as("r"), functions.col("l.id").equalTo(functions.col("r.id")));
    final Function<Dataset<Row>, Dataset<Row>> using = ds -> ds.join(ds, "id");
    return Stream.of(TestLayout.PREVIOUS, TestLayout.POF)
        .flatMap(
            layout ->
                Stream.of(
                    arguments(layout, "an aliased self-join", aliased),
                    arguments(layout, "a self-join on the id", using)));
  }

  @ParameterizedTest(name = "{1} over the {0} layout")
  @MethodSource("cases")
  void referenceToAnAmbiguousColumnFails(
      @Nonnull final TestLayout layout,
      @Nonnull final String ignoredName,
      @Nonnull final Function<Dataset<Row>, Dataset<Row>> selfJoin) {
    final Dataset<Row> patients =
        LayoutDatasets.fromJson(spark, encoders, layout, "Patient", PATIENTS);
    final SearchColumnBuilder builder =
        SearchColumnBuilder.withDefaultRegistry(encoders.getContext());
    final Column male = builder.fromExpression(ResourceType.PATIENT, "gender = 'male'");
    final Column search = builder.fromQueryString(ResourceType.PATIENT, "gender=male");
    final Column family = builder.fromExpression(ResourceType.PATIENT, "name.family.first()");

    // Each column is valid over the dataset itself.
    assertThat(patients.filter(male).count()).isEqualTo(1);
    assertThat(patients.filter(search).count()).isEqualTo(1);

    final Dataset<Row> joined = selfJoin.apply(patients);
    // The control: a plain reference to a duplicated column is ambiguous.
    assertAmbiguous(() -> joined.select("gender").collect());
    assertAmbiguous(() -> joined.filter(male).count());
    assertAmbiguous(() -> joined.filter(search).count());
    assertAmbiguous(() -> joined.select(family).collect());
  }

  private static void assertAmbiguous(@Nonnull final Runnable action) {
    assertThatThrownBy(action::run)
        .isInstanceOf(AnalysisException.class)
        .extracting(error -> ((AnalysisException) error).getCondition())
        .isEqualTo("AMBIGUOUS_REFERENCE");
  }
}
