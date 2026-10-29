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

package au.csiro.pathling.fhirpath;

import static org.assertj.core.api.Assertions.assertThat;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhirpath.evaluation.DatasetEvaluator;
import au.csiro.pathling.fhirpath.evaluation.DatasetEvaluatorBuilder;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.layout.PofDatasets;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.Objects;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Tests the type of a choice element, which is decided per row by which of its variants is present
 * (T100n).
 *
 * <p>Presence is a test of the stored field, so the type of a quantity variant is found without
 * traversing to the quantity: the plan holds none of the calls that compute its canonical form.
 *
 * @author Piotr Szul
 */
@SpringBootUnitTest
@TestInstance(Lifecycle.PER_CLASS)
class ChoiceTypeInfoTest {

  private static final Parser PARSER = new Parser();

  private static final List<String> OBSERVATIONS =
      List.of(
          "{\"resourceType\":\"Observation\",\"id\":\"q\",\"status\":\"final\","
              + "\"code\":{\"text\":\"x\"},\"valueQuantity\":{\"value\":1.5,"
              + "\"system\":\"http://unitsofmeasure.org\",\"code\":\"g\"}}",
          "{\"resourceType\":\"Observation\",\"id\":\"s\",\"status\":\"final\","
              + "\"code\":{\"text\":\"x\"},\"valueString\":\"v\"}",
          "{\"resourceType\":\"Observation\",\"id\":\"n\",\"status\":\"final\","
              + "\"code\":{\"text\":\"x\"}}");

  @Autowired SparkSession spark;

  @Autowired FhirEncoders fhirEncoders;

  @Test
  void typeOfAChoiceIsTheTypeOfThePresentVariant() {
    assertThat(evaluate("value.type().name"))
        .containsExactlyInAnyOrder("q=Quantity", "s=string", "n=null");
  }

  @Test
  void typeOfAQuantityVariantDoesNotTraverseTheQuantity() {
    final String plan = result("value.type().name").queryExecution().analyzed().toString();
    // No decode to the form the engine computes with, which computes the canonical form.
    assertThat(plan).doesNotContain("quantity_canonical");
    // The control: a comparison does decode the quantity, so the plan text names the functions.
    final String control =
        result("value.ofType(Quantity) > 1 'g'").queryExecution().analyzed().toString();
    assertThat(control).contains("quantity_canonical");
  }

  @Nonnull
  private Dataset<Row> result(@Nonnull final String expression) {
    final Dataset<Row> dataset =
        PofDatasets.fromJson(spark, fhirEncoders.getContext(), "Observation", OBSERVATIONS);
    final DatasetEvaluator evaluator =
        DatasetEvaluatorBuilder.create(ResourceType.OBSERVATION, fhirEncoders.getContext())
            .withDataset(dataset)
            .build();
    return evaluator.evaluate(PARSER.parse(expression)).toCanonical().toIdValueDataset();
  }

  @Nonnull
  private List<String> evaluate(@Nonnull final String expression) {
    return result(expression).collectAsList().stream()
        .map(row -> row.getString(0) + "=" + Objects.toString(row.get(1)))
        .toList();
  }
}
