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

package au.csiro.pathling.io;

import static au.csiro.pathling.io.TransformFixtures.DEFINITIONS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.schema.SchemaConfiguration;
import jakarta.annotation.Nonnull;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.Instant;
import java.util.List;
import java.util.regex.Pattern;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests that ingest writes each annotation as one flat field at every level an element can be
 * stored at (FR-004, FR-005, decisions 145 and 149): a quantity at the root, under a repeating
 * parent and inside an extension, and a repeating date under a repeating parent.
 *
 * <p>Each level is annotated over a fresh schema, which carries no annotations, and over data that
 * already carries them, which the step keeps as they are rather than computing them again.
 */
class FlatAnnotationLevelsTest {

  private static final String UCUM = "http://unitsofmeasure.org";

  /**
   * Each position holds a different quantity with a different canonical form, so that an annotation
   * taken from the wrong level or index fails.
   */
  @Nonnull
  private static String quantity(@Nonnull final String value, @Nonnull final String code) {
    return "{\"value\":"
        + value
        + ",\"unit\":\""
        + code
        + "\",\"system\":\""
        + UCUM
        + "\",\"code\":\""
        + code
        + "\"}";
  }

  private static final String OBSERVATION =
      "{\"resourceType\":\"Observation\",\"id\":\"o1\",\"status\":\"final\","
          + "\"code\":{\"text\":\"x\"},"
          + "\"extension\":[{\"url\":\"http://example.org/weight\",\"valueQuantity\":"
          + quantity("2", "mg")
          + "}],"
          + "\"valueQuantity\":"
          + quantity("1.5", "kg")
          + ",\"component\":[{\"code\":{\"text\":\"a\"},\"valueQuantity\":"
          + quantity("3", "cm")
          + "},{\"code\":{\"text\":\"b\"},\"valueQuantity\":"
          + quantity("4", "min")
          + "}]}";

  private static final String MEDICATION_REQUEST =
      "{\"resourceType\":\"MedicationRequest\",\"id\":\"m1\",\"status\":\"active\","
          + "\"intent\":\"order\",\"subject\":{\"reference\":\"Patient/p\"},"
          + "\"medicationCodeableConcept\":{\"text\":\"x\"},"
          + "\"dosageInstruction\":[{\"timing\":{\"event\":[\"2020\",\"2021-02\"]}},"
          + "{\"timing\":{\"event\":[\"2022-03-04\"]}}]}";

  /** Any computation of an annotation of a quantity or a date. */
  private static final Pattern COMPUTATION =
      Pattern.compile("quantity_canonical_(code|value_exact)\\(|date_(start|end)\\(");

  @Nonnull
  private static Dataset<Row> read(@Nonnull final String resourceType, @Nonnull final String json) {
    return TransformFixtures.reader()
        .read(
            resourceType,
            TransformFixtures.spark().createDataset(List.of(json), Encoders.STRING()));
  }

  @Nonnull
  private static Dataset<Row> annotate(
      @Nonnull final String resourceType, @Nonnull final Dataset<Row> resources) {
    return Annotator.of(DEFINITIONS, SchemaConfiguration.builder().build())
        .annotate(resourceType, resources);
  }

  /**
   * Annotates a resource over a fresh schema, or over the schema and data of a resource already
   * annotated, in which case the annotations are kept and nothing is computed.
   */
  @Nonnull
  private static Dataset<Row> annotated(
      @Nonnull final String resourceType, @Nonnull final String json, @Nonnull final String input) {
    final Dataset<Row> once = annotate(resourceType, read(resourceType, json));
    if ("fresh".equals(input)) {
      assertTrue(COMPUTATION.matcher(plan(once)).find(), plan(once));
      return once;
    }
    final Dataset<Row> twice = annotate(resourceType, once.localCheckpoint());
    assertFalse(COMPUTATION.matcher(plan(twice)).find(), plan(twice));
    assertEquals(once.schema(), twice.schema());
    return twice;
  }

  @Nonnull
  private static String plan(@Nonnull final Dataset<Row> dataset) {
    return dataset.queryExecution().optimizedPlan().toString();
  }

  /** Asserts the flat canonical fields of one quantity. */
  private static void assertCanonical(
      @Nonnull final Dataset<Row> annotated,
      @Nonnull final String quantity,
      @Nonnull final String code,
      @Nonnull final String value) {
    final Row row =
        annotated
            .selectExpr(
                quantity + ".__canonical_code",
                quantity + ".__canonical_value_exact.value",
                quantity + ".__canonical_value_exact.scale")
            .first();
    assertEquals(code, row.getString(0));
    assertEquals(
        0, new BigDecimal(value).compareTo(row.getDecimal(1).movePointLeft(row.getInt(2))));
    assertTrue(row.getInt(2) >= 0);
  }

  @ParameterizedTest
  @ValueSource(strings = {"fresh", "annotated"})
  void aQuantityAtTheRoot(@Nonnull final String input) {
    // 1.5 kg is 1500 g.
    assertCanonical(annotated("Observation", OBSERVATION, input), "valueQuantity", "g", "1500");
  }

  @ParameterizedTest
  @ValueSource(strings = {"fresh", "annotated"})
  void aQuantityUnderARepeatingParent(@Nonnull final String input) {
    final Dataset<Row> annotated = annotated("Observation", OBSERVATION, input);
    // 3 cm is 0.03 m, and 4 min is 240 s.
    assertCanonical(annotated, "component[0].valueQuantity", "m", "0.03");
    assertCanonical(annotated, "component[1].valueQuantity", "s", "240");
  }

  @ParameterizedTest
  @ValueSource(strings = {"fresh", "annotated"})
  void aQuantityInsideAnExtension(@Nonnull final String input) {
    // 2 mg is 0.002 g.
    assertCanonical(
        annotated("Observation", OBSERVATION, input), "extension[0].valueQuantity", "g", "0.002");
  }

  @ParameterizedTest
  @ValueSource(strings = {"fresh", "annotated"})
  void aRepeatingDateUnderARepeatingParent(@Nonnull final String input) {
    final Row row =
        annotated("MedicationRequest", MEDICATION_REQUEST, input)
            .selectExpr(
                "dosageInstruction[0].timing._event[0].__start",
                "dosageInstruction[0].timing._event[0].__end",
                "dosageInstruction[0].timing._event[1].__start",
                "dosageInstruction[0].timing._event[1].__end",
                "dosageInstruction[1].timing._event[0].__start",
                "dosageInstruction[1].timing._event[0].__end")
            .first();
    assertEquals(instant("2020-01-01T00:00:00Z"), row.getTimestamp(0).toInstant());
    assertEquals(instant("2020-12-31T23:59:59.999999Z"), row.getTimestamp(1).toInstant());
    assertEquals(instant("2021-02-01T00:00:00Z"), row.getTimestamp(2).toInstant());
    assertEquals(instant("2021-02-28T23:59:59.999999Z"), row.getTimestamp(3).toInstant());
    assertEquals(instant("2022-03-04T00:00:00Z"), row.getTimestamp(4).toInstant());
    assertEquals(instant("2022-03-04T23:59:59.999999Z"), row.getTimestamp(5).toInstant());
  }

  @Nonnull
  private static Instant instant(@Nonnull final String text) {
    return Timestamp.from(Instant.parse(text)).toInstant();
  }
}
