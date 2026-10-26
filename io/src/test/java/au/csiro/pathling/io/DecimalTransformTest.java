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
import static org.junit.jupiter.api.Assertions.assertNull;

import au.csiro.pathling.schema.LayoutFields;
import au.csiro.pathling.schema.SchemaConfiguration;
import au.csiro.pathling.utilities.SystemDecimal;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

/**
 * Tests the numeric annotation the annotation step stores beside a decimal (T050a, FR-002, decision
 * 135): it is the last field of the decimal's metadata group, at the engine's System.Decimal type,
 * and holds what decoding the stored text gives, while the text stays the stored value.
 */
class DecimalTransformTest {

  private static final String NUMERIC = LayoutFields.NUMERIC_ANNOTATION;

  @Nonnull private static final ObjectMapper MAPPER = new ObjectMapper();

  @Nonnull
  private static final String LOCATION =
      "{\"resourceType\":\"Location\",\"id\":\"l1\","
          + "\"position\":{\"latitude\":0.1234567,\"longitude\":-2.25}}";

  @Nonnull
  private static final String RISK_ASSESSMENT =
      "{\"resourceType\":\"RiskAssessment\",\"id\":\"r1\",\"status\":\"final\","
          + "\"subject\":{\"reference\":\"Patient/p\"},"
          + "\"prediction\":[{\"probabilityDecimal\":0.25},{\"probabilityDecimal\":0.75}]}";

  @Nonnull
  private static final String CHARGE_ITEM =
      "{\"resourceType\":\"ChargeItem\",\"id\":\"c1\",\"status\":\"billable\","
          + "\"code\":{\"text\":\"x\"},\"subject\":{\"reference\":\"Patient/p\"},"
          + "\"factorOverride\":12345678901234567890}";

  @Nonnull
  private static final String OBSERVATION =
      "{\"resourceType\":\"Observation\",\"id\":\"o1\",\"status\":\"final\","
          + "\"code\":{\"text\":\"x\"},"
          + "\"valueQuantity\":{\"value\":1.5,\"system\":\"http://unitsofmeasure.org\","
          + "\"code\":\"kg\"}}";

  @Nonnull
  private static Dataset<Row> read(@Nonnull final String resourceType, @Nonnull final String json) {
    return TransformFixtures.reader()
        .read(
            resourceType,
            TransformFixtures.spark().createDataset(List.of(json), Encoders.STRING()));
  }

  @Nonnull
  private static Dataset<Row> annotate(
      @Nonnull final SchemaConfiguration configuration,
      @Nonnull final String resourceType,
      @Nonnull final String json) {
    return Annotator.of(DEFINITIONS, configuration)
        .annotate(resourceType, read(resourceType, json));
  }

  @Nonnull
  private static Dataset<Row> annotate(
      @Nonnull final String resourceType, @Nonnull final String json) {
    return annotate(SchemaConfiguration.builder().build(), resourceType, json);
  }

  @Nonnull
  private static StructType structure(@Nonnull final DataType type) {
    return (StructType) (type instanceof final ArrayType array ? array.elementType() : type);
  }

  /** Asserts that a metadata group carries the annotation last, at the System.Decimal type. */
  private static void assertCarriesTheAnnotationLast(@Nonnull final DataType group) {
    final StructField[] fields = structure(group).fields();
    final StructField last = fields[fields.length - 1];
    assertEquals(NUMERIC, last.name());
    assertEquals(SystemDecimal.TYPE, last.dataType());
  }

  @Test
  void aDecimalAtTheRootCarriesTheAnnotationInItsMetadataGroup() {
    final Dataset<Row> annotated = annotate("ChargeItem", CHARGE_ITEM);

    final List<String> columns = Arrays.asList(annotated.columns());
    assertEquals(columns.indexOf("factorOverride") + 1, columns.indexOf("_factorOverride"));
    assertEquals(DataTypes.StringType, annotated.schema().apply("factorOverride").dataType());
    assertCarriesTheAnnotationLast(annotated.schema().apply("_factorOverride").dataType());
    final Row row = annotated.selectExpr("factorOverride", "_factorOverride.__numeric").first();
    assertEquals("12345678901234567890", row.getString(0));
    assertEquals(new BigDecimal("12345678901234567890.000000"), row.getDecimal(1));
  }

  @Test
  void aDecimalInAStructureCarriesTheAnnotationAsDecodingGivesIt() {
    final Dataset<Row> annotated = annotate("Location", LOCATION);

    final StructType position = structure(annotated.schema().apply("position").dataType());
    assertCarriesTheAnnotationLast(position.apply("_latitude").dataType());
    assertCarriesTheAnnotationLast(position.apply("_longitude").dataType());
    // The text keeps every digit, and the annotation is rounded to six, as decoding rounds it.
    final Row row =
        annotated
            .selectExpr(
                "position.latitude",
                "position._latitude.__numeric",
                "position._longitude.__numeric")
            .first();
    assertEquals("0.1234567", row.getString(0));
    assertEquals(new BigDecimal("0.123457"), row.getDecimal(1));
    assertEquals(new BigDecimal("-2.250000"), row.getDecimal(2));
  }

  @Test
  void aDecimalUnderARepeatingParentCarriesTheAnnotationAtEachPosition() {
    final Dataset<Row> annotated = annotate("RiskAssessment", RISK_ASSESSMENT);

    final StructType prediction = structure(annotated.schema().apply("prediction").dataType());
    assertCarriesTheAnnotationLast(prediction.apply("_probabilityDecimal").dataType());
    final Row row =
        annotated
            .selectExpr(
                "prediction[0]._probabilityDecimal.__numeric",
                "prediction[1]._probabilityDecimal.__numeric")
            .first();
    assertEquals(new BigDecimal("0.250000"), row.getDecimal(0));
    assertEquals(new BigDecimal("0.750000"), row.getDecimal(1));
  }

  @Test
  void theValueOfAQuantityCarriesTheAnnotation() {
    final Dataset<Row> annotated = annotate("Observation", OBSERVATION);

    final StructType quantity = structure(annotated.schema().apply("valueQuantity").dataType());
    assertCarriesTheAnnotationLast(quantity.apply("_value").dataType());
    final StructField[] fields = quantity.fields();
    assertEquals(LayoutFields.CANONICAL_EXACT_ANNOTATION, fields[fields.length - 1].name());
    assertEquals(
        new BigDecimal("1.500000"),
        annotated.selectExpr("valueQuantity._value.__numeric").first().getDecimal(0));
  }

  @Test
  void aValueBeyondTheRangeOfTheTypeIsAnnotatedWithNull() {
    final Dataset<Row> annotated =
        annotate(
            "ChargeItem",
            CHARGE_ITEM.replace("12345678901234567890", "123456789012345678901234567"));

    final Row row = annotated.selectExpr("factorOverride", "_factorOverride.__numeric").first();
    assertEquals("123456789012345678901234567", row.getString(0));
    assertNull(row.get(1));
  }

  @Test
  void theSwitchLeavesOutTheAnnotation() {
    final Dataset<Row> annotated =
        annotate(
            SchemaConfiguration.builder().enableNumericAnnotation(false).build(),
            "Location",
            LOCATION);

    final List<String> fields =
        Arrays.asList(structure(annotated.schema().apply("position").dataType()).fieldNames());
    assertFalse(fields.contains("_latitude"));
    assertFalse(fields.contains("_longitude"));
  }

  @Test
  void egressWritesTheTextWithoutTheAnnotation() throws IOException {
    final String written =
        TransformFixtures.fhirWriter()
            .json()
            .write("Location", annotate("Location", LOCATION))
            .first();

    assertEquals(MAPPER.readTree(LOCATION), MAPPER.readTree(written));
  }
}
