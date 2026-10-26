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
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import au.csiro.pathling.schema.LayoutFields;
import au.csiro.pathling.schema.SchemaConfiguration;
import au.csiro.pathling.schema.annotation.kinds.QuantityCanonicalKind;
import jakarta.annotation.Nonnull;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

/**
 * Tests the canonical annotation the annotation step stores in a quantity (T054, FR-005, decision
 * 137): it is present, at its type and at the end of the quantity, and it keeps the magnitude
 * canonicalisation shifts.
 */
class QuantityCanonicalAnnotationTest {

  private static final String CANONICAL_EXACT = LayoutFields.CANONICAL_EXACT_ANNOTATION;

  private static final String UCUM = "http://unitsofmeasure.org";

  @Nonnull
  private static Dataset<Row> annotate(
      @Nonnull final SchemaConfiguration configuration, @Nonnull final String... observations) {
    return Annotator.of(DEFINITIONS, configuration)
        .annotate(
            "Observation",
            TransformFixtures.reader()
                .read(
                    "Observation",
                    TransformFixtures.spark()
                        .createDataset(Arrays.asList(observations), Encoders.STRING())));
  }

  @Nonnull
  private static String observation(
      @Nonnull final String id, @Nonnull final String quantity, @Nonnull final String rest) {
    return "{\"resourceType\":\"Observation\",\"id\":\""
        + id
        + "\",\"status\":\"final\",\"code\":{\"text\":\"x\"},\"valueQuantity\":"
        + quantity
        + rest
        + "}";
  }

  @Nonnull
  private static String ucum(@Nonnull final String value, @Nonnull final String code) {
    return "{\"value\":" + value + ",\"system\":\"" + UCUM + "\",\"code\":\"" + code + "\"}";
  }

  @Nonnull
  private static StructType structure(@Nonnull final DataType type) {
    return (StructType) (type instanceof final ArrayType array ? array.elementType() : type);
  }

  @Nonnull
  private static BigDecimal exactValue(@Nonnull final Row exact) {
    final Row value = exact.getStruct(0);
    return value.getDecimal(0).movePointLeft(value.getInt(1));
  }

  @Test
  void aQuantityCarriesTheAnnotationLast() {
    final Dataset<Row> annotated =
        annotate(
            SchemaConfiguration.builder().build(),
            observation(
                "o1",
                ucum("1.5", "kg"),
                ",\"component\":[{\"code\":{\"text\":\"a\"},\"valueQuantity\":"
                    + ucum("500", "mg")
                    + "}]"));

    final StructType root = structure(annotated.schema().apply("valueQuantity").dataType());
    final StructType component =
        structure(
            structure(annotated.schema().apply("component").dataType())
                .apply("valueQuantity")
                .dataType());
    for (final StructType quantity : List.of(root, component)) {
      final StructField last = quantity.fields()[quantity.fields().length - 1];
      assertEquals(CANONICAL_EXACT, last.name());
      assertEquals(QuantityCanonicalKind.CANONICAL_EXACT_TYPE, last.dataType());
      assertFalse(Arrays.asList(quantity.fieldNames()).contains("__canonical"));
    }
    final Row exact =
        annotated
            .selectExpr(
                "valueQuantity.__canonical_exact", "component[0].valueQuantity.__canonical_exact")
            .first();
    assertEquals(0, new BigDecimal("1500").compareTo(exactValue(exact.getStruct(0))));
    assertEquals("g", exact.getStruct(0).getString(1));
    assertEquals(0, new BigDecimal("0.5").compareTo(exactValue(exact.getStruct(1))));
  }

  @Test
  void quantitiesOrdersOfMagnitudeApartDoNotCompareEqual() {
    // A nanogram and a hundred of them would both be zero grams at six fractional digits.
    final List<Row> rows =
        annotate(
                SchemaConfiguration.builder().build(),
                observation("o1", ucum("1", "ng"), ""),
                observation("o2", ucum("100", "ng"), ""))
            .orderBy("id")
            .selectExpr("valueQuantity.__canonical_exact")
            .collectAsList();

    assertNotEquals(rows.get(0).getStruct(0), rows.get(1).getStruct(0));
    assertEquals(0, new BigDecimal("1E-9").compareTo(exactValue(rows.get(0).getStruct(0))));
    assertEquals(0, new BigDecimal("1E-7").compareTo(exactValue(rows.get(1).getStruct(0))));
    assertEquals("g", rows.get(0).getStruct(0).getString(1));
  }

  @Test
  void aQuantityWithoutACodeIsAnnotatedWithNull() {
    // The schema is fitted to the data, so this quantity carries neither a system nor a code.
    final Row annotations =
        annotate(
                SchemaConfiguration.builder().build(),
                observation("o1", "{\"value\":3,\"unit\":\"tablets\"}", ""))
            .selectExpr("valueQuantity.__canonical_exact")
            .first();

    assertNull(annotations.get(0));
  }

  @Test
  void theSwitchLeavesOutTheAnnotation() {
    final Dataset<Row> annotated =
        annotate(
            SchemaConfiguration.builder().enableCanonicalAnnotation(false).build(),
            observation("o1", ucum("1.5", "kg"), ""));

    final List<String> fields =
        Arrays.asList(structure(annotated.schema().apply("valueQuantity").dataType()).fieldNames());
    assertFalse(fields.contains(CANONICAL_EXACT));
  }
}
