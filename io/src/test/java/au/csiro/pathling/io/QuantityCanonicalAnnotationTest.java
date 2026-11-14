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
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;

/**
 * Tests the canonical annotations the annotation step stores in a quantity (T054, FR-005, decisions
 * 137 and 149): they are present, flat, at their types and at the end of the quantity, and they
 * keep the magnitude canonicalisation shifts.
 */
class QuantityCanonicalAnnotationTest {

  private static final String CANONICAL_CODE = LayoutFields.CANONICAL_CODE_ANNOTATION;

  private static final String CANONICAL_VALUE_EXACT = LayoutFields.CANONICAL_VALUE_EXACT_ANNOTATION;

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
  private static BigDecimal exactValue(@Nonnull final Row value) {
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
      final StructField[] fields = quantity.fields();
      final StructField code = fields[fields.length - 2];
      final StructField value = fields[fields.length - 1];
      assertEquals(CANONICAL_CODE, code.name());
      assertEquals(DataTypes.StringType, code.dataType());
      assertEquals(CANONICAL_VALUE_EXACT, value.name());
      assertEquals(QuantityCanonicalKind.EXACT_VALUE_TYPE, value.dataType());
      assertFalse(Arrays.asList(quantity.fieldNames()).contains("__canonical"));
      assertFalse(Arrays.asList(quantity.fieldNames()).contains("__canonical_exact"));
    }
    final Row exact =
        annotated
            .selectExpr(
                "valueQuantity.__canonical_value_exact",
                "valueQuantity.__canonical_code",
                "component[0].valueQuantity.__canonical_value_exact")
            .first();
    assertEquals(0, new BigDecimal("1500").compareTo(exactValue(exact.getStruct(0))));
    assertEquals("g", exact.getString(1));
    assertEquals(0, new BigDecimal("0.5").compareTo(exactValue(exact.getStruct(2))));
  }

  @Test
  void aRepeatingQuantityIsCanonicalisedOncePerQuantity() {
    // Both canonical annotations of each component's quantity share one memoised
    // canonicalisation. The values are distinct, and distinct from those of every other test, so
    // that no canonicalisation is spared by one cached before.
    final int components = 50;
    final String component =
        IntStream.range(0, components)
            .mapToObj(
                i ->
                    "{\"code\":{\"text\":\"c"
                        + i
                        + "\"},\"valueQuantity\":"
                        + ucum("987654." + (100 + i), "mg")
                        + "}")
            .collect(Collectors.joining(","));
    final Dataset<Row> annotated =
        annotate(
            SchemaConfiguration.builder().build(),
            observation("o1", ucum("1", "g"), ",\"component\":[" + component + "]"));
    final long before = QuantityCanonicalKind.getCanonicalisationCount();
    annotated.select("component").collectAsList();
    assertEquals(components, QuantityCanonicalKind.getCanonicalisationCount() - before);
  }

  @Test
  void aQuantityWithoutASystemHasNoCanonicalFormThroughTheStructure() {
    // The kind applied to a quantity's structure, as AnnotationKind.apply does by default, reads
    // its fields by name and takes one the structure lacks as null, rather than failing.
    final Dataset<Row> quantities =
        TransformFixtures.spark()
            .createDataset(List.of("1.5"), Encoders.STRING())
            .select(
                functions
                    .struct(
                        functions.col("value").alias("value"), functions.lit("kg").alias("code"))
                    .alias("q"));
    final QuantityCanonicalKind kind = QuantityCanonicalKind.INSTANCE;
    final Row row =
        quantities
            .select(
                kind.compute(CANONICAL_CODE, functions.col("q")),
                kind.compute(CANONICAL_VALUE_EXACT, functions.col("q")),
                kind.apply(
                    CANONICAL_CODE,
                    functions.col("q"),
                    functions.col("q"),
                    (structure, field, fallback) -> fallback))
            .first();
    assertNull(row.get(0));
    assertNull(row.get(1));
    assertNull(row.get(2));
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
            .selectExpr("valueQuantity.__canonical_value_exact", "valueQuantity.__canonical_code")
            .collectAsList();

    assertNotEquals(rows.get(0).getStruct(0), rows.get(1).getStruct(0));
    assertEquals(0, new BigDecimal("1E-9").compareTo(exactValue(rows.get(0).getStruct(0))));
    assertEquals(0, new BigDecimal("1E-7").compareTo(exactValue(rows.get(1).getStruct(0))));
    assertEquals("g", rows.get(0).getString(1));
  }

  @Test
  void aQuantityWithoutACodeIsAnnotatedWithNull() {
    // The schema is fitted to the data, so this quantity carries neither a system nor a code.
    final Row annotations =
        annotate(
                SchemaConfiguration.builder().build(),
                observation("o1", "{\"value\":3,\"unit\":\"tablets\"}", ""))
            .selectExpr("valueQuantity.__canonical_code", "valueQuantity.__canonical_value_exact")
            .first();

    assertNull(annotations.get(0));
    assertNull(annotations.get(1));
  }

  @Test
  void theSwitchLeavesOutTheAnnotation() {
    final Dataset<Row> annotated =
        annotate(
            SchemaConfiguration.builder().enableCanonicalAnnotation(false).build(),
            observation("o1", ucum("1.5", "kg"), ""));

    final List<String> fields =
        Arrays.asList(structure(annotated.schema().apply("valueQuantity").dataType()).fieldNames());
    assertFalse(fields.contains(CANONICAL_CODE));
    assertFalse(fields.contains(CANONICAL_VALUE_EXACT));
  }
}
