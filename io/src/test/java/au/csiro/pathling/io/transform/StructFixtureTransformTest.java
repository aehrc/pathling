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

package au.csiro.pathling.io.transform;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import jakarta.annotation.Nonnull;
import java.nio.charset.StandardCharsets;
import java.sql.Date;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Tests the transform on datasets built with an explicit schema rather than read from JSON text
 * (decision 70).
 *
 * <p>The transform is structured in and structured out, so its contract is stated over schemas, and
 * some of the schemas it has to answer for are ones JSON inference never produces: a narrower
 * integral type, a float, a Spark date, a map. Building the input directly is the only way to reach
 * them, and it keeps the text reader out of what is being tested.
 */
class StructFixtureTransformTest {

  @Nonnull
  static Stream<Arguments> narrowerIntegrals() {
    return Stream.of(
        Arguments.of(DataTypes.ByteType, (byte) 2),
        Arguments.of(DataTypes.ShortType, (short) 2),
        Arguments.of(DataTypes.IntegerType, 2));
  }

  // The input contract: the lossless widenings of a JSON type are accepted.

  @ParameterizedTest
  @MethodSource("narrowerIntegrals")
  void storesANarrowerIntegralColumnAsTheIntegerTheDefinitionsDeclare(
      @Nonnull final DataType type, @Nonnull final Object value) {
    final Dataset<Row> source =
        dataset(
            schema(
                field("resourceType", DataTypes.StringType),
                field("id", DataTypes.StringType),
                field("multipleBirthInteger", type)),
            RowFactory.create("Patient", "1", value));

    final Dataset<Row> stored = toLayout("Patient", source);

    assertEquals(DataTypes.IntegerType, stored.schema().apply("multipleBirthInteger").dataType());
    assertEquals(2, stored.first().<Integer>getAs("multipleBirthInteger"));
    assertEquals(List.of(), findings("Patient", source));
  }

  @ParameterizedTest
  @MethodSource("narrowerIntegrals")
  void storesAnIntegralColumnForADecimalAsItsText(
      @Nonnull final DataType type, @Nonnull final Object value) {
    final Dataset<Row> source = observation(type, value);

    final Dataset<Row> stored = toLayout("Observation", source);

    assertEquals("2", stored.select("valueQuantity.value").first().getString(0));
    assertEquals(List.of(), findings("Observation", source));
  }

  // Types outside the contract are reported and stored as typed nulls, never converted.

  @Test
  void reportsAndNullsAFloatColumnForADecimal() {
    final Dataset<Row> source = observation(DataTypes.FloatType, 1.5f);

    final Dataset<Row> stored = toLayout("Observation", source);

    assertEquals(DataTypes.StringType, valueQuantity(stored.schema()).apply("value").dataType());
    assertNull(stored.select("valueQuantity.value").first().get(0));
    assertEquals(
        List.of("Observation.valueQuantity.value"),
        paths(findings("Observation", source), NonConformantContent::isEncodingMismatch));
  }

  @Test
  void reportsAndNullsADateColumnForADate() {
    final Dataset<Row> source =
        dataset(
            schema(
                field("resourceType", DataTypes.StringType),
                field("id", DataTypes.StringType),
                field("birthDate", DataTypes.DateType)),
            RowFactory.create("Patient", "1", Date.valueOf("1980-01-01")));

    final Dataset<Row> stored = toLayout("Patient", source);

    assertEquals(DataTypes.StringType, stored.schema().apply("birthDate").dataType());
    assertNull(stored.first().getAs("birthDate"));
    assertEquals(
        List.of("Patient.birthDate"),
        paths(findings("Patient", source), NonConformantContent::isEncodingMismatch));
  }

  @Test
  void dropsAndReportsAMapWhereAStructureIsDeclared() {
    final Dataset<Row> source =
        dataset(
            schema(
                field("resourceType", DataTypes.StringType),
                field("id", DataTypes.StringType),
                field(
                    "maritalStatus",
                    DataTypes.createMapType(DataTypes.StringType, DataTypes.StringType))),
            RowFactory.create("Patient", "1", Map.of("text", "M")));

    final Dataset<Row> stored = toLayout("Patient", source);

    assertEquals(List.of("resourceType", "id"), List.of(stored.schema().fieldNames()));
    assertEquals(
        List.of("Patient.maritalStatus"),
        paths(findings("Patient", source), NonConformantContent::isShapeMismatch));
  }

  // Structure: what the definitions decide, whatever the input's schema says.

  @Test
  void namesTheResourceTypeWhereTheSourceCarriesNoColumnForIt() {
    final Dataset<Row> source =
        dataset(
            schema(field("id", DataTypes.StringType), field("gender", DataTypes.StringType)),
            RowFactory.create("1", "female"));

    final Dataset<Row> stored = toLayout("Patient", source);

    assertEquals(List.of("resourceType", "id", "gender"), List.of(stored.schema().fieldNames()));
    assertEquals("Patient", stored.first().getAs("resourceType"));
  }

  @Test
  void ordersARepeatingStructureAsTheDefinitionsDeclareAndReportsWhatTheyDoNotDescribe() {
    final StructType name =
        schema(
            field("given", DataTypes.createArrayType(DataTypes.StringType)),
            field("bogus", DataTypes.StringType),
            field("family", DataTypes.StringType));
    final Dataset<Row> source =
        dataset(
            schema(
                field("resourceType", DataTypes.StringType),
                field("id", DataTypes.StringType),
                field("name", DataTypes.createArrayType(name))),
            RowFactory.create(
                "Patient",
                "1",
                List.of(
                    RowFactory.create(List.of("Jane"), "x", "Smith"),
                    RowFactory.create(null, "y", "Jones"))));

    final Dataset<Row> stored = toLayout("Patient", source);

    final StructType storedName =
        (StructType) ((ArrayType) stored.schema().apply("name").dataType()).elementType();
    assertEquals(List.of("family", "given"), List.of(storedName.fieldNames()));
    final List<Row> names = stored.first().getList(stored.schema().fieldIndex("name"));
    assertEquals(List.of("Smith", "Jones"), names.stream().map(row -> row.getString(0)).toList());
    assertEquals(List.of("Jane"), names.get(0).getList(1));
    assertEquals(
        List.of("Patient.name.bogus"),
        paths(findings("Patient", source), NonConformantContent::isUndescribedContent));
  }

  // The two directions are inverses over input in the JSON data model.

  @Test
  void returnsInputInTheJsonDataModelThroughTheLayoutAndBack() {
    final String data =
        Base64.getEncoder().encodeToString("hello".getBytes(StandardCharsets.US_ASCII));
    final StructType schema =
        schema(
            field("resourceType", DataTypes.StringType),
            field("id", DataTypes.StringType),
            field("active", DataTypes.BooleanType),
            field(
                "name",
                DataTypes.createArrayType(
                    schema(
                        field("family", DataTypes.StringType),
                        field("given", DataTypes.createArrayType(DataTypes.StringType))))),
            field("gender", DataTypes.StringType),
            field("photo", DataTypes.createArrayType(schema(field("data", DataTypes.StringType)))));
    final Dataset<Row> source =
        dataset(
            schema,
            RowFactory.create(
                "Patient",
                "1",
                true,
                List.of(RowFactory.create("Smith", List.of("Jane", "Elizabeth"))),
                "female",
                List.of(RowFactory.create(data))));

    final Dataset<Row> stored = toLayout("Patient", source);
    final Dataset<Row> shaped = TransformFixtures.transformer().toJsonShape("Patient", stored);

    assertEquals(
        DataTypes.BinaryType,
        ((StructType) ((ArrayType) stored.schema().apply("photo").dataType()).elementType())
            .apply("data")
            .dataType());
    assertEquals(schema.catalogString(), shaped.schema().catalogString());
    assertEquals(source.collectAsList(), shaped.collectAsList());
  }

  @Test
  void returnsADecimalAsTheDoubleItWasGiven() {
    final Dataset<Row> source = observation(DataTypes.DoubleType, 1.5);

    final Dataset<Row> stored = toLayout("Observation", source);
    final Dataset<Row> shaped = TransformFixtures.transformer().toJsonShape("Observation", stored);

    assertEquals("1.5", stored.select("valueQuantity.value").first().getString(0));
    assertEquals(DataTypes.DoubleType, valueQuantity(shaped.schema()).apply("value").dataType());
    assertEquals(source.collectAsList(), shaped.collectAsList());
  }

  /** Returns an observation whose quantity carries a value of the given column type. */
  @Nonnull
  private static Dataset<Row> observation(
      @Nonnull final DataType type, @Nonnull final Object value) {
    return dataset(
        schema(
            field("resourceType", DataTypes.StringType),
            field("id", DataTypes.StringType),
            field("status", DataTypes.StringType),
            field(
                "valueQuantity",
                schema(field("value", type), field("unit", DataTypes.StringType)))),
        RowFactory.create("Observation", "1", "final", RowFactory.create(value, "mg")));
  }

  @Nonnull
  private static Dataset<Row> toLayout(
      @Nonnull final String resourceType, @Nonnull final Dataset<Row> source) {
    return TransformFixtures.transformer().toLayout(resourceType, source);
  }

  @Nonnull
  private static List<NonConformantContent> findings(
      @Nonnull final String resourceType, @Nonnull final Dataset<Row> source) {
    return TransformFixtures.transformer().findings(resourceType, source.schema());
  }

  @Nonnull
  private static List<String> paths(
      @Nonnull final List<NonConformantContent> findings,
      @Nonnull final Predicate<NonConformantContent> kind) {
    return findings.stream().filter(kind).map(NonConformantContent::getPath).toList();
  }

  @Nonnull
  private static StructType valueQuantity(@Nonnull final StructType schema) {
    return (StructType) schema.apply("valueQuantity").dataType();
  }

  @Nonnull
  private static Dataset<Row> dataset(@Nonnull final StructType schema, @Nonnull final Row row) {
    return TransformFixtures.spark().createDataFrame(List.of(row), schema);
  }

  @Nonnull
  private static StructType schema(@Nonnull final StructField... fields) {
    return new StructType(fields);
  }

  @Nonnull
  private static StructField field(@Nonnull final String name, @Nonnull final DataType type) {
    return new StructField(name, type, true, Metadata.empty());
  }
}
