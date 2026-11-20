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

package au.csiro.pathling.fhirpath.collection;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.schema.SchemaConfiguration;
import au.csiro.pathling.search.SearchColumnBuilder;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.datasource.PrunedSchemaReader;
import au.csiro.pathling.test.layout.AnnotatedDatasets;
import au.csiro.pathling.test.layout.PofDatasets;
import jakarta.annotation.Nonnull;
import java.nio.file.Path;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Tests that whether the engine uses an annotation is decided from the schema, never per row
 * (FR-023), for each annotation kind.
 *
 * <p>The same resources are stored three ways: without annotations, annotated, and annotated with
 * the annotations of the first resource set to null, as a table merged from annotated and
 * unannotated sources holds them under the default <em>fill</em> (#2923). Where the schema carries
 * the annotation the engine reads it in every row, so the first resource takes the null annotation
 * and is not matched, although its value is present. Had the choice been made per row, it would
 * have computed the annotation for that resource, as it does where the schema does not carry it.
 *
 * <p>The plans that each choice builds are asserted by the tests of each kind: {@code
 * DecimalCollectionTest}, {@code DateRangeComparisonTest}, {@code QuantityCollectionTest} and
 * {@code QuantityNumericAnnotationTest}.
 */
@SpringBootUnitTest
@TestInstance(Lifecycle.PER_CLASS)
class AnnotationDecidedBySchemaTest {

  @Autowired SparkSession spark;

  @Autowired FhirEncoders fhirEncoders;

  @TempDir Path tempDir;

  @Nonnull
  Stream<Arguments> kinds() {
    return Stream.of(
        arguments(
            "numeric",
            "ChargeItem",
            List.of(
                "{\"resourceType\":\"ChargeItem\",\"id\":\"r1\",\"status\":\"billable\","
                    + "\"factorOverride\":2.5}",
                "{\"resourceType\":\"ChargeItem\",\"id\":\"r2\",\"status\":\"billable\","
                    + "\"factorOverride\":3}"),
            "factorOverride > 1",
            "_factorOverride",
            List.of("__numeric")),
        arguments(
            "range",
            "Patient",
            List.of(
                "{\"resourceType\":\"Patient\",\"id\":\"r1\",\"birthDate\":\"1980-06-15\"}",
                "{\"resourceType\":\"Patient\",\"id\":\"r2\",\"birthDate\":\"1990\"}"),
            "birthDate > @1970",
            "_birthDate",
            List.of("__start", "__end")),
        arguments(
            "canonical",
            "Observation",
            List.of(
                "{\"resourceType\":\"Observation\",\"id\":\"r1\",\"status\":\"final\","
                    + "\"code\":{\"text\":\"weight\"},\"valueQuantity\":{\"value\":1500,"
                    + "\"unit\":\"mg\",\"system\":\"http://unitsofmeasure.org\",\"code\":\"mg\"}}",
                "{\"resourceType\":\"Observation\",\"id\":\"r2\",\"status\":\"final\","
                    + "\"code\":{\"text\":\"weight\"},\"valueQuantity\":{\"value\":2,"
                    + "\"unit\":\"g\",\"system\":\"http://unitsofmeasure.org\",\"code\":\"g\"}}"),
            "value.ofType(Quantity) > 1 'g'",
            "valueQuantity",
            List.of("__canonical_code", "__canonical_value_exact")));
  }

  /**
   * A null annotation in a schema that carries the annotation is read as it is, and is not computed
   * for that row, while the same value without the annotation in the schema is computed and
   * matched.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("kinds")
  void aNullAnnotationIsReadWhereTheSchemaCarriesIt(
      @Nonnull final String kind,
      @Nonnull final String resourceType,
      @Nonnull final List<String> json,
      @Nonnull final String expression,
      @Nonnull final String container,
      @Nonnull final List<String> fields) {
    final Dataset<Row> resources =
        PofDatasets.fromJson(spark, fhirEncoders.getContext(), resourceType, json);
    final Dataset<Row> read =
        AnnotatedDatasets.stored(
            fhirEncoders.getContext(),
            resourceType,
            resources,
            Optional.empty(),
            tempDir.resolve("read").toString());
    final Dataset<Row> annotated =
        AnnotatedDatasets.stored(
            fhirEncoders.getContext(),
            resourceType,
            resources,
            Optional.of(SchemaConfiguration.builder().build()),
            tempDir.resolve("annotated").toString());
    final Dataset<Row> merged =
        PrunedSchemaReader.write(
                withNullAnnotations(annotated, container, fields),
                tempDir.resolve("merged").toString())
            .read();

    assertThat(matches(read, resourceType, expression)).containsExactlyInAnyOrder("r1", "r2");
    assertThat(matches(annotated, resourceType, expression)).containsExactlyInAnyOrder("r1", "r2");
    assertThat(matches(merged, resourceType, expression)).containsExactly("r2");
  }

  /** Sets the annotation fields of a container to null in the first resource. */
  @Nonnull
  private static Dataset<Row> withNullAnnotations(
      @Nonnull final Dataset<Row> resources,
      @Nonnull final String container,
      @Nonnull final List<String> fields) {
    final StructType type = (StructType) resources.schema().apply(container).dataType();
    Column nulled = col(container);
    for (final String field : fields) {
      final StructField annotation = type.apply(field);
      nulled = nulled.withField(field, lit(null).cast(annotation.dataType()));
    }
    return resources.withColumn(
        container, when(col("id").equalTo("r1"), nulled).otherwise(col(container)));
  }

  /** The ids of the resources an expression is true for. */
  @Nonnull
  private List<String> matches(
      @Nonnull final Dataset<Row> resources,
      @Nonnull final String resourceType,
      @Nonnull final String expression) {
    return resources
        .filter(
            SearchColumnBuilder.withDefaultRegistry(fhirEncoders.getContext())
                .fromExpression(ResourceType.fromCode(resourceType), expression))
        .select("id")
        .collectAsList()
        .stream()
        .map(row -> row.getString(0))
        .toList();
  }
}
