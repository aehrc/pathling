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
import static org.junit.jupiter.params.provider.Arguments.arguments;

import au.csiro.pathling.definition.fhir.FhirDefinitionContext;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.io.Annotator;
import au.csiro.pathling.schema.SchemaConfiguration;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.datasource.PrunedSchemaReader;
import au.csiro.pathling.test.layout.PofDatasets;
import jakarta.annotation.Nonnull;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Tests that a search over a primitive whose value is absent, but whose metadata group carries an
 * id or an extension, does not match it, with the annotations of the primitive stored and not.
 *
 * <p>The group is stored for the id or the extension, and over annotated data for the annotations
 * too, so the schema carries the group while the value is absent. A date search reads the group's
 * range annotations where they are stored, and a token search reads the value. Neither may take the
 * group for a value. The first patient carries values, as a control that each search matches.
 */
@SpringBootUnitTest
@TestInstance(Lifecycle.PER_CLASS)
class MetadataOnlyPrimitiveSearchTest {

  private static final String EXTENSION =
      "{\"extension\":[{\"url\":\"http://example.org/reason\",\"valueString\":\"unknown\"}]}";

  private static final List<String> PATIENTS =
      List.of(
          "{\"resourceType\":\"Patient\",\"id\":\"p1\",\"birthDate\":\"1980-06-15\","
              + "\"gender\":\"female\"}",
          "{\"resourceType\":\"Patient\",\"id\":\"p2\",\"_birthDate\":{\"id\":\"b2\"},"
              + "\"_gender\":{\"id\":\"g2\"}}",
          "{\"resourceType\":\"Patient\",\"id\":\"p3\",\"_birthDate\":"
              + EXTENSION
              + ",\"_gender\":"
              + EXTENSION
              + "}");

  @Autowired SparkSession spark;

  @Autowired FhirEncoders fhirEncoders;

  @TempDir static Path tempDir;

  /** The patients as stored without annotations, with annotations on, and with them off. */
  private Map<String, Dataset<Row>> paths;

  @BeforeAll
  void setUp() {
    paths =
        Map.of(
            "read",
            stored("read", Optional.empty()),
            "annotated",
            stored("annotated", Optional.of(SchemaConfiguration.builder().build())),
            "disabled",
            stored(
                "disabled",
                Optional.of(
                    SchemaConfiguration.builder()
                        .enableNumericAnnotation(false)
                        .enableRangeAnnotation(false)
                        .enableCanonicalAnnotation(false)
                        .build())));
  }

  @Nonnull
  Stream<Arguments> searchCases() {
    return Stream.of(
        arguments("birthdate=1980-06-15", List.of("p1")),
        arguments("birthdate=ge1900", List.of("p1")),
        arguments("birthdate=lt2100", List.of("p1")),
        arguments("birthdate=ne2000", List.of("p1")),
        arguments("birthdate=2000", List.of()),
        arguments("gender=female", List.of("p1")),
        arguments("gender=male", List.of()));
  }

  /** A search matches only the patient that carries a value, over every path. */
  @ParameterizedTest(name = "{0}")
  @MethodSource("searchCases")
  void aPrimitiveWithOnlyAnIdOrExtensionIsNotMatched(
      @Nonnull final String query, @Nonnull final List<String> expected) {
    for (final Map.Entry<String, Dataset<Row>> path : paths.entrySet()) {
      final List<String> ids =
          path
              .getValue()
              .filter(
                  SearchColumnBuilder.withDefaultRegistry(fhirEncoders.getContext())
                      .fromQueryString(ResourceType.PATIENT, query))
              .select("id")
              .collectAsList()
              .stream()
              .map(row -> row.getString(0))
              .toList();
      assertThat(ids).as(path.getKey()).containsExactlyInAnyOrderElementsOf(expected);
    }
  }

  /** The metadata group is stored for each path, so that the search reads a schema carrying it. */
  @ParameterizedTest(name = "{0}")
  @MethodSource("storedGroups")
  void theMetadataGroupIsStored(@Nonnull final String path, @Nonnull final String group) {
    assertThat(paths.get(path).schema().simpleString()).contains(group);
  }

  @Nonnull
  Stream<Arguments> storedGroups() {
    return Stream.of(
        arguments("read", "_birthDate:struct<id:string,extension:"),
        arguments("disabled", "_birthDate:struct<id:string,extension:"),
        arguments("annotated", "__start:timestamp,__end:timestamp>"),
        arguments("read", "_gender:struct<id:string,extension:"));
  }

  /**
   * Builds the patients, as read or annotated as encoding stores them under a configuration, and
   * writes them to Parquet and reads them back, so that the search reads stored files.
   */
  @Nonnull
  private Dataset<Row> stored(
      @Nonnull final String name, @Nonnull final Optional<SchemaConfiguration> annotation) {
    final Dataset<Row> resources =
        PofDatasets.fromJson(spark, fhirEncoders.getContext(), "Patient", PATIENTS);
    final Dataset<Row> dataset =
        annotation
            .map(
                configuration ->
                    Annotator.of(FhirDefinitionContext.of(fhirEncoders.getContext()), configuration)
                        .annotate("Patient", resources))
            .orElse(resources);
    return PrunedSchemaReader.write(dataset, tempDir.resolve(name).toString()).read();
  }
}
