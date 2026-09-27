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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhirpath.evaluation.DatasetEvaluator;
import au.csiro.pathling.fhirpath.evaluation.DatasetEvaluatorBuilder;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.test.SharedMocks;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.datasource.PrunedSchemaReader;
import au.csiro.pathling.test.helpers.TerminologyServiceHelpers;
import au.csiro.pathling.test.layout.LayoutDatasets;
import au.csiro.pathling.test.layout.TestLayout;
import jakarta.annotation.Nonnull;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Tests that extension traversal reads inline extensions on the new layout and continues to read
 * the previous layout's extension map, keyed by {@code _fid} (T097).
 *
 * <p>Each fixture is given once as FHIR JSON and read in both layouts, so every case runs over each
 * layout with the same expected answer. The new layout's schema is pruned to the elements the
 * fixtures populate, so a structure that carries no extension in any resource has no {@code
 * extension} field, and a table whose resources carry none at all has no {@code extension} column
 * and no {@code _extension} column either. Neither can be produced on the previous layout.
 *
 * <p>The cases cover extensions at the resource root, under a singular and a repeating parent,
 * inside a lambda, nested extensions, and extensions on a Coding and on a quantity, which the
 * engine decodes at traversal into the structure it computes with.
 *
 * @author Piotr Szul
 */
@SpringBootUnitTest
@TestInstance(Lifecycle.PER_CLASS)
class ExtensionTraversalTest {

  private static final Parser PARSER = new Parser();

  private static final String UCUM = "http://unitsofmeasure.org";

  private static final List<String> PATIENTS =
      List.of(
          "{\"resourceType\":\"Patient\",\"id\":\"p1\","
              + "\"extension\":["
              + extension("urn:root", "r1")
              + ",{\"url\":\"urn:nested\",\"extension\":["
              + extension("urn:inner", "i1")
              + "]}],"
              + "\"name\":[{\"family\":\"F1\",\"extension\":["
              + extension("urn:name", "n1")
              + "]},{\"family\":\"F2\"}],"
              + "\"maritalStatus\":{\"extension\":["
              + extension("urn:status", "s1")
              + "],\"coding\":[{\"system\":\"urn:system\",\"code\":\"M\",\"extension\":["
              + extension("urn:coding", "c1")
              + "]}]}}",
          "{\"resourceType\":\"Patient\",\"id\":\"p2\","
              + "\"name\":[{\"family\":\"F3\",\"extension\":["
              + extension("urn:name", "n3")
              + "]}]}",
          "{\"resourceType\":\"Patient\",\"id\":\"p3\",\"name\":[{\"family\":\"F4\"}]}");

  // Resources that carry no extension anywhere, so that the new layout's pruned table has neither
  // an extension column, nor an extension field in any structure, nor an extension map.
  private static final List<String> PLAIN_PATIENTS =
      List.of(
          "{\"resourceType\":\"Patient\",\"id\":\"q1\",\"name\":[{\"family\":\"G1\"}],"
              + "\"maritalStatus\":{\"coding\":[{\"system\":\"urn:system\",\"code\":\"M\"}]}}",
          "{\"resourceType\":\"Patient\",\"id\":\"q2\"}");

  private static final List<String> OBSERVATIONS =
      List.of(
          "{\"resourceType\":\"Observation\",\"id\":\"o1\",\"status\":\"final\","
              + "\"code\":{\"text\":\"x\"},\"valueQuantity\":{\"extension\":["
              + extension("urn:quantity", "q1")
              + "],"
              + quantityFields("1.5", "g")
              + "},\"component\":[{\"code\":{\"text\":\"a\"},\"valueQuantity\":{\"extension\":["
              + extension("urn:component", "cq1")
              + "],"
              + quantityFields("2", "kg")
              + "}},{\"code\":{\"text\":\"b\"},\"valueQuantity\":{"
              + quantityFields("3", "kg")
              + "}}]}",
          "{\"resourceType\":\"Observation\",\"id\":\"o2\",\"status\":\"final\","
              + "\"code\":{\"text\":\"x\"},\"valueQuantity\":{"
              + quantityFields("4", "g")
              + "}}");

  // Quantities that carry no extension, so that the new layout's quantity has no extension field.
  private static final List<String> PLAIN_OBSERVATIONS =
      List.of(
          "{\"resourceType\":\"Observation\",\"id\":\"o3\",\"status\":\"final\","
              + "\"code\":{\"text\":\"x\"},\"valueQuantity\":{"
              + quantityFields("5", "g")
              + "},\"component\":[{\"code\":{\"text\":\"a\"},\"valueQuantity\":{"
              + quantityFields("6", "g")
              + "}}]}");

  @Autowired SparkSession spark;

  @Autowired FhirEncoders fhirEncoders;

  @Autowired TerminologyService terminologyService;

  @TempDir static Path tempDir;

  private Map<String, Dataset<Row>> patients;

  private Map<String, Dataset<Row>> plainPatients;

  private Map<String, Dataset<Row>> observations;

  private Map<String, Dataset<Row>> plainObservations;

  @BeforeAll
  void setUp() {
    patients = bothLayouts("Patient", "patients", PATIENTS);
    plainPatients = bothLayouts("Patient", "plainPatients", PLAIN_PATIENTS);
    observations = bothLayouts("Observation", "observations", OBSERVATIONS);
    plainObservations = bothLayouts("Observation", "plainObservations", PLAIN_OBSERVATIONS);
  }

  @Nonnull
  Stream<Arguments> patientCases() {
    return onBothLayouts(
        // At the resource root.
        arguments(
            "extension.url.join(',')", List.of("p1=urn:root,urn:nested", "p2=null", "p3=null")),
        arguments(
            "extension('urn:root').value.ofType(string)",
            List.of("p1=ArraySeq(r1)", "p2=null", "p3=null")),
        // Nested extensions.
        arguments(
            "extension('urn:nested').extension('urn:inner').value.ofType(string)",
            List.of("p1=ArraySeq(i1)", "p2=null", "p3=null")),
        arguments(
            "extension.extension.url", List.of("p1=ArraySeq(urn:inner)", "p2=null", "p3=null")),
        // Under a repeating parent.
        arguments("name.extension.url.count()", List.of("p1=1", "p2=1", "p3=0")),
        arguments(
            "name.extension('urn:name').value.ofType(string).join(',')",
            List.of("p1=n1", "p2=n3", "p3=")),
        // Inside a lambda.
        arguments(
            "name.where(extension('urn:name').exists()).family.join(',')",
            List.of("p1=F1", "p2=F3", "p3=")),
        // Under a singular parent, and on a Coding beneath it.
        arguments(
            "maritalStatus.extension('urn:status').value.ofType(string)",
            List.of("p1=ArraySeq(s1)", "p2=null", "p3=null")),
        arguments(
            "maritalStatus.coding.extension('urn:coding').value.ofType(string)",
            List.of("p1=ArraySeq(c1)", "p2=null", "p3=null")),
        // After a function that keeps the elements.
        arguments(
            "name.first().extension.url",
            List.of("p1=ArraySeq(urn:name)", "p2=ArraySeq(urn:name)", "p3=null")));
  }

  @ParameterizedTest(name = "{1} over the {0} layout")
  @MethodSource("patientCases")
  void extensionsOfPatientsMatchOnBothLayouts(
      @Nonnull final String layout,
      @Nonnull final String expression,
      @Nonnull final List<String> expected) {
    assertThat(evaluate(patients.get(layout), ResourceType.PATIENT, expression))
        .containsExactlyInAnyOrderElementsOf(expected);
  }

  @Nonnull
  Stream<Arguments> plainPatientCases() {
    return onBothLayouts(
        arguments("extension.url.count()", List.of("q1=0", "q2=0")),
        arguments("extension.extension.url.count()", List.of("q1=0", "q2=0")),
        arguments("name.extension.url.count()", List.of("q1=0", "q2=0")),
        arguments("name.where(extension.exists()).count()", List.of("q1=0", "q2=0")),
        arguments("maritalStatus.coding.extension.exists()", List.of("q1=false", "q2=false")));
  }

  @ParameterizedTest(name = "{1} over the {0} layout, with no extensions stored")
  @MethodSource("plainPatientCases")
  void noExtensionsStoredIsEmptyOnBothLayouts(
      @Nonnull final String layout,
      @Nonnull final String expression,
      @Nonnull final List<String> expected) {
    assertThat(evaluate(plainPatients.get(layout), ResourceType.PATIENT, expression))
        .containsExactlyInAnyOrderElementsOf(expected);
  }

  @Nonnull
  Stream<Arguments> quantityCases() {
    return onBothLayouts(
        // A quantity at the resource root, through a choice.
        arguments(
            "value.ofType(Quantity).extension('urn:quantity').value.ofType(string)",
            List.of("o1=ArraySeq(q1)", "o2=null")),
        arguments("value.ofType(Quantity).extension.url.count()", List.of("o1=1", "o2=0")),
        // A quantity under a repeating parent.
        arguments(
            "component.value.ofType(Quantity).extension.value.ofType(string).join(',')",
            List.of("o1=cq1", "o2=null")),
        // Inside a lambda, beside a computation on the decoded quantity.
        arguments(
            "component.value.ofType(Quantity).where($this > 2.5 'kg').extension.exists()",
            List.of("o1=false", "o2=false")),
        arguments(
            "component.value.ofType(Quantity).where(extension.exists()).value",
            List.of("o1=ArraySeq(2.000000)", "o2=null")),
        // After taking the first.
        arguments(
            "component.value.ofType(Quantity).first().extension.url",
            List.of("o1=ArraySeq(urn:component)", "o2=null")),
        // A union with a literal, which combines the decoded structure with the engine's own.
        arguments("(value.ofType(Quantity) | 1 'g').count()", List.of("o1=2", "o2=2")));
  }

  @ParameterizedTest(name = "{1} over the {0} layout")
  @MethodSource("quantityCases")
  void extensionsOfQuantitiesMatchOnBothLayouts(
      @Nonnull final String layout,
      @Nonnull final String expression,
      @Nonnull final List<String> expected) {
    assertThat(evaluate(observations.get(layout), ResourceType.OBSERVATION, expression))
        .containsExactlyInAnyOrderElementsOf(expected);
  }

  @Nonnull
  Stream<Arguments> plainQuantityCases() {
    return onBothLayouts(
        arguments("value.ofType(Quantity).extension.exists()", List.of("o3=false")),
        arguments("component.value.ofType(Quantity).extension.exists()", List.of("o3=false")));
  }

  @ParameterizedTest(name = "{1} over the {0} layout, with no extensions stored")
  @MethodSource("plainQuantityCases")
  void quantityWithNoExtensionsStoredIsEmptyOnBothLayouts(
      @Nonnull final String layout,
      @Nonnull final String expression,
      @Nonnull final List<String> expected) {
    assertThat(evaluate(plainObservations.get(layout), ResourceType.OBSERVATION, expression))
        .containsExactlyInAnyOrderElementsOf(expected);
  }

  @ParameterizedTest(name = "over the {0} layout")
  @ValueSource(strings = {"previous", "pof"})
  void codingBuiltByTheEngineHasNoExtensions(@Nonnull final String layout) {
    // A Coding returned by a terminology operation is built by the engine and carries a null
    // field identifier. It has no extensions to reach on either layout.
    SharedMocks.resetAll();
    TerminologyServiceHelpers.setupLookup(terminologyService)
        .withProperty(
            new Coding("urn:system", "M", null),
            "category",
            null,
            new Coding("urn:category", "C", "Category"))
        .done();
    final String property = "maritalStatus.coding.property('category', 'Coding')";
    assertThat(evaluate(patients.get(layout), ResourceType.PATIENT, property + ".code.exists()"))
        .containsExactlyInAnyOrder("p1=true", "p2=false", "p3=false");
    assertThat(
            evaluate(patients.get(layout), ResourceType.PATIENT, property + ".extension.exists()"))
        .containsExactlyInAnyOrder("p1=false", "p2=false", "p3=false");
  }

  @Nonnull
  private static Stream<Arguments> onBothLayouts(@Nonnull final Arguments... cases) {
    return Stream.of(cases)
        .flatMap(
            c ->
                Stream.of("previous", "pof")
                    .map(layout -> arguments(layout, c.get()[0], c.get()[1])));
  }

  @Nonnull
  private Map<String, Dataset<Row>> bothLayouts(
      @Nonnull final String resourceType,
      @Nonnull final String name,
      @Nonnull final List<String> json) {
    return Map.of(
        "previous",
        stored(TestLayout.PREVIOUS, resourceType, name, json),
        "pof",
        stored(TestLayout.POF, resourceType, name, json));
  }

  /**
   * Builds the fixtures in a layout, and writes them to Parquet and reads them back, so that the
   * engine reads stored files.
   */
  @Nonnull
  private Dataset<Row> stored(
      @Nonnull final TestLayout layout,
      @Nonnull final String resourceType,
      @Nonnull final String name,
      @Nonnull final List<String> json) {
    final Dataset<Row> dataset =
        LayoutDatasets.fromJson(spark, fhirEncoders, layout, resourceType, json);
    return PrunedSchemaReader.write(dataset, tempDir.resolve(layout + "-" + name).toString())
        .read();
  }

  /**
   * Evaluates an expression over each resource in the dataset, returning one {@code id=value} entry
   * per resource.
   */
  @Nonnull
  private List<String> evaluate(
      @Nonnull final Dataset<Row> dataset,
      @Nonnull final ResourceType resourceType,
      @Nonnull final String expression) {
    final DatasetEvaluator evaluator =
        DatasetEvaluatorBuilder.create(resourceType, fhirEncoders.getContext())
            .withDataset(dataset)
            .build();
    return evaluator
        .evaluate(PARSER.parse(expression))
        .toCanonical()
        .toIdValueDataset()
        .collectAsList()
        .stream()
        .map(row -> row.getString(0) + "=" + Objects.toString(row.get(1)))
        .toList();
  }

  @Nonnull
  private static String extension(@Nonnull final String url, @Nonnull final String value) {
    return "{\"url\":\"" + url + "\",\"valueString\":\"" + value + "\"}";
  }

  @Nonnull
  private static String quantityFields(@Nonnull final String value, @Nonnull final String code) {
    return "\"value\":"
        + value
        + ",\"unit\":\""
        + code
        + "\",\"system\":\""
        + UCUM
        + "\",\"code\":\""
        + code
        + "\"";
  }
}
