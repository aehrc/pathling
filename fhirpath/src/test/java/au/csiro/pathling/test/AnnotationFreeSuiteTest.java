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

package au.csiro.pathling.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import au.csiro.pathling.definition.ElementDefinition;
import au.csiro.pathling.definition.fhir.FhirDefinitionContext;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhirpath.evaluation.DatasetEvaluator;
import au.csiro.pathling.fhirpath.evaluation.DatasetEvaluatorBuilder;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.schema.DefinitionCanonicalStructure;
import au.csiro.pathling.schema.LayoutEntry;
import au.csiro.pathling.schema.LayoutFields;
import au.csiro.pathling.test.datasource.PrunedSchemaReader;
import au.csiro.pathling.test.layout.LayoutDatasets;
import au.csiro.pathling.test.layout.TestLayout;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.annotation.Nonnull;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.DateTimeType;
import org.hl7.fhir.r4.model.DateType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Quantity;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Shows that the engine test suite runs over data written with no annotations, so that a green
 * default run is the evidence SC-004 asks for and FR-022 is exercised by every test in it (T089).
 *
 * <p>Until M5 the new layout is written in one mode only: the transform in {@code io} emits no
 * annotation of any kind (decision 50). Running the suite a second time "with every annotation
 * disabled" would therefore run the same suite over the same data, and prove nothing a default run
 * does not. What a default run does not prove on its own is the premise, that the data it runs over
 * really is annotation-free. That is what this class asserts, rather than duplicating the suite:
 *
 * <ul>
 *   <li>The fixtures the layout dimension routes, built through {@link LayoutDatasets} on the new
 *       layout, carry no annotation at any depth. Both entry points are covered, JSON documents as
 *       the conformance suites supply them and HAPI objects as the DSL and data-source tests build
 *       them, and so is every resource in the conformance corpora.
 *   <li>The absence is not vacuous. The schema is fitted to the data, so an element that is absent
 *       has no annotation either, and a check for absent annotations could pass because the
 *       annotated elements were never there. Every check therefore records the annotation slots the
 *       definitions declare beside an element that <em>is</em> stored, and asserts that every kind
 *       — numeric, range and canonical — is represented, at the top of a resource and beneath a
 *       repeating element.
 *   <li>The check itself can fail. A control injects annotations at the top of a resource and
 *       inside the elements of an array and asserts both are reported, so a scan that never
 *       descended could not pass for a clean writer.
 *   <li>Files written to disk are annotation-free too, as SC-004 literally states. Fixtures are
 *       written to Parquet and the schema is read from the files' own footers, not imposed on the
 *       read, and a handful of expressions are evaluated over them whose answers depend on a value
 *       an annotation would otherwise supply: a decimal from its stored text, a date range from the
 *       stated precision, and a quantity compared across units. These are illustrative only; the
 *       per-kind detail is in {@code DecimalCollectionTest}, {@code QuantityCollectionTest} and the
 *       conformance suites.
 * </ul>
 *
 * <p>The claim is scoped to the new layout, which is the one annotations belong to. The layout is
 * passed explicitly rather than taken from the active dimension, so a run with {@code
 * -Dpathling.testLayout=previous} asserts the same thing. A few test classes build previous-layout
 * data of their own regardless of the dimension, such as {@code DivergentSchemaViewTest}; that
 * layout has none of these annotations, so they do not weaken the claim, but they are not evidence
 * for it either.
 *
 * <p>At M5 each kind starts being emitted by default and becomes individually disableable (FR-021).
 * This class then changes from asserting that none is emitted to asserting that none is emitted
 * with every kind switched off, and the annotated cases are covered per kind there, with T090
 * asserting that the choice is made from the schema.
 *
 * @author Piotr Szul
 */
@SpringBootUnitTest
@TestInstance(Lifecycle.PER_CLASS)
class AnnotationFreeSuiteTest {

  private static final Parser PARSER = new Parser();

  private static final String UCUM = "http://unitsofmeasure.org";

  /** The directories holding the resources the conformance suites evaluate over. */
  private static final List<String> CONFORMANCE_CORPORA =
      List.of("/fhirpath-js/resources", "/fhirpath-ptl/resources");

  private static final List<String> PATIENTS =
      List.of(
          "{\"resourceType\":\"Patient\",\"id\":\"p1\","
              + "\"meta\":{\"lastUpdated\":\"2020-01-01T00:00:00Z\"},"
              + "\"name\":[{\"family\":\"A\",\"period\":{\"start\":\"2001-01-01\"}}],"
              + "\"birthDate\":\"1974-12-25\",\"deceasedDateTime\":\"2020-05\"}",
          "{\"resourceType\":\"Patient\",\"id\":\"p2\",\"birthDate\":\"1974\"}");

  private static final List<String> OBSERVATIONS =
      List.of(
          "{\"resourceType\":\"Observation\",\"id\":\"o1\",\"status\":\"final\","
              + "\"code\":{\"text\":\"weight\"},\"effectiveDateTime\":\"2020-01\","
              + "\"issued\":\"2020-01-01T10:00:00Z\","
              + "\"valueQuantity\":"
              + quantity("185", "[lb_av]")
              + ",\"component\":[{\"code\":{\"text\":\"a\"},\"valueQuantity\":"
              + quantity("500", "mg")
              + "}]}",
          "{\"resourceType\":\"Observation\",\"id\":\"o2\",\"status\":\"final\","
              + "\"code\":{\"text\":\"weight\"},\"valueQuantity\":"
              + quantity("90", "kg")
              + "}");

  private static final List<String> CONDITIONS =
      List.of(
          "{\"resourceType\":\"Condition\",\"id\":\"c1\",\"subject\":{\"reference\":\"Patient/p1\"},"
              + "\"onsetAge\":"
              + quantity("42", "a")
              + "}");

  private static final List<String> CHARGE_ITEMS =
      List.of(
          "{\"resourceType\":\"ChargeItem\",\"id\":\"ci1\",\"status\":\"billable\","
              + "\"code\":{\"text\":\"x\"},\"subject\":{\"reference\":\"Patient/p1\"},"
              + "\"factorOverride\":0.8}");

  private static final List<String> RISK_ASSESSMENTS =
      List.of(
          "{\"resourceType\":\"RiskAssessment\",\"id\":\"r1\",\"status\":\"final\","
              + "\"subject\":{\"reference\":\"Patient/p1\"},"
              + "\"prediction\":[{\"probabilityDecimal\":0.25}]}");

  /** The annotation slots beside the stored elements of the patient fixtures. */
  private static final List<String> PATIENT_SLOTS =
      List.of(
          "meta.__lastUpdated_start",
          "meta.__lastUpdated_end",
          "name[].period.__start_start",
          "name[].period.__start_end",
          "__birthDate_start",
          "__birthDate_end",
          "__deceasedDateTime_start",
          "__deceasedDateTime_end");

  /** The annotation slots beside the stored elements of the observation fixtures. */
  private static final List<String> OBSERVATION_SLOTS =
      List.of(
          "__effectiveDateTime_start",
          "__issued_start",
          "__valueQuantity_canonical",
          "__valueQuantity_canonical_exact",
          "valueQuantity.__value_numeric",
          "component[].__valueQuantity_canonical",
          "component[].__valueQuantity_canonical_exact",
          "component[].valueQuantity.__value_numeric");

  @Autowired SparkSession spark;

  @Autowired FhirEncoders fhirEncoders;

  @TempDir static Path tempDir;

  private FhirDefinitionContext definitions;

  private String patientDir;

  private String observationDir;

  @BeforeAll
  void setUp() {
    definitions = FhirDefinitionContext.of(fhirEncoders.getContext());
    patientDir = tempDir.resolve("Patient").toString();
    observationDir = tempDir.resolve("Observation").toString();
    PrunedSchemaReader.write(fromJson("Patient", PATIENTS), patientDir);
    PrunedSchemaReader.write(fromJson("Observation", OBSERVATIONS), observationDir);
  }

  @Nonnull
  Stream<Arguments> jsonFixtures() {
    return Stream.of(
        arguments("Patient", PATIENTS, PATIENT_SLOTS),
        arguments("Observation", OBSERVATIONS, OBSERVATION_SLOTS),
        arguments(
            "Condition", CONDITIONS, List.of("__onsetAge_canonical", "onsetAge.__value_numeric")),
        arguments("ChargeItem", CHARGE_ITEMS, List.of("__factorOverride_numeric")),
        arguments(
            "RiskAssessment",
            RISK_ASSESSMENTS,
            List.of("prediction[].__probabilityDecimal_numeric")));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("jsonFixtures")
  void fixturesReadFromJsonCarryNoAnnotations(
      @Nonnull final String resourceType,
      @Nonnull final List<String> json,
      @Nonnull final List<String> expectedSlots) {
    final List<String> slots = new ArrayList<>();
    final List<String> annotations = new ArrayList<>();
    scan(resourceType, fromJson(resourceType, json).schema(), slots, annotations);
    assertThat(slots).containsAll(expectedSlots);
    assertThat(annotations).isEmpty();
  }

  @Test
  void fixturesBuiltAsHapiObjectsCarryNoAnnotations() {
    final Patient patient = new Patient();
    patient.setId("p1");
    patient.getMeta().setLastUpdated(new Date(0));
    patient.addName().setFamily("A").getPeriod().setStartElement(new DateTimeType("2001-01-01"));
    patient.setBirthDateElement(new DateType("1974-12-25"));
    patient.setDeceased(new DateTimeType("2020-05"));

    final Observation observation = new Observation();
    observation.setId("o1");
    observation.getCode().setText("weight");
    observation.setEffective(new DateTimeType("2020-01"));
    observation.setIssued(new Date(0));
    observation.setValue(quantityObject(185, "[lb_av]"));
    observation.addComponent().setValue(quantityObject(500, "mg")).getCode().setText("a");

    final List<String> patientSlots = new ArrayList<>();
    final List<String> observationSlots = new ArrayList<>();
    final List<String> annotations = new ArrayList<>();
    scan("Patient", fromResources("Patient", patient).schema(), patientSlots, annotations);
    scan(
        "Observation",
        fromResources("Observation", observation).schema(),
        observationSlots,
        annotations);
    assertThat(patientSlots).containsAll(PATIENT_SLOTS);
    assertThat(observationSlots).containsAll(OBSERVATION_SLOTS);
    assertThat(annotations).isEmpty();
  }

  @Test
  void conformanceFixturesCarryNoAnnotations() throws IOException, URISyntaxException {
    final List<String> slots = new ArrayList<>();
    final List<String> annotations = new ArrayList<>();
    final ObjectMapper mapper = new ObjectMapper();
    for (final Path file : conformanceFixtures()) {
      final String json = Files.readString(file, StandardCharsets.UTF_8);
      final JsonNode resourceTypeNode = mapper.readTree(json).get(LayoutFields.RESOURCE_TYPE);
      if (resourceTypeNode == null) {
        // A fragment of a resource, which the conformance suites do not read as a resource.
        continue;
      }
      final String resourceType = resourceTypeNode.asText();
      final List<String> fileSlots = new ArrayList<>();
      final List<String> fileAnnotations = new ArrayList<>();
      scan(
          resourceType, fromJson(resourceType, List.of(json)).schema(), fileSlots, fileAnnotations);
      fileSlots.forEach(slot -> slots.add(file.getFileName() + ": " + slot));
      fileAnnotations.forEach(
          annotation -> annotations.add(file.getFileName() + ": " + annotation));
    }
    // The corpora reach every kind, so their being free of annotations is not an accident of what
    // they happen to populate.
    assertThat(slots)
        .anyMatch(slot -> slot.endsWith(LayoutFields.NUMERIC_SUFFIX))
        .anyMatch(slot -> slot.endsWith(LayoutFields.START_SUFFIX))
        .anyMatch(slot -> slot.endsWith(LayoutFields.CANONICAL_SUFFIX));
    assertThat(annotations).isEmpty();
  }

  @Test
  void scanReportsAnnotationsAtTheTopAndInsideArrays() {
    final Dataset<Row> patients =
        fromJson("Patient", PATIENTS)
            .withColumn(LayoutFields.startAnnotationName("birthDate"), functions.lit("1974-12-25"));
    final Dataset<Row> observations =
        fromJson("Observation", OBSERVATIONS)
            .withColumn(
                "component",
                functions.transform(
                    functions.col("component"),
                    component ->
                        component.withField(
                            LayoutFields.canonicalAnnotationName("valueQuantity"),
                            functions.lit("0.0005"))));
    final List<String> annotations = new ArrayList<>();
    scan("Patient", patients.schema(), new ArrayList<>(), annotations);
    scan("Observation", observations.schema(), new ArrayList<>(), annotations);
    assertThat(annotations)
        .containsExactlyInAnyOrder("__birthDate_start", "component[].__valueQuantity_canonical");
  }

  @Test
  void filesWrittenToDiskCarryNoAnnotations() {
    final List<String> patientSlots = new ArrayList<>();
    final List<String> observationSlots = new ArrayList<>();
    final List<String> annotations = new ArrayList<>();
    // The schema is read from the footers of the files, not imposed on the read, so it is what the
    // files hold rather than what was asked of them.
    scan("Patient", spark.read().parquet(patientDir).schema(), patientSlots, annotations);
    scan(
        "Observation",
        spark.read().parquet(observationDir).schema(),
        observationSlots,
        annotations);
    assertThat(patientSlots).containsAll(PATIENT_SLOTS);
    assertThat(observationSlots).containsAll(OBSERVATION_SLOTS);
    assertThat(annotations).isEmpty();
  }

  @Nonnull
  Stream<Arguments> computedValues() {
    return Stream.of(
        // A decimal, from its stored text rather than a numeric annotation.
        arguments(
            ResourceType.OBSERVATION,
            "value.ofType(Quantity).value > 184.5",
            List.of("o1=true", "o2=false")),
        // A quantity compared across units, canonicalised per row rather than read from a
        // canonical annotation. 185 [lb_av] is about 83.9 kg.
        arguments(
            ResourceType.OBSERVATION,
            "value.ofType(Quantity) < 85000 'g'",
            List.of("o1=true", "o2=false")),
        arguments(
            ResourceType.OBSERVATION,
            "component.value.ofType(Quantity) = 0.5 'g'",
            List.of("o1=true", "o2=null")),
        // A date compared with one of another precision, from bounds computed from the stated
        // precision rather than read from range annotations.
        arguments(ResourceType.PATIENT, "birthDate < @1975", List.of("p1=true", "p2=true")),
        arguments(ResourceType.PATIENT, "birthDate > @1974-12", List.of("p1=null", "p2=null")),
        arguments(ResourceType.PATIENT, "birthDate < @1974-06-01", List.of("p1=false", "p2=null")));
  }

  @ParameterizedTest(name = "{1}")
  @MethodSource("computedValues")
  void computesOverFilesWrittenWithNoAnnotations(
      @Nonnull final ResourceType resourceType,
      @Nonnull final String expression,
      @Nonnull final List<String> expected) {
    final String path = resourceType == ResourceType.PATIENT ? patientDir : observationDir;
    final DatasetEvaluator evaluator =
        DatasetEvaluatorBuilder.create(resourceType, fhirEncoders.getContext())
            .withDataset(spark.read().parquet(path))
            .build();
    final List<String> actual =
        evaluator
            .evaluate(PARSER.parse(expression))
            .toCanonical()
            .toIdValueDataset()
            .collectAsList()
            .stream()
            .map(row -> row.getString(0) + "=" + Objects.toString(row.get(1)))
            .toList();
    assertThat(actual).containsExactlyInAnyOrderElementsOf(expected);
  }

  /**
   * Scans a stored schema against the canonical structure of its resource type. Every annotation
   * slot the definitions declare beside an element the schema stores is added to {@code slots}, and
   * every stored field that is an annotation is added to {@code annotations}, each by its path.
   */
  private void scan(
      @Nonnull final String resourceType,
      @Nonnull final StructType stored,
      @Nonnull final List<String> slots,
      @Nonnull final List<String> annotations) {
    scan(
        DefinitionCanonicalStructure.forResource(definitions, resourceType),
        stored,
        "",
        slots,
        annotations);
  }

  private static void scan(
      @Nonnull final DefinitionCanonicalStructure node,
      @Nonnull final StructType stored,
      @Nonnull final String prefix,
      @Nonnull final List<String> slots,
      @Nonnull final List<String> annotations) {
    final List<String> storedNames = Arrays.asList(stored.fieldNames());
    node.entries().stream()
        .filter(LayoutEntry::isAnnotation)
        .filter(
            entry ->
                entry
                    .getElement()
                    .map(ElementDefinition::getElementName)
                    .filter(storedNames::contains)
                    .isPresent())
        .forEach(entry -> slots.add(prefix + entry.getName()));
    for (final StructField field : stored.fields()) {
      final String path = prefix + field.name();
      final Optional<LayoutEntry> entry = node.entry(field.name());
      if (isAnnotation(field.name(), entry)) {
        annotations.add(path);
      } else {
        final Optional<DefinitionCanonicalStructure> child =
            entry.filter(LayoutEntry::isElement).flatMap(node::elementStructure);
        final Optional<StructType> struct = structOf(field.dataType());
        if (child.isPresent() && struct.isPresent()) {
          scan(child.get(), struct.get(), path + step(field.dataType()), slots, annotations);
        } else {
          // A field the definitions do not describe as a structure, such as a metadata group, is
          // still searched by name, so that an annotation beneath it cannot go unseen.
          struct.ifPresent(s -> scanNames(s, path + step(field.dataType()), annotations));
        }
      }
    }
  }

  private static void scanNames(
      @Nonnull final StructType stored,
      @Nonnull final String prefix,
      @Nonnull final List<String> annotations) {
    for (final StructField field : stored.fields()) {
      final String path = prefix + field.name();
      if (field.name().startsWith(LayoutFields.ANNOTATION_PREFIX)) {
        annotations.add(path);
      }
      structOf(field.dataType())
          .ifPresent(s -> scanNames(s, path + step(field.dataType()), annotations));
    }
  }

  private static boolean isAnnotation(
      @Nonnull final String name, @Nonnull final Optional<LayoutEntry> entry) {
    return name.startsWith(LayoutFields.ANNOTATION_PREFIX)
        || entry.filter(LayoutEntry::isAnnotation).isPresent();
  }

  @Nonnull
  private static Optional<StructType> structOf(@Nonnull final DataType type) {
    if (type instanceof final StructType struct) {
      return Optional.of(struct);
    }
    if (type instanceof final ArrayType array) {
      return structOf(array.elementType());
    }
    return Optional.empty();
  }

  @Nonnull
  private static String step(@Nonnull final DataType type) {
    return type instanceof ArrayType ? "[]." : ".";
  }

  @Nonnull
  private Dataset<Row> fromJson(
      @Nonnull final String resourceType, @Nonnull final List<String> json) {
    return LayoutDatasets.fromJson(spark, fhirEncoders, TestLayout.POF, resourceType, json);
  }

  @Nonnull
  private Dataset<Row> fromResources(
      @Nonnull final String resourceType, @Nonnull final IBaseResource resource) {
    return LayoutDatasets.fromResources(
        spark, fhirEncoders, TestLayout.POF, resourceType, List.of(resource));
  }

  @Nonnull
  private static List<Path> conformanceFixtures() throws IOException, URISyntaxException {
    final List<Path> files = new ArrayList<>();
    for (final String corpus : CONFORMANCE_CORPORA) {
      final Path dir =
          Path.of(
              Objects.requireNonNull(AnnotationFreeSuiteTest.class.getResource(corpus)).toURI());
      try (final Stream<Path> listing = Files.list(dir)) {
        listing.filter(file -> file.toString().endsWith(".json")).sorted().forEach(files::add);
      }
    }
    return files;
  }

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

  @Nonnull
  private static Quantity quantityObject(final double value, @Nonnull final String code) {
    return new Quantity().setValue(value).setUnit(code).setSystem(UCUM).setCode(code);
  }
}
