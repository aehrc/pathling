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

package au.csiro.pathling.fhirpath.operator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.errors.UnsupportedFhirPathFeatureError;
import au.csiro.pathling.fhirpath.evaluation.CollectionDataset;
import au.csiro.pathling.fhirpath.evaluation.DatasetEvaluator;
import au.csiro.pathling.fhirpath.evaluation.DatasetEvaluatorBuilder;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.datasource.PrunedSchemaReader;
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
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Tests that two collections of the same FHIR type whose SQL shapes differ are reconciled wherever
 * they must share a type (T104a, T104b, FR-056, FR-057).
 *
 * <p>Each fixture is given once as FHIR JSON and read in both layouts. On the new layout the schema
 * is pruned to the elements the fixtures populate, so one type reached by two paths has two shapes.
 * On the previous layout the schema is dense, and the shapes differ only where the encoder
 * truncates a recursive type at its nesting bound, so that the same type met at two depths has two
 * shapes. Each shape-sensitive test asserts the shapes it relies on before it asserts anything
 * else, because a pruned schema is derived from the whole fixture set and a new fixture could
 * silently make the shapes agree (T109).
 *
 * <p>Equality and membership are covered with the complex types that have a FHIRPath type, which
 * are Coding and Quantity. The engine does not support equality of any other complex type, which it
 * reports as an unsupported feature before any shape is consulted. Conditional selection is not
 * covered, because the engine does not implement {@code iif()}.
 *
 * @author Piotr Szul
 */
@SpringBootUnitTest
@TestInstance(Lifecycle.PER_CLASS)
class ShapeReconciliationTest {

  private static final Parser PARSER = new Parser();

  private static final String UCUM = "http://unitsofmeasure.org";

  // The names of a Patient have a use and given names, and those of its contacts have text, so
  // the two HumanName shapes of the new layout each carry a field the other lacks.
  private static final List<String> PATIENTS =
      List.of(
          "{\"resourceType\":\"Patient\",\"id\":\"p1\","
              + "\"name\":[{\"use\":\"official\",\"family\":\"F1\",\"given\":[\"G1\"]}],"
              + "\"contact\":[{\"name\":{\"text\":\"Contact One\",\"family\":\"C1\"}}]}",
          "{\"resourceType\":\"Patient\",\"id\":\"p2\",\"name\":[{\"family\":\"F2\"}]}");

  // The Coding of an Observation's code carries a display somewhere in the fixture set and the
  // Coding of a component's code never does. The Observation's quantity carries a unit and the
  // component's quantity does not.
  private static final List<String> OBSERVATIONS =
      List.of(
          "{\"resourceType\":\"Observation\",\"id\":\"o1\",\"status\":\"final\","
              + "\"code\":{\"coding\":[{\"system\":\"urn:s\",\"code\":\"c\"}]},"
              + "\"valueQuantity\":{\"value\":2,\"unit\":\"kg\",\"system\":\""
              + UCUM
              + "\",\"code\":\"kg\"},"
              + "\"component\":[{\"code\":{\"coding\":[{\"system\":\"urn:s\",\"code\":\"c\"}]},"
              + "\"valueQuantity\":{\"value\":2000,\"system\":\""
              + UCUM
              + "\",\"code\":\"g\"}}]}",
          "{\"resourceType\":\"Observation\",\"id\":\"o2\",\"status\":\"final\","
              + "\"code\":{\"coding\":[{\"system\":\"urn:s\",\"code\":\"d\","
              + "\"display\":\"D\"}]}}");

  // A Questionnaire whose items are nested three deep, so that an item and the item beneath it
  // are the same recursive type at two depths.
  private static final List<String> QUESTIONNAIRES =
      List.of(
          "{\"resourceType\":\"Questionnaire\",\"id\":\"q1\",\"status\":\"active\","
              + "\"item\":[{\"linkId\":\"1\",\"type\":\"group\",\"item\":[{\"linkId\":\"1.1\","
              + "\"type\":\"group\",\"item\":[{\"linkId\":\"1.1.1\",\"type\":\"string\"}]}]},"
              + "{\"linkId\":\"2\",\"type\":\"string\"}]}",
          "{\"resourceType\":\"Questionnaire\",\"id\":\"q2\",\"status\":\"active\"}");

  @Autowired SparkSession spark;

  @Autowired FhirEncoders fhirEncoders;

  @TempDir static Path tempDir;

  private Map<String, Dataset<Row>> patients;

  private Map<String, Dataset<Row>> observations;

  private Map<String, Dataset<Row>> questionnaires;

  @BeforeAll
  void setUp() {
    patients = bothLayouts(fhirEncoders, "Patient", "patients", PATIENTS);
    observations = bothLayouts(fhirEncoders, "Observation", "observations", OBSERVATIONS);
    // The encoders of the unit test context do not nest a recursive type at all, so the item
    // beneath
    // an item would be absent on the previous layout. These nest it three deep, as the released
    // encoders do by default, so that it is present and truncated.
    final FhirEncoders nestingEncoders =
        FhirEncoders.forR4()
            .withExtensionsEnabled(true)
            .withAllOpenTypes()
            .withMaxNestingLevel(3)
            .getOrCreate();
    questionnaires =
        bothLayouts(nestingEncoders, "Questionnaire", "questionnaires", QUESTIONNAIRES);
  }

  // -----------------------------------------------------------------------------------------------
  // T104a: one complex type reached by two paths whose fitted shapes differ.
  // -----------------------------------------------------------------------------------------------

  @Test
  void humanNamesHaveTwoShapesOnTheNewLayout() {
    // The precondition of every HumanName case below (T109).
    final Dataset<Row> dataset = patients.get("pof");
    assertThat(elementFields(valueType(dataset, ResourceType.PATIENT, "name")))
        .containsExactly("use", "family", "given");
    assertThat(elementFields(valueType(dataset, ResourceType.PATIENT, "contact.name")))
        .containsExactly("text", "family");
  }

  @Nonnull
  Stream<Arguments> humanNameCases() {
    return onBothLayouts(
        arguments("(name | contact.name).family.join(',')", List.of("p1=F1,C1", "p2=F2")),
        arguments("name.combine(contact.name).family.join(',')", List.of("p1=F1,C1", "p2=F2")),
        arguments("(name | contact.name).count()", List.of("p1=2", "p2=1")),
        arguments("contact.name.combine(name).count()", List.of("p1=2", "p2=1")),
        // Each side's own fields remain traversable, and are empty for the other side's elements.
        arguments("(name | contact.name).text", List.of("p1=ArraySeq(Contact One)", "p2=null")),
        arguments("(contact.name | name).given", List.of("p1=ArraySeq(G1)", "p2=null")),
        arguments(
            "(name | contact.name).where(use = 'official').family",
            List.of("p1=ArraySeq(F1)", "p2=null")),
        // A combination with an operand that is empty in every row, and absent from neither shape.
        arguments("(name | contact.name | name).count()", List.of("p1=2", "p2=1")));
  }

  @ParameterizedTest(name = "{1} over the {0} layout")
  @MethodSource("humanNameCases")
  void humanNamesOfDifferentShapesCombine(
      @Nonnull final String layout,
      @Nonnull final String expression,
      @Nonnull final List<String> expected) {
    assertThat(evaluate(patients.get(layout), ResourceType.PATIENT, expression))
        .containsExactlyInAnyOrderElementsOf(expected);
  }

  @ParameterizedTest(name = "{0}")
  @ValueSource(
      strings = {"name | contact.name", "contact.name | name", "name.combine(contact.name)"})
  void combinedHumanNamesAreInDefinitionOrder(@Nonnull final String expression) {
    // The merged structure carries the fields of both shapes, in the order of the definition of
    // HumanName, whichever operand comes first (FR-057).
    assertThat(elementFields(valueType(patients.get("pof"), ResourceType.PATIENT, expression)))
        .containsExactly("use", "text", "family", "given");
  }

  @Test
  void codingsAndQuantitiesHaveTwoShapesOnTheNewLayout() {
    // The precondition of every Coding and Quantity case below (T109).
    final Dataset<Row> dataset = observations.get("pof");
    assertThat(elementFields(valueType(dataset, ResourceType.OBSERVATION, "code.coding")))
        .containsExactly("system", "code", "display");
    assertThat(elementFields(valueType(dataset, ResourceType.OBSERVATION, "component.code.coding")))
        .containsExactly("system", "code");
    final StructType stored = storedType(dataset, "valueQuantity");
    final StructType storedComponent =
        (StructType)
            ((StructType)
                    ((ArrayType) dataset.schema().apply("component").dataType()).elementType())
                .apply("valueQuantity")
                .dataType();
    assertThat(stored.fieldNames()).contains("unit");
    assertThat(storedComponent.fieldNames()).doesNotContain("unit");
  }

  @Nonnull
  Stream<Arguments> codingAndQuantityCases() {
    return onBothLayouts(
        // Equality of complex values.
        arguments("code.coding = component.code.coding", List.of("o1=true", "o2=null")),
        arguments("component.code.coding != code.coding", List.of("o1=false", "o2=null")),
        arguments(
            "value.ofType(Quantity) = component.value.ofType(Quantity)",
            List.of("o1=true", "o2=null")),
        // Membership.
        arguments("component.code.coding.first() in code.coding", List.of("o1=true", "o2=null")),
        arguments(
            "code.coding contains component.code.coding.first()", List.of("o1=true", "o2=null")),
        arguments(
            "component.value.ofType(Quantity).first() in value.ofType(Quantity)",
            List.of("o1=true", "o2=null")),
        // Union and combination.
        arguments("(code.coding | component.code.coding).count()", List.of("o1=1", "o2=1")),
        arguments("code.coding.combine(component.code.coding).count()", List.of("o1=2", "o2=1")),
        arguments(
            "(code.coding | component.code.coding).display", List.of("o1=null", "o2=ArraySeq(D)")),
        arguments(
            "(value.ofType(Quantity) | component.value.ofType(Quantity)).count()",
            List.of("o1=1", "o2=0")),
        arguments(
            "value.ofType(Quantity).combine(component.value.ofType(Quantity)).unit",
            List.of("o1=ArraySeq(kg)", "o2=null")));
  }

  @ParameterizedTest(name = "{1} over the {0} layout")
  @MethodSource("codingAndQuantityCases")
  void codingsAndQuantitiesOfDifferentShapesCompareAndCombine(
      @Nonnull final String layout,
      @Nonnull final String expression,
      @Nonnull final List<String> expected) {
    assertThat(evaluate(observations.get(layout), ResourceType.OBSERVATION, expression))
        .containsExactlyInAnyOrderElementsOf(expected);
  }

  // -----------------------------------------------------------------------------------------------
  // T104b: one recursive type met at two depths.
  // -----------------------------------------------------------------------------------------------

  @ParameterizedTest(name = "over the {0} layout")
  @ValueSource(strings = {"previous", "pof"})
  void itemsAtTwoDepthsHaveTwoShapes(@Nonnull final String layout) {
    // The precondition of every item case below (T109). On the previous layout the encoder
    // truncates the recursion at its nesting bound, so the item beneath an item is one level
    // shallower. On the new layout the innermost item has no items of its own.
    final Dataset<Row> dataset = questionnaires.get(layout);
    final DataType item = valueType(dataset, ResourceType.QUESTIONNAIRE, "item");
    final DataType nested = valueType(dataset, ResourceType.QUESTIONNAIRE, "item.item");
    assertThat(elementFields(item)).contains("item");
    assertThat(item).isNotEqualTo(nested);
  }

  @Nonnull
  Stream<Arguments> itemCases() {
    return onBothLayouts(
        arguments("(item | item.item).linkId.join(',')", List.of("q1=1,2,1.1", "q2=")),
        arguments("item.item.combine(item).linkId.join(',')", List.of("q1=1.1,1,2", "q2=")),
        arguments("(item | item.item).count()", List.of("q1=3", "q2=0")),
        // The combined items remain traversable, including into the items beneath them.
        arguments("(item | item.item).item.linkId.join(',')", List.of("q1=1.1,1.1.1", "q2=")),
        arguments(
            "(item | item.item).where(type = 'group').linkId.join(',')",
            List.of("q1=1,1.1", "q2=")));
  }

  @ParameterizedTest(name = "{1} over the {0} layout")
  @MethodSource("itemCases")
  void itemsAtTwoDepthsCombine(
      @Nonnull final String layout,
      @Nonnull final String expression,
      @Nonnull final List<String> expected) {
    assertThat(evaluate(questionnaires.get(layout), ResourceType.QUESTIONNAIRE, expression))
        .containsExactlyInAnyOrderElementsOf(expected);
  }

  @ParameterizedTest(name = "over the {0} layout")
  @ValueSource(strings = {"previous", "pof"})
  void itemsAtTwoDepthsDoNotCompare(@Nonnull final String layout) {
    // An item has no FHIRPath type, and the engine does not support equality of such a type. It
    // says so before any shape is consulted, so this is not a reconciliation failure.
    assertThatThrownBy(
            () ->
                evaluate(
                    questionnaires.get(layout),
                    ResourceType.QUESTIONNAIRE,
                    "item.item.first() in item"))
        .hasRootCauseInstanceOf(UnsupportedFhirPathFeatureError.class);
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
      @Nonnull final FhirEncoders encoders,
      @Nonnull final String resourceType,
      @Nonnull final String name,
      @Nonnull final List<String> json) {
    return Map.of(
        "previous",
        stored(encoders, TestLayout.PREVIOUS, resourceType, name, json),
        "pof",
        stored(encoders, TestLayout.POF, resourceType, name, json));
  }

  /**
   * Builds the fixtures in a layout, and writes them to Parquet and reads them back, so that the
   * engine reads stored files.
   */
  @Nonnull
  private Dataset<Row> stored(
      @Nonnull final FhirEncoders encoders,
      @Nonnull final TestLayout layout,
      @Nonnull final String resourceType,
      @Nonnull final String name,
      @Nonnull final List<String> json) {
    final Dataset<Row> dataset =
        LayoutDatasets.fromJson(spark, encoders, layout, resourceType, json);
    return PrunedSchemaReader.write(dataset, tempDir.resolve(layout + "-" + name).toString())
        .read();
  }

  @Nonnull
  private CollectionDataset evaluateToDataset(
      @Nonnull final Dataset<Row> dataset,
      @Nonnull final ResourceType resourceType,
      @Nonnull final String expression) {
    final DatasetEvaluator evaluator =
        DatasetEvaluatorBuilder.create(resourceType, fhirEncoders.getContext())
            .withDataset(dataset)
            .build();
    return evaluator.evaluate(PARSER.parse(expression));
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
    return evaluateToDataset(dataset, resourceType, expression)
        .toCanonical()
        .toIdValueDataset()
        .collectAsList()
        .stream()
        .map(row -> row.getString(0) + "=" + Objects.toString(row.get(1)))
        .toList();
  }

  /** Returns the SQL type of the value an expression evaluates to. */
  @Nonnull
  private DataType valueType(
      @Nonnull final Dataset<Row> dataset,
      @Nonnull final ResourceType resourceType,
      @Nonnull final String expression) {
    return evaluateToDataset(dataset, resourceType, expression)
        .toIdValueDataset()
        .schema()
        .apply(CollectionDataset.VALUE_COLUMN)
        .dataType();
  }

  /** Returns the stored type of a singular top-level structure. */
  @Nonnull
  private static StructType storedType(
      @Nonnull final Dataset<Row> dataset, @Nonnull final String column) {
    return (StructType) dataset.schema().apply(column).dataType();
  }

  /** Returns the field names of a structure, or of the elements of an array of structures. */
  @Nonnull
  private static List<String> elementFields(@Nonnull final DataType type) {
    final DataType element = type instanceof final ArrayType array ? array.elementType() : type;
    return List.of(((StructType) element).fieldNames());
  }
}
