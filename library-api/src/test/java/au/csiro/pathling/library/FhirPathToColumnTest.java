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

package au.csiro.pathling.library;

import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.config.TerminologyConfiguration;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.commons.io.FileUtils;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Tests that the expression-to-column API is independent of the schema of the dataset a column is
 * applied to (FR-054, SC-010, FR-030).
 *
 * <p>The engine's expression-level test set is taken to be the FHIRPath conformance cases the
 * {@code fhirpath} module runs from the fhirpath.js test suite, restricted to those that name an
 * input resource. Each case's expression is compiled to a column once, with no reference to any
 * dataset, and applied to two datasets holding the case's resource: one encoded from that resource
 * alone, and one encoded from it together with other resources of its type, which populate elements
 * it does not and so give the dataset a wider fitted schema. The results for the case's resource
 * must be equal, or the column must fail on both. Whether either result is correct is the
 * conformance suite's concern, not this test's, so no case is excluded here.
 *
 * @author Piotr Szul
 */
class FhirPathToColumnTest {

  private static final String CASES = "fhirpath-js/cases/";
  private static final String RESOURCES = "fhirpath-js/resources/";

  private static final List<String> CASE_FILES =
      List.of(
          "3.2_paths.yaml",
          "4.1_literals.yaml",
          "5.1_existence.yaml",
          "5.2.3_repeat.yaml",
          "5.2_filtering_and_projection.yaml",
          "5.3_subsetting.yaml",
          "5.4_combining.yaml",
          "5.5_conversion.yaml",
          "5.6_string_manipulation.yaml",
          "5.7_math.yaml",
          "5.8_tree_navigation.yaml",
          "5.9_utility_functions.yaml",
          "6.1_equality.yaml",
          "6.2_comparision.yaml",
          "6.3_types.yaml",
          "6.4_collection.yaml",
          "6.4_collections.yaml",
          "6.5_boolean_logic.yaml",
          "6.6_math.yaml",
          "7_aggregate.yaml",
          "8_variables.yaml",
          "extensions.yaml",
          "factory.yaml",
          "fhir-quantity.yaml",
          "fhir-r4.yaml",
          "hasValue.yaml",
          "simple.yaml");

  private static final List<String> RESOURCE_FILES =
      List.of(
          "conceptmap-example.json",
          "diagnosticreport-example.json",
          "medicationrequest-example.json",
          "observation-example-2.json",
          "observation-example.json",
          "patient-example-2.json",
          "patient-example-3.json",
          "patient-example.json",
          "phq9-response-with-embedded-scores.json",
          "phq9-response-with-unlinked-answers.json",
          "phq9-response.json",
          "phq9.json",
          // quantity-example.json is left out: its QuestionnaireResponse.item.answer is an object
          // where FHIR JSON requires an array, which ingest under trust cannot read. It returns
          // once ingest tolerates such input (ticket 26).
          "questionnaire-example.json",
          "questionnaire-response-example.json",
          "valueset-example-expansion.json");

  private static final ObjectMapper JSON = new ObjectMapper();

  private static final String RESULT = "result";

  private static SparkSession spark;
  private static PathlingContext pathling;
  private static Path temporaryDirectory;

  @BeforeAll
  static void setUp() throws IOException {
    spark = TestHelpers.spark();
    pathling =
        PathlingContext.builder(spark)
            .terminologyConfiguration(TerminologyConfiguration.builder().enabled(false).build())
            .build();
    temporaryDirectory = Files.createTempDirectory("pathling-fhirpath-to-column-test-");
  }

  @AfterAll
  static void tearDown() throws IOException {
    FileUtils.deleteDirectory(temporaryDirectory.toFile());
  }

  /**
   * One column, built without reference to any dataset, gives equal results over a narrow and a
   * wide fitted dataset holding the same resource, for every case of the expression-level test set
   * that names an input resource (FR-054, SC-010).
   */
  @Test
  void oneColumnGivesEqualResultsOverFittedSchemasOfDifferingWidth() {
    final Map<String, List<TestCase>> casesByFile =
        CASE_FILES.stream()
            .flatMap(FhirPathToColumnTest::cases)
            .filter(testCase -> RESOURCE_FILES.contains(testCase.inputFile()))
            .collect(
                Collectors.groupingBy(
                    TestCase::inputFile, LinkedHashMap::new, Collectors.toList()));

    final List<String> mismatches = new ArrayList<>();
    int compared = 0;
    int uncompiled = 0;
    for (final Map.Entry<String, List<TestCase>> entry : casesByFile.entrySet()) {
      final InputPair inputs = InputPair.of(entry.getKey());
      assertNotEquals(
          inputs.narrow().schema(),
          inputs.wide().schema(),
          "the wide dataset should have a wider schema than the narrow one for " + entry.getKey());
      for (final TestCase testCase : entry.getValue()) {
        final Optional<Column> column = compile(inputs.resourceType(), testCase.expression());
        if (column.isEmpty()) {
          uncompiled++;
          continue;
        }
        final String narrow = outcome(inputs.narrow(), column.get(), inputs.id());
        final String wide = outcome(inputs.wide(), column.get(), inputs.id());
        compared++;
        if (!narrow.equals(wide)) {
          mismatches.add(testCase + "\n    narrow: " + narrow + "\n    wide:   " + wide);
        }
        // Every branch of the traversal yields the same type, so the column's type does not depend
        // on the width of the schema either.
        final Optional<DataType> narrowType = type(inputs.narrow(), column.get());
        final Optional<DataType> wideType = type(inputs.wide(), column.get());
        if (!narrowType.equals(wideType)) {
          mismatches.add(
              testCase + "\n    narrow type: " + narrowType + "\n    wide type:   " + wideType);
        }
      }
    }

    assertTrue(compared > 500, "expected the test set to yield many comparisons, got " + compared);
    assertTrue(
        mismatches.isEmpty(),
        mismatches.size()
            + " of "
            + compared
            + " cases differ ("
            + uncompiled
            + " did not compile):\n"
            + String.join("\n", mismatches));
  }

  /**
   * A column over an absent primitive element, reaching a caller through the expression-to-column
   * API, carries the element's type rather than the null type, and can be written to Parquet
   * (FR-030).
   */
  @Test
  void absentPrimitiveColumnIsTypedAndWritableToParquet() {
    final Dataset<Row> present =
        encode(
            "Patient",
            """
            {"resourceType":"Patient","id":"p1","birthDate":"1990-01-01","active":true,
             "name":[{"family":"Smith","given":["John"],"prefix":["Mr"],
                      "period":{"start":"2000-01-01"}}],
             "address":[{"line":["1 Main St"]}],
             "maritalStatus":{"text":"married","coding":[{"code":"M"}]},
             "contact":[{"name":{"family":"Jones"}}]}\
            """);
    final Dataset<Row> absent =
        encode("Patient", "{\"resourceType\":\"Patient\",\"id\":\"p2\",\"gender\":\"male\"}");
    // The parents are present, and the elements beneath them absent.
    final Dataset<Row> partial =
        encode(
            "Patient",
            """
            {"resourceType":"Patient","id":"p3","name":[{"given":["Ann"]}],
             "maritalStatus":{"coding":[{"system":"http://example.org"}]},
             "contact":[{"name":{"given":["Bob"]}}]}\
            """);

    final List<String> failures = new ArrayList<>();
    for (final String expression :
        List.of(
            "birthDate",
            "active",
            "name.given",
            "name.prefix",
            "name.family",
            "name.period.start",
            "address.line",
            "maritalStatus.text",
            "maritalStatus.coding.code",
            "contact.name.family")) {
      final Column column = pathling.fhirPathToColumn("Patient", expression);
      final Dataset<Row> overAbsent = absent.select(column.alias(RESULT));
      final DataType absentType = overAbsent.schema().apply(RESULT).dataType();
      final DataType presentType =
          present.select(column.alias(RESULT)).schema().apply(RESULT).dataType();
      final DataType partialType =
          partial.select(column.alias(RESULT)).schema().apply(RESULT).dataType();
      if (!presentType.equals(partialType)) {
        failures.add(
            expression
                + ": "
                + partialType
                + " where only the parent is present, "
                + presentType
                + " where the element is");
      }
      // The column has the type it has where the element is present, including the arrays a
      // repeating parent adds, whether or not the parent itself is present (decision 88).
      if (DataTypes.NullType.equals(absentType) || !presentType.equals(absentType)) {
        failures.add(
            expression + ": " + absentType + " where absent, " + presentType + " where present");
        continue;
      }
      final String target = temporaryDirectory.resolve(expression).toString();
      overAbsent.write().parquet(target);
      final List<Row> written = spark.read().parquet(target).collectAsList();
      if (written.size() != 1 || !written.getFirst().isNullAt(0)) {
        failures.add(expression + ": written as " + written);
      }
    }
    assertTrue(failures.isEmpty(), String.join("\n", failures));
  }

  // The test set.

  /** A case of the test set: an expression over an input resource. */
  private record TestCase(String file, String description, String inputFile, String expression) {

    @Override
    @Nonnull
    public String toString() {
      return file + " / " + description + " / " + inputFile + ": " + expression;
    }
  }

  @Nonnull
  private static Stream<TestCase> cases(@Nonnull final String file) {
    final JsonNode root;
    try (final InputStream input = resource(CASES + file)) {
      root = new ObjectMapper(new YAMLFactory()).readTree(input);
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
    final List<TestCase> cases = new ArrayList<>();
    collect(file, root, cases);
    return cases.stream();
  }

  /** Collects every object carrying an expression and an input file, at any depth. */
  private static void collect(
      @Nonnull final String file,
      @Nullable final JsonNode node,
      @Nonnull final List<TestCase> cases) {
    if (node == null) {
      return;
    }
    if (node.isObject()
        && node.path("expression").isTextual()
        && node.path("inputfile").isTextual()) {
      cases.add(
          new TestCase(
              file,
              node.path("desc").asText(""),
              node.get("inputfile").asText(),
              node.get("expression").asText()));
    }
    node.elements().forEachRemaining(child -> collect(file, child, cases));
  }

  // The two datasets.

  /**
   * The case's resource stored alone, and stored with the other resources of its type and a sibling
   * populating elements commonly absent, each in its fitted schema.
   */
  private record InputPair(String resourceType, String id, Dataset<Row> narrow, Dataset<Row> wide) {

    @Nonnull
    static InputPair of(@Nonnull final String inputFile) {
      final ObjectNode input = readResource(inputFile);
      final String resourceType = input.get("resourceType").asText();
      final String id = Optional.ofNullable(input.get("id")).map(JsonNode::asText).orElse(null);

      final List<String> siblings = new ArrayList<>();
      final List<ObjectNode> others =
          RESOURCE_FILES.stream()
              .filter(file -> !file.equals(inputFile))
              .map(FhirPathToColumnTest::readResource)
              .filter(other -> resourceType.equals(other.path("resourceType").asText()))
              .toList();
      IntStream.range(0, others.size())
          .forEach(i -> siblings.add(others.get(i).put("id", "sibling-" + i).toString()));
      siblings.add(widening(resourceType));

      final List<String> wide = new ArrayList<>(siblings);
      wide.addFirst(input.toString());
      return new InputPair(
          resourceType,
          id,
          encode(resourceType, input.toString()).cache(),
          encode(resourceType, wide.toArray(String[]::new)).cache());
    }
  }

  /** A resource populating elements every resource type has and few examples carry. */
  @Nonnull
  private static String widening(@Nonnull final String resourceType) {
    return """
    {"resourceType":"%s","id":"widening-sibling","implicitRules":"http://example.org/rules",
     "meta":{"versionId":"7","source":"http://example.org/source",
             "security":[{"system":"http://example.org/security","code":"s"}]},
     "extension":[{"url":"http://example.org/widening",
                   "valueQuantity":{"value":1.5,"unit":"mg","code":"mg"},
                   "extension":[{"url":"http://example.org/nested",
                                 "valueCodeableConcept":{"text":"nested"}}]}],
     "modifierExtension":[{"url":"http://example.org/modifier","valueBoolean":true}]}\
    """
        .formatted(resourceType)
        .replace("\n", "");
  }

  @Nonnull
  private static Dataset<Row> encode(
      @Nonnull final String resourceType, @Nonnull final String... documents) {
    return pathling.encode(
        spark.createDataset(List.of(documents), Encoders.STRING()).toDF(), resourceType);
  }

  // Evaluation.

  @Nonnull
  private static Optional<Column> compile(
      @Nonnull final String resourceType, @Nonnull final String expression) {
    try {
      return Optional.of(pathling.fhirPathToColumn(resourceType, expression));
    } catch (final Exception e) {
      return Optional.empty();
    }
  }

  /**
   * Returns the result of a column for one resource, as JSON from which nulls are left out, so that
   * a structure is compared by the values it carries rather than by the width of its type; or the
   * class of the failure, where the column fails.
   */
  @Nonnull
  private static String outcome(
      @Nonnull final Dataset<Row> dataset,
      @Nonnull final Column column,
      @Nullable final String id) {
    // A resource without an id is stored without an id column when it is stored alone.
    final boolean hasIds = List.of(dataset.columns()).contains("id");
    final Column selected =
        id == null
            ? (hasIds ? functions.col("id").isNull() : functions.lit(true))
            : functions.col("id").equalTo(id);
    try {
      return dataset
          .filter(selected)
          .select(functions.to_json(functions.struct(column.alias(RESULT))).alias(RESULT))
          .as(Encoders.STRING())
          .collectAsList()
          .toString();
    } catch (final Exception e) {
      return "failed: " + rootCause(e).getClass().getName() + firstLine(rootCause(e).getMessage());
    }
  }

  /** Returns the type of a column over a dataset, or empty where the column does not resolve. */
  @Nonnull
  private static Optional<DataType> type(
      @Nonnull final Dataset<Row> dataset, @Nonnull final Column column) {
    try {
      return Optional.of(dataset.select(column.alias(RESULT)).schema().apply(RESULT).dataType());
    } catch (final Exception e) {
      return Optional.empty();
    }
  }

  @Nonnull
  private static String firstLine(@Nullable final String message) {
    return message == null ? "" : " (" + message.lines().findFirst().orElse("") + ")";
  }

  @Nonnull
  private static Throwable rootCause(@Nonnull final Throwable throwable) {
    Throwable cause = throwable;
    while (cause.getCause() != null && !Objects.equals(cause.getCause(), cause)) {
      cause = cause.getCause();
    }
    return cause;
  }

  @Nonnull
  private static ObjectNode readResource(@Nonnull final String file) {
    try (final InputStream input = resource(RESOURCES + file)) {
      return (ObjectNode) JSON.readTree(new String(input.readAllBytes(), StandardCharsets.UTF_8));
    } catch (final IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Nonnull
  private static InputStream resource(@Nonnull final String name) {
    return Objects.requireNonNull(
        FhirPathToColumnTest.class.getClassLoader().getResourceAsStream(name),
        "missing test resource " + name);
  }
}
