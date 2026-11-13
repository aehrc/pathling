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

import static au.csiro.pathling.views.FhirView.columns;
import static au.csiro.pathling.views.FhirView.forEach;
import static au.csiro.pathling.views.FhirView.unionAll;
import static org.apache.spark.sql.functions.array;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.when;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import au.csiro.pathling.definition.fhir.FhirDefinitionContext;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhirpath.evaluation.DatasetEvaluator;
import au.csiro.pathling.fhirpath.evaluation.DatasetEvaluatorBuilder;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.io.Annotator;
import au.csiro.pathling.io.FhirReader;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.schema.SchemaConfiguration;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.datasource.PrunedSchemaReader;
import au.csiro.pathling.test.layout.PofDatasets;
import au.csiro.pathling.views.Column;
import au.csiro.pathling.views.FhirView;
import au.csiro.pathling.views.FhirViewExecutor;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.Alias;
import org.apache.spark.sql.catalyst.expressions.ArrayTransform;
import org.apache.spark.sql.catalyst.expressions.ArraysZip;
import org.apache.spark.sql.catalyst.expressions.Coalesce;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.GetArrayStructFields;
import org.apache.spark.sql.catalyst.expressions.IsNotNull;
import org.apache.spark.sql.catalyst.expressions.IsNull;
import org.apache.spark.sql.catalyst.expressions.Size;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.FileSourceScanExec;
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
import org.springframework.beans.factory.annotation.Autowired;
import scala.jdk.javaapi.CollectionConverters;

/**
 * Tests the shapes the engine emits so that Spark can prune the read (decision 143, L2; design §8,
 * ticket 51), on the Spark version Pathling builds on.
 *
 * <ul>
 *   <li><b>Existence tests.</b> {@code exists()}, {@code count()} and {@code empty()}, with or
 *       without criteria and in a view's {@code where}, never take the size of, or test the
 *       nullness of, an array of whole elements. Their expected results were recorded from the
 *       engine before the forms were rewritten, over data with an absent array, an empty one, null
 *       elements and elements whose read fields are all null.
 *   <li><b>Pairing a repeating primitive.</b> Its values are zipped with the extracted metadata and
 *       annotation arrays under the names of the singular form, with no projection in between, so
 *       that the read names the fields of the metadata group, not the group.
 * </ul>
 *
 * <p>On Spark 4.0 the read of an array is not pruned through a lambda, so a pruned read is asserted
 * only where Spark 4.0 prunes it: a repeating primitive under a singular parent. Elsewhere the
 * shape is asserted (decision 143). The third shape of decision 143, computing a view's result
 * before generating its rows, is not emitted until Pathling builds on a Spark that prunes through
 * it (owner, 2026-11-12; design §8).
 */
@SpringBootUnitTest
@TestInstance(Lifecycle.PER_CLASS)
class EmissionShapesTest {

  private static final Parser PARSER = new Parser();

  private static final List<String> PATIENTS =
      List.of(
          "{\"resourceType\":\"Patient\",\"id\":\"p1\",\"name\":[{\"use\":\"official\","
              + "\"family\":\"A\",\"given\":[\"X\"]},{\"family\":\"B\",\"given\":[\"Y\",\"Z\"]}],"
              + "\"address\":[{\"city\":\"C1\"}]}",
          "{\"resourceType\":\"Patient\",\"id\":\"p2\"}",
          "{\"resourceType\":\"Patient\",\"id\":\"p3\",\"name\":[{\"use\":\"nickname\","
              + "\"family\":\"C\"}],\"address\":[{\"city\":\"C2\"}]}",
          "{\"resourceType\":\"Patient\",\"id\":\"p4\",\"name\":[{\"given\":[\"W\"]}]}",
          "{\"resourceType\":\"Patient\",\"id\":\"p5\",\"name\":[{\"use\":\"official\"}]}",
          "{\"resourceType\":\"Patient\",\"id\":\"p6\",\"name\":[{\"use\":\"old\"}]}");

  private static final String EXTENSION_TYPE =
      "extension:array<struct<url:string,valueString:string>>";

  private static final String EXTENSION =
      "{\"extension\":[{\"url\":\"http://example.org/source\",\"valueString\":\"device\"}]}";

  private static final List<String> OBSERVATIONS =
      List.of(
          "{\"resourceType\":\"Observation\",\"id\":\"o1\",\"status\":\"final\","
              + "\"code\":{\"text\":\"x\"},\"effectiveTiming\":{\"event\":"
              + "[\"2020-01-01\",\"2020-02\",\"2019\"],\"_event\":["
              + EXTENSION
              + ",null,"
              + EXTENSION
              + "]}}",
          "{\"resourceType\":\"Observation\",\"id\":\"o2\",\"status\":\"final\","
              + "\"code\":{\"text\":\"x\"},\"effectiveDateTime\":\"2020-03\","
              + "\"_effectiveDateTime\":"
              + EXTENSION
              + "}");

  @Autowired SparkSession spark;

  @Autowired FhirEncoders fhirEncoders;

  @TempDir static Path tempDir;

  private Dataset<Row> patients;

  private Dataset<Row> observationsRead;

  private Dataset<Row> observationsAnnotated;

  @BeforeAll
  void setUp() {
    // The edge cases are made in the stored rows directly: p3 has null names around its name, p5
    // an empty array of names and p6 an array holding only a null name. p2 has no names, and p4 a
    // name whose use and family are null.
    final Dataset<Row> raw =
        PofDatasets.fromJson(spark, fhirEncoders.getContext(), "Patient", PATIENTS);
    final DataType element = ((ArrayType) raw.schema().apply("name").dataType()).elementType();
    final org.apache.spark.sql.Column nullName = lit(null).cast(element);
    final Dataset<Row> edited =
        raw.withColumn(
            "name",
            when(col("id").equalTo("p3"), array(nullName, col("name").getItem(0), nullName))
                .when(
                    col("id").equalTo("p5"),
                    org.apache.spark.sql.functions.filter(col("name"), name -> lit(false)))
                .when(col("id").equalTo("p6"), array(nullName))
                .otherwise(col("name")));
    patients = PrunedSchemaReader.write(edited, tempDir.resolve("patients").toString()).read();

    // The observations keep each primitive's extensions in its metadata group, so that a read
    // that names only the annotations is seen to leave them out.
    final FhirDefinitionContext definitions = FhirDefinitionContext.of(fhirEncoders.getContext());
    final Dataset<Row> observations =
        FhirReader.of(
                spark,
                definitions,
                SchemaConfiguration.builder().storePrimitiveMetadata(true).build())
            .json()
            .read("Observation", spark.createDataset(OBSERVATIONS, Encoders.STRING()));
    observationsAnnotated =
        PrunedSchemaReader.write(
                Annotator.of(definitions, SchemaConfiguration.builder().build())
                    .annotate("Observation", observations),
                tempDir.resolve("observations-annotated").toString())
            .read();
    observationsRead =
        PrunedSchemaReader.write(
                PofDatasets.fromJson(spark, fhirEncoders.getContext(), "Observation", OBSERVATIONS),
                tempDir.resolve("observations-read").toString())
            .read();
  }

  // Existence tests.

  @Nonnull
  Stream<Arguments> existenceTests() {
    return Stream.of(
        arguments("name.exists()", "p1=true p2=false p3=true p4=true p5=false p6=false"),
        arguments("name.count()", "p1=2 p2=0 p3=1 p4=1 p5=0 p6=0"),
        arguments("name.empty()", "p1=false p2=true p3=false p4=false p5=true p6=true"),
        arguments(
            "name.where(use = 'official').exists()",
            "p1=true p2=false p3=false p4=false p5=false p6=false"),
        arguments("name.where(use = 'official').count()", "p1=1 p2=0 p3=0 p4=0 p5=0 p6=0"),
        arguments(
            "name.where(use = 'official').empty()",
            "p1=false p2=true p3=true p4=true p5=true p6=true"),
        arguments(
            "name.exists(use = 'official')",
            "p1=true p2=false p3=false p4=false p5=false p6=false"),
        arguments(
            "name.exists(use != 'official')",
            "p1=false p2=false p3=true p4=false p5=false p6=false"),
        arguments(
            "name.exists(use.empty())", "p1=true p2=false p3=false p4=true p5=false p6=false"),
        arguments(
            "name.exists(family.exists())", "p1=true p2=false p3=true p4=false p5=false p6=false"),
        arguments(
            "name.all(use = 'official')", "p1=false p2=true p3=false p4=false p5=true p6=true"),
        arguments(
            "name.all(use != 'official')", "p1=false p2=true p3=true p4=false p5=true p6=true"),
        arguments("name.all(use.empty())", "p1=false p2=true p3=false p4=true p5=true p6=true"),
        arguments("name.all(family.exists())", "p1=true p2=true p3=true p4=false p5=true p6=true"),
        arguments("name.where(use.empty()).count()", "p1=1 p2=0 p3=0 p4=1 p5=0 p6=0"),
        arguments("name.where(given.exists()).given.count()", "p1=3 p2=0 p3=0 p4=1 p5=0 p6=0"),
        arguments(
            "name.where(given.where($this = 'Y').exists()).family.count()",
            "p1=1 p2=0 p3=0 p4=0 p5=0 p6=0"),
        arguments("name.given.exists()", "p1=true p2=false p3=false p4=true p5=false p6=false"),
        arguments("name.given.count()", "p1=3 p2=0 p3=0 p4=1 p5=0 p6=0"),
        arguments("(name | name).count()", "p1=2 p2=0 p3=1 p4=1 p5=0 p6=0"),
        arguments("name.exists().not()", "p1=false p2=true p3=false p4=false p5=true p6=true"),
        arguments(
            "address.exists(city = 'C1')", "p1=true p2=false p3=false p4=false p5=false p6=false"));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("existenceTests")
  void anExistenceTestNeverTakesTheSizeOfWholeElements(
      @Nonnull final String expression, @Nonnull final String expected) {
    final Dataset<Row> result = evaluate(patients, ResourceType.PATIENT, expression);
    assertThat(String.join(" ", rows(result))).isEqualTo(expected);
    assertThat(testsOfWholeElements(result.queryExecution().optimizedPlan())).isEmpty();
  }

  @Nonnull
  Stream<Arguments> viewFilters() {
    return Stream.of(
        arguments("name.exists(use = 'official')", List.of("[p1,A]", "[p1,B]", "[p1,C1]")),
        arguments(
            "name.where(family.exists()).exists()",
            List.of("[p1,A]", "[p1,B]", "[p1,C1]", "[p3,C2]", "[p3,C]")),
        arguments(
            "name.where(use = 'official').empty()", List.of("[p3,C2]", "[p3,C]", "[p4,null]")));
  }

  /**
   * A view with a {@code where} and a {@code unionAll} of two {@code forEach}: its filter tests no
   * whole element.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("viewFilters")
  void aViewFilterTestsNoWholeElement(
      @Nonnull final String where, @Nonnull final List<String> expected) {
    final Dataset<Row> result =
        view(
            Map.of("Patient", patients),
            FhirView.ofResource("Patient")
                .where(where)
                .select(
                    columns(Column.single("id", "id")),
                    unionAll(
                        forEach("name", Column.single("v", "family")),
                        forEach("address", Column.single("v", "city"))))
                .build());
    assertThat(result.collectAsList().stream().map(Row::toString).sorted().toList())
        .containsExactlyElementsOf(expected);
    assertThat(testsOfWholeElements(result.queryExecution().optimizedPlan())).isEmpty();
  }

  // Pairing a repeating primitive.

  @Test
  void aRepeatingPrimitiveCarriesTheFieldsOfTheSingularForm() {
    final String repeating =
        evaluate(observationsAnnotated, ResourceType.OBSERVATION, "effective.ofType(Timing).event")
            .schema()
            .apply("value")
            .dataType()
            .simpleString();
    final String singular =
        evaluate(observationsAnnotated, ResourceType.OBSERVATION, "effective.ofType(dateTime)")
            .schema()
            .apply("value")
            .dataType()
            .simpleString();
    assertThat(singular)
        .isEqualTo("struct<value:string," + EXTENSION_TYPE + ",__start:timestamp,__end:timestamp>");
    assertThat(repeating).isEqualTo("array<" + singular + ">");
  }

  /**
   * The values are zipped directly with the metadata and annotation arrays extracted from the
   * metadata groups, each extracted array guarded against an absent array of groups and nothing
   * else, and no alias or projection stands between them: each is a read of a named field of the
   * groups, never of the group itself, which {@link #assertPairedDirectly} checks in the plan. The
   * read schema cannot show it here, because the fixture's group holds only the fields that are
   * read. The extensions are read with the annotations, because they decide whether a position
   * without a value is an element (decision 154).
   */
  @Test
  void aRepeatingPrimitiveIsPairedWithItsMetadataAndAnnotations() {
    final String expression = "effective.ofType(Timing).event.where($this > @2019-06-01).count()";
    final Dataset<Row> annotated =
        evaluate(observationsAnnotated, ResourceType.OBSERVATION, expression);
    assertThat(rows(annotated)).containsExactly("o1=2", "o2=0");
    assertThat(rows(evaluate(observationsRead, ResourceType.OBSERVATION, expression)))
        .containsExactly("o1=2", "o2=0");

    final List<ArraysZip> zips =
        all(annotated.queryExecution().optimizedPlan())
            .filter(ArraysZip.class::isInstance)
            .map(ArraysZip.class::cast)
            .toList();
    assertThat(zips).isNotEmpty().allSatisfy(EmissionShapesTest::assertPairedDirectly);

    assertThat(observationsAnnotated.schema().simpleString())
        .contains("_event:array<struct<extension:");
    assertThat(readSchema(annotated))
        .contains("_event:array<struct<" + EXTENSION_TYPE + ",__start:timestamp,__end:timestamp>>");
  }

  /**
   * Asserts that a pairing zips the values with the metadata and annotation arrays under the
   * singular names, each array extracted from the metadata groups and guarded against their
   * absence, with no alias between, and no guard around a transform.
   */
  private static void assertPairedDirectly(@Nonnull final ArraysZip zip) {
    assertThat(CollectionConverters.asJava(zip.names()).stream().map(String::valueOf))
        .containsExactly("value", "extension", "__start", "__end");
    final List<Expression> inputs = CollectionConverters.asJava(zip.children());
    assertThat(inputs).noneMatch(Alias.class::isInstance);
    assertThat(inputs.subList(1, inputs.size()))
        .allSatisfy(
            input -> {
              assertThat(input).isInstanceOf(Coalesce.class);
              assertThat(((Coalesce) input).children().head())
                  .isInstanceOf(GetArrayStructFields.class);
            });
    assertThat(inputs.stream().flatMap(EmissionShapesTest::descendants))
        .noneMatch(
            input ->
                input instanceof Coalesce
                    && CollectionConverters.asJava(input.children()).stream()
                        .anyMatch(ArrayTransform.class::isInstance));
  }

  /**
   * The same pairing, output by a view. The read schema shows that the extensions and annotations
   * are read; that they are read as named fields, not as the group, is checked by {@link
   * #aRepeatingPrimitiveIsPairedWithItsMetadataAndAnnotations}.
   */
  @Test
  void aViewOfARepeatingPrimitiveReadsItsMetadataAndAnnotations() {
    final Dataset<Row> result =
        view(
            Map.of("Observation", observationsAnnotated),
            FhirView.ofResource("Observation")
                .select(
                    columns(
                        Column.single("id", "id"),
                        Column.collectionBuilder("event", "effective.ofType(Timing).event").build(),
                        Column.single(
                            "late",
                            "effective.ofType(Timing).event.where($this > @2019-06-01).exists()")))
                .build());
    assertThat(result.collectAsList().stream().map(Row::toString).sorted().toList())
        .containsExactly("[o1,ArraySeq(2020-01-01, 2020-02, 2019),true]", "[o2,ArraySeq(),false]");
    assertThat(readSchema(result))
        .contains("_event:array<struct<" + EXTENSION_TYPE + ",__start:timestamp,__end:timestamp>>");
  }

  /**
   * The size and null tests in a plan that are taken of an array of whole elements, which Spark's
   * schema pruning reads in full. An array of primitives as the engine pairs them, each a structure
   * of its value and the fields it names from its metadata group (decision 154), is not one: it is
   * built from the fields the expression reads, not read whole.
   */
  @Nonnull
  private static List<Expression> testsOfWholeElements(@Nonnull final LogicalPlan plan) {
    return all(plan)
        .filter(
            expression ->
                expression instanceof Size
                    || expression instanceof IsNull
                    || expression instanceof IsNotNull)
        .filter(test -> isArrayOfStructures(test.children().head().dataType()))
        .toList();
  }

  private static boolean isArrayOfStructures(@Nonnull final DataType type) {
    return type instanceof final ArrayType array
        && array.elementType() instanceof final StructType structure
        && !isPairedPrimitive(structure);
  }

  /** Whether a structure is a primitive as the engine pairs it with its metadata group. */
  private static boolean isPairedPrimitive(@Nonnull final StructType structure) {
    // A paired primitive has its value; a metadata group read whole, which has none, is not one.
    final List<String> names = Arrays.asList(structure.fieldNames());
    return names.contains("value")
        && names.stream()
            .allMatch(
                name ->
                    List.of("value", "id", "extension").contains(name) || name.startsWith("__"));
  }

  @Nonnull
  private static Stream<LogicalPlan> nodes(@Nonnull final LogicalPlan plan) {
    return Stream.concat(
        Stream.of(plan),
        CollectionConverters.asJava(plan.children()).stream().flatMap(EmissionShapesTest::nodes));
  }

  /** Every expression in a plan, at every depth. */
  @Nonnull
  private static Stream<Expression> all(@Nonnull final LogicalPlan plan) {
    return nodes(plan)
        .flatMap(node -> CollectionConverters.asJava(node.expressions()).stream())
        .flatMap(EmissionShapesTest::descendants);
  }

  @Nonnull
  private static Stream<Expression> descendants(@Nonnull final Expression expression) {
    return Stream.concat(
        Stream.of(expression),
        CollectionConverters.asJava(expression.children()).stream()
            .flatMap(EmissionShapesTest::descendants));
  }

  @Nonnull
  private static String readSchema(@Nonnull final Dataset<Row> query) {
    return CollectionConverters.asJava(query.queryExecution().executedPlan().collectLeaves())
        .stream()
        .filter(FileSourceScanExec.class::isInstance)
        .map(FileSourceScanExec.class::cast)
        .findFirst()
        .orElseThrow()
        .requiredSchema()
        .simpleString();
  }

  @Nonnull
  private Dataset<Row> evaluate(
      @Nonnull final Dataset<Row> data,
      @Nonnull final ResourceType resourceType,
      @Nonnull final String expression) {
    final DatasetEvaluator evaluator =
        DatasetEvaluatorBuilder.create(resourceType, fhirEncoders.getContext())
            .withDataset(data)
            .build();
    return evaluator.evaluate(PARSER.parse(expression)).toIdValueDataset();
  }

  @Nonnull
  private Dataset<Row> view(
      @Nonnull final Map<String, Dataset<Row>> data, @Nonnull final FhirView view) {
    final DataSource source =
        new DataSource() {
          @Nonnull
          @Override
          public Dataset<Row> read(@Nullable final String resourceCode) {
            return data.get(resourceCode);
          }

          @Nonnull
          @Override
          public Set<String> getResourceTypes() {
            return data.keySet();
          }
        };
    return new FhirViewExecutor(fhirEncoders.getContext(), source).buildQuery(view);
  }

  /** The rows of an id and value dataset, as {@code id=value}, sorted by id. */
  @Nonnull
  private static List<String> rows(@Nonnull final Dataset<Row> result) {
    return result.collectAsList().stream()
        .map(row -> row.getString(0) + "=" + row.get(1))
        .sorted()
        .toList();
  }
}
