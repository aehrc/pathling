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

import au.csiro.pathling.fhirpath.encoding.CodingSchema;
import au.csiro.pathling.fhirpath.evaluation.DatasetEvaluator;
import au.csiro.pathling.fhirpath.evaluation.DatasetEvaluatorBuilder;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.terminology.TerminologyService.Translation;
import au.csiro.pathling.test.SharedMocks;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.helpers.TerminologyServiceHelpers;
import au.csiro.pathling.test.layout.PofDatasets;
import au.csiro.pathling.utilities.SparkTypes;
import ca.uhn.fhir.context.FhirContext;
import jakarta.annotation.Nonnull;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.codesystems.ConceptMapEquivalence;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * Pins the shape of a {@code System.Coding} that the engine builds itself (FR-059).
 *
 * <p>Whatever produces it, such a Coding has one fixed, complete shape: the elements of a FHIR
 * Coding other than {@code extension}, in definition order. It carries no {@code _fid}, the field
 * identifier of the previous layout.
 *
 * <p>A stored Coding combined with an engine-built one is reconciled with it by name and keeps the
 * fields of the stored Coding (decision 79's addendum), so its shape is not fixed; it is only
 * checked to carry no {@code _fid}.
 *
 * @author Piotr Szul
 */
@SpringBootUnitTest
@TestInstance(Lifecycle.PER_CLASS)
class SystemCodingShapeTest {

  private static final Parser PARSER = new Parser();

  private static final String SYSTEM = "http://example.org/system";

  private static final String CONCEPT_MAP = "http://example.org/ConceptMap/cm";

  /** The fixed shape of an engine-built System.Coding, written out field by field. */
  private static final StructType SYSTEM_CODING =
      new StructType(
          new StructField[] {
            DataTypes.createStructField("id", DataTypes.StringType, true),
            DataTypes.createStructField("system", DataTypes.StringType, true),
            DataTypes.createStructField("version", DataTypes.StringType, true),
            DataTypes.createStructField("code", DataTypes.StringType, true),
            DataTypes.createStructField("display", DataTypes.StringType, true),
            DataTypes.createStructField("userSelected", DataTypes.BooleanType, true)
          });

  private static final String OBSERVATION =
      "{\"resourceType\":\"Observation\",\"id\":\"o1\",\"status\":\"final\","
          + "\"code\":{\"coding\":[{\"system\":\""
          + SYSTEM
          + "\",\"code\":\"a\",\"extension\":[{\"url\":\"http://example.org/e\","
          + "\"valueString\":\"e\"}]}]}}";

  @Autowired SparkSession spark;

  @Autowired FhirContext fhirContext;

  @Autowired TerminologyService terminologyService;

  private Dataset<Row> dataset;

  @BeforeAll
  void setUp() {
    dataset = PofDatasets.fromJson(spark, fhirContext, "Observation", List.of(OBSERVATION));
  }

  @BeforeEach
  void setUpTerminology() {
    SharedMocks.resetAll();
    TerminologyServiceHelpers.setupTranslate(terminologyService)
        .withTranslations(
            new Coding(SYSTEM, "a", null),
            CONCEPT_MAP,
            Translation.of(ConceptMapEquivalence.EQUIVALENT, new Coding(SYSTEM, "b", "Bee")));
  }

  @Nonnull
  Stream<Arguments> producers() {
    return Stream.of(
        arguments("a literal", SYSTEM + "|a"),
        arguments("a literal with every field", SYSTEM + "|a|v1|'Alpha'|true"),
        arguments("a union of two literals", SYSTEM + "|a | " + SYSTEM + "|b"),
        arguments("a terminology result", "code.coding.translate('" + CONCEPT_MAP + "')"));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("producers")
  void engineBuiltCodingHasTheFixedShape(
      @Nonnull final String producer, @Nonnull final String expression) {
    // A literal's fields are not nullable where the literal populates them, which is not part of
    // the shape, so nullability is set aside.
    assertThat(nullable((StructType) elementTypeOf(expression))).isEqualTo(SYSTEM_CODING);
  }

  @Test
  void canonicalCodingStructureHasTheFixedShape() {
    assertThat(CodingSchema.DATA_TYPE).isEqualTo(SYSTEM_CODING);
    assertThat(CodingSchema.codingStructType()).isEqualTo(SYSTEM_CODING);
  }

  @Nonnull
  Stream<Arguments> combinations() {
    return Stream.of(
        arguments("a union of a stored Coding with a literal", "code.coding | " + SYSTEM + "|x"),
        arguments("a union of a literal with a stored Coding", SYSTEM + "|x | code.coding"));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("combinations")
  void codingCombinedWithAStoredOneCarriesNoFieldIdentifier(
      @Nonnull final String combination, @Nonnull final String expression) {
    assertThat(((StructType) elementTypeOf(expression)).fieldNames()).doesNotContain("_fid");
  }

  /** Returns a structure with the same fields, each of them nullable. */
  @Nonnull
  private static StructType nullable(@Nonnull final StructType struct) {
    return new StructType(
        Arrays.stream(struct.fields())
            .map(field -> DataTypes.createStructField(field.name(), field.dataType(), true))
            .toArray(StructField[]::new));
  }

  @Nonnull
  private DataType elementTypeOf(@Nonnull final String expression) {
    return SparkTypes.elementTypeOf(
        dataset
            .select(evaluator().evaluateToCollection(PARSER.parse(expression)).getColumnValue())
            .schema()
            .fields()[0]
            .dataType());
  }

  @Nonnull
  private DatasetEvaluator evaluator() {
    return DatasetEvaluatorBuilder.create(ResourceType.OBSERVATION, fhirContext)
        .withDataset(dataset)
        .build();
  }
}
