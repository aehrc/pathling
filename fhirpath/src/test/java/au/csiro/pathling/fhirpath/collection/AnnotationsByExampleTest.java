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
import static org.apache.spark.sql.functions.struct;
import static org.apache.spark.sql.functions.transform;
import static org.apache.spark.sql.functions.when;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import au.csiro.pathling.definition.fhir.FhirDefinitionContext;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhirpath.evaluation.DatasetEvaluator;
import au.csiro.pathling.fhirpath.evaluation.DatasetEvaluatorBuilder;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.io.Annotator;
import au.csiro.pathling.schema.SchemaConfiguration;
import au.csiro.pathling.search.SearchColumnBuilder;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.datasource.PrunedSchemaReader;
import au.csiro.pathling.test.layout.PofDatasets;
import jakarta.annotation.Nonnull;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
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

/**
 * The examples of `design/annotations-by-example.md` §3 to §5, over the example resource of its §1
 * (ticket 51): each engine form, FHIR and System, for annotated and unannotated data, and how the
 * forms connect in output and in merges.
 *
 * <p>Each case names the example's section. The forms are the engine's own, as an evaluation
 * returns them: Spark prints a timestamp in the session's time zone, UTC in the tests, and a
 * structure's fields in order without their names. Examples whose behaviour is tested in detail
 * elsewhere are shown here once: a decimal that does not decode and a quantity that does not
 * convert ({@code AnnotatedTypesParityTest}), the comparison key of a complex value ({@code
 * ComparisonKeyTest}) and a date's output ({@code DateDeduplicationTest}).
 *
 * <p>A primitive carries the id and extensions the data stores for it (decision 154), which {@code
 * PrimitiveMetadataTest} shows in detail (tickets 52 and 54). Not covered here: the System.Coding
 * of §4.4, whose shape without {@code id} (decision 150) the engine does not build.
 */
@SpringBootUnitTest
@TestInstance(Lifecycle.PER_CLASS)
class AnnotationsByExampleTest {

  private static final Parser PARSER = new Parser();

  private static final String UCUM = "http://unitsofmeasure.org";

  /** The example resource of §1, and two quantities that §4.3 shows. */
  private static final List<String> OBSERVATIONS =
      List.of(
          "{\"resourceType\":\"Observation\",\"id\":\"obs1\",\"status\":\"final\","
              + "\"code\":{\"coding\":[{\"id\":\"c1\",\"extension\":[{\"url\":"
              + "\"http://example.org/origin\",\"valueString\":\"lab\"}],"
              + "\"system\":\"http://loinc.org\",\"code\":\"29463-7\","
              + "\"display\":\"Body weight\"}]},"
              + "\"effectiveDateTime\":\"2024-03-15T10:30+10:00\","
              + "\"_effectiveDateTime\":{\"extension\":[{\"url\":\"http://example.org/source\","
              + "\"valueString\":\"device\"}]},"
              + "\"issued\":\"2024-03-15T10:31:12.345+10:00\","
              + "\"valueQuantity\":{\"extension\":[{\"url\":\"http://example.org/method\","
              + "\"valueString\":\"scale\"}],"
              + quantityFields("72.50", "kg")
              + "},\"component\":[{\"code\":{\"text\":\"height\"},\"valueQuantity\":{"
              + quantityFields("180", "cm")
              + "}},{\"code\":{\"text\":\"note\"}}]}",
          "{\"resourceType\":\"Observation\",\"id\":\"obs2\",\"status\":\"final\","
              + "\"code\":{\"text\":\"x\"},\"valueQuantity\":{"
              + quantityFields("1", "ns")
              + "}}",
          "{\"resourceType\":\"Observation\",\"id\":\"obs3\",\"status\":\"final\","
              + "\"code\":{\"text\":\"x\"},\"valueQuantity\":{\"comparator\":\"<\","
              + quantityFields("5", "mg")
              + "}}");

  private static final List<String> PATIENTS =
      List.of(
          "{\"resourceType\":\"Patient\",\"id\":\"p1\",\"birthDate\":\"1970-03\","
              + "\"name\":[{\"family\":\"Smith\",\"given\":[\"Anne\",\"Mary\"]}]}");

  @Autowired SparkSession spark;

  @Autowired FhirEncoders fhirEncoders;

  @TempDir static Path tempDir;

  /** The forms of data written with annotations, without them, and by other producers. */
  private Map<String, Dataset<Row>> data;

  @BeforeAll
  void setUp() {
    final FhirDefinitionContext definitions = FhirDefinitionContext.of(fhirEncoders.getContext());
    final Annotator annotator = Annotator.of(definitions, SchemaConfiguration.builder().build());
    // JSON is ingested through a double, so the decimal's stored text is set as the example has
    // it, before the annotations are computed from it.
    final Dataset<Row> observations =
        PofDatasets.fromJson(spark, fhirEncoders.getContext(), "Observation", OBSERVATIONS)
            .withColumn(
                "valueQuantity",
                when(
                        col("id").equalTo("obs1"),
                        col("valueQuantity").withField("value", lit("72.50")))
                    .otherwise(col("valueQuantity")));
    final Dataset<Row> annotated = annotator.annotate("Observation", observations);
    final Dataset<Row> patients =
        annotator.annotate(
            "Patient", PofDatasets.fromJson(spark, fhirEncoders.getContext(), "Patient", PATIENTS));
    data =
        Map.of(
            "annotated", store("annotated", annotated),
            "unannotated", store("unannotated", observations),
            // Another producer that writes __start but not __end.
            "partial",
                store(
                    "partial",
                    annotated.withColumn(
                        "_effectiveDateTime",
                        struct(col("_effectiveDateTime.__start").alias("__start")))),
            // Another producer's fields beside Pathling's annotations.
            "foreign",
                store(
                    "foreign",
                    patients
                        .withColumn(
                            "_birthDate", col("_birthDate").withField("__acme_hash", lit("9f2c")))
                        .withColumn(
                            "name",
                            transform(
                                col("name"), name -> name.withField("__acme_hash", lit("77ab"))))));
  }

  @Nonnull
  Stream<Arguments> examples() {
    return Stream.of(
        // §3.1 Primitives: struct(value, ...), flat.
        arguments(
            "§3.1 a dateTime over annotated data, with the extension it carries",
            "annotated",
            "effective.ofType(dateTime)",
            "struct<value:string,extension:array<struct<url:string,valueString:string>>,"
                + "__start:timestamp,__end:timestamp>",
            "[2024-03-15T10:30+10:00,ArraySeq([http://example.org/source,device]),"
                + "2024-03-15 00:30:00.0,2024-03-15 00:30:59.999999]"),
        arguments(
            "§3.1 the same over unannotated data",
            "unannotated",
            "effective.ofType(dateTime)",
            "struct<value:string,extension:array<struct<url:string,valueString:string>>>",
            "[2024-03-15T10:30+10:00,ArraySeq([http://example.org/source,device])]"),
        arguments(
            "§3.1 the same where the schema has __start but not __end",
            "partial",
            "effective.ofType(dateTime)",
            "struct<value:string,__start:timestamp>",
            "[2024-03-15T10:30+10:00,2024-03-15 00:30:00.0]"),
        arguments(
            "§2.2 an instant, whose bounds are equal",
            "annotated",
            "issued",
            "struct<value:string,__start:timestamp,__end:timestamp>",
            "[2024-03-15T10:31:12.345+10:00,2024-03-15 00:31:12.345,2024-03-15 00:31:12.345]"),
        arguments(
            "§3.1 a decimal: its stored text and its number",
            "annotated",
            "valueQuantity.value",
            "struct<value:string,__numeric:decimal(38,6)>",
            "[72.50,72.500000]"),
        arguments(
            "§3.1 the same over unannotated data",
            "unannotated",
            "valueQuantity.value",
            "struct<value:string>",
            "[72.50]"),
        arguments(
            "§3.1 a string has no annotation kind, so it is its value and the metadata it has",
            "foreign",
            "name.given",
            "array<struct<value:string>>",
            "ArraySeq([Anne], [Mary])"),
        arguments(
            "§3.1 a code is its value and the metadata it has",
            "annotated",
            "status",
            "struct<value:string>",
            "[final]"),
        arguments(
            "§3.1 another producer's annotation never enters the element",
            "foreign",
            "birthDate",
            "struct<value:string,__start:timestamp,__end:timestamp>",
            "[1970-03,1970-03-01 00:00:00.0,1970-03-31 23:59:59.999999]"),
        // §3.2 Complex types: the stored structure as it is.
        arguments(
            "§3.2 a FHIR Quantity",
            "annotated",
            "valueQuantity",
            "struct<extension:array<struct<url:string,valueString:string>>,value:string,"
                + "_value:struct<__numeric:decimal(38,6)>,comparator:string,unit:string,system:string,"
                + "code:string,__canonical_code:string,"
                + "__canonical_value_exact:struct<value:decimal(38,0),scale:int>>",
            "[ArraySeq([http://example.org/method,scale]),72.50,[72.500000],null,kg,"
                + UCUM
                + ",kg,g,[72500,0]]"),
        arguments(
            "§3.2 a FHIR Coding: only the fields the data has",
            "annotated",
            "code.coding.first()",
            "struct<id:string,extension:array<struct<url:string,valueString:string>>,"
                + "system:string,code:string,display:string>",
            "[c1,ArraySeq([http://example.org/origin,lab]),http://loinc.org,29463-7,Body weight]"),
        arguments(
            "§3.2 a HumanName with another producer's field, carried as stored",
            "foreign",
            "name.first()",
            "struct<family:string,given:array<string>,__acme_hash:string>",
            "[Smith,ArraySeq(Anne, Mary),77ab]"),
        // §4.1 Dates.
        arguments(
            "§4.1 the literal @2024-03: constants built at plan time",
            "unannotated",
            "@2024-03",
            "struct<value:string,__start:timestamp,__end:timestamp>",
            "[2024-03,2024-03-01 00:00:00.0,2024-03-31 23:59:59.999999]"),
        arguments(
            "§4.1 a function result: value only (rule A)",
            "annotated",
            "'2024-03-15'.toDate()",
            "struct<value:string>",
            "[2024-03-15]"),
        arguments(
            "§4.1 converted from the partial element: only __start is kept",
            "partial",
            "effective.ofType(dateTime) | effective.ofType(dateTime)",
            "array<struct<value:string,__start:timestamp>>",
            "ArraySeq([2024-03-15T10:30+10:00,2024-03-15 00:30:00.0])"),
        // §4.2 Scalars.
        arguments(
            "§4.2 System.Decimal: the number only",
            "annotated",
            "valueQuantity.value + 0",
            "decimal(38,6)",
            "72.500000"),
        // §4.3 System.Quantity.
        arguments(
            "§4.3 the literal 72.50 'kg': the canonical form as constants",
            "unannotated",
            "72.50 'kg'",
            "struct<value:decimal(38,6),unit:string,code:string,system:string,"
                + "__canonical_code:string,"
                + "__canonical_value_exact:struct<value:decimal(38,0),scale:int>>",
            "[72.500000,kg,kg," + UCUM + ",g,[72500,0]]"),
        arguments(
            "§4.3 the literal 4 days: no canonical form",
            "unannotated",
            "4 days",
            "struct<value:decimal(38,6),unit:string,code:string,system:string>",
            "[4.000000,days,day,https://hl7.org/fhirpath/N1/calendar-duration]"),
        arguments(
            "§4.3 the literal 5 seconds: canonicalised to UCUM s",
            "unannotated",
            "5 seconds",
            "struct<value:decimal(38,6),unit:string,code:string,system:string,"
                + "__canonical_code:string,"
                + "__canonical_value_exact:struct<value:decimal(38,0),scale:int>>",
            "[5.000000,seconds,second,https://hl7.org/fhirpath/N1/calendar-duration,s,[5,0]]"),
        // §5.1 Merges.
        arguments(
            "§5.1 birthDate | @2000, annotated data: both sides keep their bounds",
            "foreign",
            "birthDate | @2000",
            "array<struct<value:string,__start:timestamp,__end:timestamp>>",
            "ArraySeq([1970-03,1970-03-01 00:00:00.0,1970-03-31 23:59:59.999999],"
                + " [2000,2000-01-01 00:00:00.0,2000-12-31 23:59:59.999999])"),
        arguments(
            "§4.1 and §5.1 a stored dateTime merged with a literal, unannotated data",
            "unannotated",
            "effective.ofType(dateTime) | @2024-03",
            "array<struct<value:string>>",
            "ArraySeq([2024-03-15T10:30+10:00], [2024-03])"),
        arguments(
            "§4.3 and §5.1 valueQuantity | 5 'kg', annotated data: the stored canonical fields",
            "annotated",
            "valueQuantity | 5 'kg'",
            "array<struct<value:decimal(38,6),unit:string,code:string,system:string,"
                + "__canonical_code:string,"
                + "__canonical_value_exact:struct<value:decimal(38,0),scale:int>>>",
            "ArraySeq([72.500000,kg,kg,"
                + UCUM
                + ",g,[72500,0]], [5.000000,kg,kg,"
                + UCUM
                + ",g,[5000,0]])"),
        arguments(
            "§4.3 and §5.1 valueQuantity | 5 'kg', unannotated data: no canonical fields",
            "unannotated",
            "valueQuantity | 5 'kg'",
            "array<struct<value:decimal(38,6),unit:string,code:string,system:string>>",
            "ArraySeq([72.500000,kg,kg," + UCUM + "], [5.000000,kg,kg," + UCUM + "])"));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("examples")
  void theEngineFormIsAsTheExampleShows(
      @Nonnull final String example,
      @Nonnull final String dataset,
      @Nonnull final String expression,
      @Nonnull final String type,
      @Nonnull final String value) {
    final boolean patients = "foreign".equals(dataset);
    final Dataset<Row> result =
        evaluate(dataset, patients ? ResourceType.PATIENT : ResourceType.OBSERVATION, expression);
    assertThat(result.schema().apply("value").dataType().simpleString()).isEqualTo(type);
    assertThat(valueOf(result, patients ? "p1" : "obs1")).isEqualTo(value);
  }

  /**
   * §4.3: a stored 1 ns over annotated data carries the canonical form ingest stored, which loses
   * nothing, 0.000000001 s; a quantity with a comparator has no System form, so converting it gives
   * empty.
   */
  @Test
  void aSmallUnitKeepsItsExactCanonicalValueAndAComparatorHasNoSystemForm() {
    final Dataset<Row> merged =
        evaluate("annotated", ResourceType.OBSERVATION, "valueQuantity | 1 'g'");
    assertThat(valueOf(merged, "obs2"))
        .isEqualTo(
            "ArraySeq([1.000000,ns,ns," + UCUM + ",s,[1,9]], [1.000000,g,g," + UCUM + ",g,[1,0]])");
    assertThat(valueOf(merged, "obs3")).isEqualTo("ArraySeq([1.000000,g,g," + UCUM + ",g,[1,0]])");
  }

  /**
   * §5: a primitive leaves the engine as its value only; its annotation fields are never output. A
   * complex FHIR value is output in its fitted shape, its extensions included.
   */
  @Test
  void aPrimitiveIsOutputAsItsValueAndAComplexValueAsStored() {
    final SearchColumnBuilder builder =
        SearchColumnBuilder.withDefaultRegistry(fhirEncoders.getContext());
    final Dataset<Row> output =
        data.get("annotated")
            .filter(col("id").equalTo("obs1"))
            .select(
                builder
                    .fromExpression(ResourceType.OBSERVATION, "effective.ofType(dateTime)")
                    .alias("date"),
                builder
                    .fromExpression(ResourceType.OBSERVATION, "code.coding.first()")
                    .alias("coding"),
                builder
                    .fromExpression(ResourceType.OBSERVATION, "valueQuantity")
                    .alias("quantity"));
    assertThat(output.schema().apply("date").dataType().simpleString()).isEqualTo("string");
    assertThat(output.collectAsList().get(0).getString(0)).isEqualTo("2024-03-15T10:30+10:00");
    assertThat(output.schema().apply("coding").dataType().simpleString())
        .isEqualTo(
            "struct<id:string,extension:array<struct<url:string,valueString:string>>,"
                + "system:string,code:string,display:string>");
    assertThat(output.schema().apply("quantity").dataType().simpleString())
        .isEqualTo("struct<value:decimal(38,6),unit:string,code:string,system:string>");
  }

  @Nonnull
  private Dataset<Row> evaluate(
      @Nonnull final String dataset,
      @Nonnull final ResourceType resourceType,
      @Nonnull final String expression) {
    final DatasetEvaluator evaluator =
        DatasetEvaluatorBuilder.create(resourceType, fhirEncoders.getContext())
            .withDataset(data.get(dataset))
            .build();
    return evaluator.evaluate(PARSER.parse(expression)).toIdValueDataset();
  }

  @Nonnull
  private static String valueOf(@Nonnull final Dataset<Row> result, @Nonnull final String id) {
    return String.valueOf(result.filter(col("id").equalTo(id)).collectAsList().get(0).get(1));
  }

  @Nonnull
  private Dataset<Row> store(@Nonnull final String name, @Nonnull final Dataset<Row> dataset) {
    return PrunedSchemaReader.write(dataset, tempDir.resolve(name).toString()).read();
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
