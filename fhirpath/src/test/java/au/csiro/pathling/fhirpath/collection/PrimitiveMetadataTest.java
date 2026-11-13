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
import static org.apache.spark.sql.functions.col;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import au.csiro.pathling.config.FhirpathConfiguration;
import au.csiro.pathling.config.QueryConfiguration;
import au.csiro.pathling.definition.fhir.FhirDefinitionContext;
import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhirpath.evaluation.CollectionDataset;
import au.csiro.pathling.fhirpath.evaluation.DatasetEvaluatorBuilder;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.io.Annotator;
import au.csiro.pathling.io.FhirReader;
import au.csiro.pathling.io.source.DataSource;
import au.csiro.pathling.schema.SchemaConfiguration;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.datasource.PrunedSchemaReader;
import au.csiro.pathling.views.Column;
import au.csiro.pathling.views.FhirView;
import au.csiro.pathling.views.FhirViewExecutor;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.ArraysZip;
import org.apache.spark.sql.catalyst.expressions.Coalesce;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.catalyst.expressions.GetArrayStructFields;
import org.apache.spark.sql.catalyst.plans.logical.LogicalPlan;
import org.apache.spark.sql.execution.FileSourceScanExec;
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
 * A primitive's id and extensions in the engine (ticket 52; decisions 141, 142 and 152): read with
 * the engine's switch on from data stored with them, and empty with it off. Every expression is
 * evaluated over data stored with them both annotated and unannotated, which give the same results.
 */
@SpringBootUnitTest
@TestInstance(Lifecycle.PER_CLASS)
class PrimitiveMetadataTest {

  private static final Parser PARSER = new Parser();

  private static final String NICKNAME = "http://example.org/nickname";

  private static final String SOURCE = "http://example.org/source";

  /**
   * p1 carries an id and extensions on a singular code, a date and a boolean, and on items of a
   * repeating string under a repeating parent; p2 a metadata group without its value, singular and
   * repeating, beside an item with no metadata; p3 none at all; p4 two equal values with different
   * ids.
   */
  private static final List<String> PATIENTS =
      List.of(
          "{\"resourceType\":\"Patient\",\"id\":\"p1\","
              + "\"gender\":\"female\",\"_gender\":{\"id\":\"g1\",\"extension\":"
              + "[{\"url\":\""
              + SOURCE
              + "\",\"valueString\":\"device\"}]},"
              + "\"birthDate\":\"1970-03\",\"_birthDate\":{\"extension\":"
              + "[{\"url\":\"http://example.org/precision\",\"valueCode\":\"month\"}]},"
              + "\"active\":true,\"_active\":{\"id\":\"a1\"},\"multipleBirthInteger\":1,"
              + "\"name\":[{\"family\":\"Smith\",\"given\":[\"Anne\",\"Mary\",\"Jo\"],"
              + "\"_given\":[null,{\"id\":\"n2\",\"extension\":[{\"url\":\""
              + NICKNAME
              + "\",\"valueBoolean\":true}]},{\"id\":\"n3\"}]},{\"given\":[\"Sue\"]}]}",
          "{\"resourceType\":\"Patient\",\"id\":\"p2\",\"_gender\":{\"id\":\"orphan\"},"
              + "\"name\":[{\"given\":[null,\"B\"],\"_given\":[{\"id\":\"orphan\",\"extension\":"
              + "[{\"url\":\""
              + NICKNAME
              + "\",\"valueBoolean\":false}]},null]}]}",
          "{\"resourceType\":\"Patient\",\"id\":\"p3\",\"gender\":\"male\","
              + "\"birthDate\":\"1980\",\"name\":[{\"given\":[\"C\"]}]}",
          "{\"resourceType\":\"Patient\",\"id\":\"p4\",\"name\":[{\"given\":[\"Ann\",\"Ann\"],"
              + "\"_given\":[{\"id\":\"first\"},{\"id\":\"second\"}]}]}");

  /**
   * o1 is the example resource of {@code annotations-by-example.md} §1 in part: a dateTime and a
   * decimal that carry an extension, and a code that carries none. o2 gives the metadata groups an
   * id, so that the schema has one, and o3 a repeating dateTime with extensions on some items.
   */
  private static final List<String> OBSERVATIONS =
      List.of(
          "{\"resourceType\":\"Observation\",\"id\":\"o1\",\"status\":\"final\","
              + "\"code\":{\"text\":\"x\"},"
              + "\"effectiveDateTime\":\"2024-03-15T10:30+10:00\","
              + "\"_effectiveDateTime\":{\"extension\":[{\"url\":\""
              + SOURCE
              + "\",\"valueString\":\"device\"}]},"
              + "\"valueQuantity\":{\"value\":72.50,\"_value\":"
              + "{\"id\":\"v1\",\"extension\":[{\"url\":\"http://example.org/method\","
              + "\"valueString\":\"scale\"}]},\"unit\":\"kg\"}}",
          "{\"resourceType\":\"Observation\",\"id\":\"o2\",\"status\":\"final\","
              + "\"_status\":{\"id\":\"s2\"},\"code\":{\"text\":\"x\"},"
              + "\"effectiveDateTime\":\"2020\",\"_effectiveDateTime\":{\"id\":\"e2\"},"
              + "\"valueQuantity\":{\"value\":3,\"unit\":\"kg\"}}",
          "{\"resourceType\":\"Observation\",\"id\":\"o3\",\"status\":\"final\","
              + "\"code\":{\"text\":\"x\"},\"effectiveTiming\":{\"event\":"
              + "[\"2020-01-01\",\"2020-02\",\"2019\"],\"_event\":[{\"id\":\"t1\",\"extension\":"
              + "[{\"url\":\""
              + SOURCE
              + "\",\"valueString\":\"device\"}]},null,{\"id\":\"t3\"}]}}",
          "{\"resourceType\":\"Observation\",\"id\":\"o4\",\"status\":\"final\","
              + "\"code\":{\"text\":\"x\"},\"valueQuantity\":{\"value\":5,\"comparator\":\"<\","
              + "\"unit\":\"mg\"}}");

  /**
   * A repeating date and time under a repeating parent, and a repeating decimal under a repeating
   * parent, each with two positions that carry an id but no value.
   */
  private static final String MEDICATION_REQUEST =
      "{\"resourceType\":\"MedicationRequest\",\"id\":\"m1\",\"status\":\"active\","
          + "\"intent\":\"order\",\"subject\":{\"reference\":\"Patient/p1\"},"
          + "\"dosageInstruction\":[{\"timing\":{\"event\":[null,null,\"2020-01-01\"],"
          + "\"_event\":[{\"id\":\"a\"},{\"id\":\"b\"},null],\"repeat\":{\"timeOfDay\":"
          + "[null,null,\"08:00:00\"],\"_timeOfDay\":[{\"id\":\"c\"},{\"id\":\"d\"},null]}}}]}";

  private static final String MOLECULAR_SEQUENCE =
      "{\"resourceType\":\"MolecularSequence\",\"id\":\"s1\",\"coordinateSystem\":0,"
          + "\"quality\":[{\"type\":\"snp\",\"roc\":{\"precision\":[null,null,0.5],"
          + "\"_precision\":[{\"id\":\"a\"},{\"id\":\"b\"},null]}}]}";

  private static final FhirpathConfiguration ON =
      FhirpathConfiguration.builder().readPrimitiveMetadata(true).build();

  private static final FhirpathConfiguration OFF =
      FhirpathConfiguration.builder().readPrimitiveMetadata(false).build();

  @Autowired SparkSession spark;

  @Autowired FhirEncoders fhirEncoders;

  @TempDir static Path tempDir;

  /** The data stored with primitive metadata, unannotated and annotated, by resource type. */
  private Map<String, Dataset<Row>> unannotated;

  private Map<String, Dataset<Row>> annotated;

  /** The patients stored without primitive metadata. */
  private Dataset<Row> patientsWithout;

  @BeforeAll
  void setUp() {
    final FhirDefinitionContext definitions = FhirDefinitionContext.of(fhirEncoders.getContext());
    final FhirReader reader =
        FhirReader.of(
            spark, definitions, SchemaConfiguration.builder().storePrimitiveMetadata(true).build());
    final Annotator annotator = Annotator.of(definitions, SchemaConfiguration.builder().build());
    final Dataset<Row> patients = read(reader, "Patient", PATIENTS);
    final Dataset<Row> observations = read(reader, "Observation", OBSERVATIONS);
    final Dataset<Row> requests = read(reader, "MedicationRequest", List.of(MEDICATION_REQUEST));
    final Dataset<Row> sequences = read(reader, "MolecularSequence", List.of(MOLECULAR_SEQUENCE));
    unannotated =
        Map.of(
            "Patient", store("p", patients),
            "Observation", store("o", observations),
            "MedicationRequest", store("m", requests),
            "MolecularSequence", store("s", sequences));
    annotated =
        Map.of(
            "Patient", store("pa", annotator.annotate("Patient", patients)),
            "Observation", store("oa", annotator.annotate("Observation", observations)),
            "MedicationRequest", store("ma", annotator.annotate("MedicationRequest", requests)),
            "MolecularSequence", store("sa", annotator.annotate("MolecularSequence", sequences)));
    patientsWithout =
        store(
            "pw",
            read(
                FhirReader.of(spark, definitions, SchemaConfiguration.builder().build()),
                "Patient",
                PATIENTS));
  }

  // Navigation.

  @Nonnull
  Stream<Arguments> navigation() {
    return Stream.of(
        arguments("gender.id", "p1=g1 p2=orphan p3=null p4=null"),
        arguments(
            "gender.extension('" + SOURCE + "').value.ofType(string)",
            "p1=ArraySeq(device) p2=null p3=null p4=null"),
        arguments(
            "birthDate.extension.url",
            "p1=ArraySeq(http://example.org/precision) p2=null p3=null p4=null"),
        arguments("birthDate.id", "p1=null p2=null p3=null p4=null"),
        arguments("active.id", "p1=a1 p2=null p3=null p4=null"),
        arguments(
            "name.given.id",
            "p1=ArraySeq(n2, n3) p2=ArraySeq(orphan) p3=ArraySeq() "
                + "p4=ArraySeq(first, second)"),
        arguments(
            "name.given.where(extension('" + NICKNAME + "').exists())",
            "p1=ArraySeq(Mary) p2=ArraySeq() p3=ArraySeq() p4=ArraySeq()"),
        arguments(
            "name.given.extension('" + NICKNAME + "').value.ofType(boolean)",
            "p1=ArraySeq(true) p2=ArraySeq(false) p3=ArraySeq() p4=ArraySeq()"),
        arguments("name.given.extension.exists()", "p1=true p2=true p3=false p4=false"));
  }

  /**
   * With the switch on, a primitive's id and extensions are as stored, singular and repeating, and
   * empty where the source carried neither. A metadata group without its value is an element
   * without a value.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("navigation")
  void withTheSwitchOnAPrimitiveHasItsIdAndExtensions(
      @Nonnull final String expression, @Nonnull final String expected) {
    assertThat(rows(unannotated, ResourceType.PATIENT, expression, ON)).isEqualTo(expected);
    assertThat(rows(annotated, ResourceType.PATIENT, expression, ON)).isEqualTo(expected);
  }

  /** With the switch off, the same navigations are empty, with no error. */
  @ParameterizedTest(name = "{0}")
  @MethodSource("navigation")
  void withTheSwitchOffAPrimitiveHasNoIdOrExtensions(
      @Nonnull final String expression, @Nonnull final String ignoredOn) {
    final String result = rows(unannotated, ResourceType.PATIENT, expression, OFF);
    assertThat(result.split(" "))
        .allSatisfy(row -> assertThat(row).matches("p\\d=(null|false|ArraySeq\\(\\))"));
  }

  /** Data stored without the metadata groups gives empty with the switch on, silently. */
  @Test
  void dataStoredWithoutTheGroupsHasNoIdOrExtensions() {
    final Map<String, Dataset<Row>> without = Map.of("Patient", patientsWithout);
    assertThat(rows(without, ResourceType.PATIENT, "gender.id", ON))
        .isEqualTo("p1=null p2=null p3=null p4=null");
    assertThat(rows(without, ResourceType.PATIENT, "name.given.extension.exists()", ON))
        .isEqualTo("p1=false p2=false p3=false p4=false");
    assertThat(rows(without, ResourceType.PATIENT, "name.given.count()", ON))
        .isEqualTo("p1=4 p2=1 p3=1 p4=2");
  }

  @Nonnull
  Stream<Arguments> values() {
    return Stream.of(
        arguments("Patient", "gender", "p1=female p2=null p3=male p4=null"),
        arguments("Patient", "name.given.join(',')", "p1=Anne,Mary,Jo,Sue p2=B p3=C p4=Ann,Ann"),
        arguments("Patient", "birthDate < @1975", "p1=true p2=null p3=false p4=null"),
        arguments("Patient", "active and true", "p1=true p2=null p3=null p4=null"),
        arguments(
            "Patient",
            "name.given.where($this = 'Sue').exists()",
            "p1=true p2=false p3=false p4=false"),
        arguments("Observation", "valueQuantity.value > 10", "o1=true o2=false o3=null o4=false"),
        arguments("Observation", "valueQuantity.value.id", "o1=v1 o2=null o3=null o4=null"),
        arguments(
            "Observation",
            "valueQuantity.value.extension('http://example.org/method').value.ofType(string)",
            "o1=ArraySeq(scale) o2=null o3=null o4=null"),
        arguments(
            "Observation",
            "where(value.ofType(Quantity)).exists()",
            "o1=true o2=true o3=false o4=true"),
        arguments("Patient", "gender.convertsToInteger()", "p1=false p2=null p3=false p4=null"),
        arguments(
            "Patient",
            "name.given[multipleBirth.ofType(integer)]",
            "p1=Mary p2=null p3=null p4=null"),
        arguments("Observation", "status.id", "o1=null o2=s2 o3=null o4=null"),
        arguments(
            "Observation",
            "effective.ofType(Timing).event.id",
            "o1=null o2=null o3=ArraySeq(t1, t3) o4=null"),
        arguments(
            "Observation",
            "effective.ofType(Timing).event.where(extension.exists()) > @2019-06",
            "o1=null o2=null o3=true o4=null"));
  }

  /**
   * With the switch on, a primitive with an id or extensions but no value is an element without a
   * value, as FHIRPath takes it (owner, 2026-11-13): it is counted and indexed, its id is read, and
   * its value is empty wherever it is used as a System value. Off, it is not read as one.
   */
  @Test
  void aPrimitiveWithMetadataButNoValueIsAnElement() {
    for (final Map<String, Dataset<Row>> data : List.of(unannotated, annotated)) {
      assertThat(rows(data, ResourceType.PATIENT, "name.given.count()", ON))
          .isEqualTo("p1=4 p2=2 p3=1 p4=2");
      // The elements without a value are kept once by a deduplication.
      assertThat(rows(data, ResourceType.PATIENT, "name.given.distinct().count()", ON))
          .isEqualTo("p1=4 p2=2 p3=1 p4=1");
      assertThat(rows(data, ResourceType.PATIENT, "gender.exists()", ON))
          .isEqualTo("p1=true p2=true p3=true p4=false");
      assertThat(rows(data, ResourceType.PATIENT, "name.given[0].all(id = 'orphan')", ON))
          .isEqualTo("p1=false p2=true p3=false p4=false");
      assertThat(rows(data, ResourceType.PATIENT, "name.given[0].startsWith('B')", ON))
          .isEqualTo("p1=false p2=null p3=false p4=false");
      assertThat(rows(data, ResourceType.PATIENT, "name.given[0].toString()", ON))
          .isEqualTo("p1=Anne p2=null p3=C p4=Ann");
      assertThat(rows(data, ResourceType.PATIENT, "gender = 'female'", ON))
          .isEqualTo("p1=true p2=null p3=false p4=null");
      assertThat(rows(data, ResourceType.PATIENT, "name.given", ON))
          .isEqualTo(
              "p1=ArraySeq(Anne, Mary, Jo, Sue) p2=ArraySeq(B) p3=ArraySeq(C) "
                  + "p4=ArraySeq(Ann, Ann)");
    }
    assertThat(rows(unannotated, ResourceType.PATIENT, "gender.exists()", OFF))
        .isEqualTo("p1=true p2=false p3=true p4=false");
  }

  /**
   * With the switch on, a repeating date, time and decimal without a value are elements too: they
   * are counted, a deduplication keeps one of them, and they are output as nothing. With it off,
   * under a repeating parent each is kept as a null item, as {@code main} keeps it.
   */
  @Test
  void aRepeatingDateTimeOrDecimalWithoutAValue() {
    for (final Map<String, Dataset<Row>> data : List.of(unannotated, annotated)) {
      final ResourceType request = ResourceType.MEDICATIONREQUEST;
      final ResourceType sequence = ResourceType.MOLECULARSEQUENCE;
      assertThat(rows(data, request, "dosageInstruction.timing.event", ON))
          .isEqualTo("m1=ArraySeq(2020-01-01)");
      assertThat(rows(data, request, "dosageInstruction.timing.event.count()", ON))
          .isEqualTo("m1=3");
      assertThat(rows(data, request, "dosageInstruction.timing.event.distinct().count()", ON))
          .isEqualTo("m1=2");
      assertThat(rows(data, request, "dosageInstruction.timing.event.id", ON))
          .isEqualTo("m1=ArraySeq(a, b)");
      assertThat(
              rows(
                  data,
                  request,
                  "dosageInstruction.timing.repeat.timeOfDay.distinct().count()",
                  ON))
          .isEqualTo("m1=2");
      assertThat(rows(data, sequence, "quality.roc.precision", ON)).isEqualTo("s1=ArraySeq(0.5)");
      assertThat(rows(data, sequence, "quality.roc.precision.count()", ON)).isEqualTo("s1=3");
      assertThat(rows(data, sequence, "quality.roc.precision.distinct().count()", ON))
          .isEqualTo("s1=2");
      assertThat(rows(data, sequence, "(quality.roc.precision | 1.0).count()", ON))
          .isEqualTo("s1=2");
      assertThat(rows(data, request, "dosageInstruction.timing.event.count()", OFF))
          .isEqualTo("m1=3");
      assertThat(rows(data, sequence, "quality.roc.precision.count()", OFF)).isEqualTo("s1=3");
    }
  }

  /**
   * A System type that a choice holds as several FHIR types is taken as the System values of all of
   * them, with the switch on as off.
   */
  @Test
  void aChoiceOfSeveralFhirTypesIsTakenAsSystemValues() {
    for (final FhirpathConfiguration configuration : List.of(ON, OFF)) {
      assertThat(
              rows(
                  unannotated,
                  ResourceType.PATIENT,
                  "birthDate.extension.value.ofType(System.String)",
                  configuration))
          .isEqualTo(
              configuration == ON
                  ? "p1=ArraySeq(month) p2=null p3=null p4=null"
                  : "p1=null p2=null p3=null p4=null");
      assertThat(
              rows(
                  unannotated,
                  ResourceType.PATIENT,
                  "gender.extension.value.ofType(System.String) = 'device'",
                  configuration))
          .isEqualTo(
              configuration == ON
                  ? "p1=true p2=null p3=null p4=null"
                  : "p1=null p2=null p3=null p4=null");
    }
  }

  /**
   * A primitive's value is the same with the switch on as off, and with it on, over annotated and
   * unannotated data alike.
   */
  @ParameterizedTest(name = "{0}: {1}")
  @MethodSource("values")
  void theSwitchLeavesTheValuesAsTheyAre(
      @Nonnull final String resourceType,
      @Nonnull final String expression,
      @Nonnull final String expected) {
    final ResourceType type = ResourceType.fromCode(resourceType);
    assertThat(rows(unannotated, type, expression, ON)).isEqualTo(expected);
    assertThat(rows(annotated, type, expression, ON)).isEqualTo(expected);
    if (!expression.contains(".id") && !expression.contains("extension")) {
      assertThat(rows(unannotated, type, expression, OFF)).isEqualTo(expected);
    }
  }

  // The engine form: design annotations-by-example.md §3.1, with the switch on.

  @Nonnull
  Stream<Arguments> engineForms() {
    final String extension = "array<struct<url:string,valueString:string>>";
    return Stream.of(
        arguments(
            "§3.1 a dateTime, annotated: id and extension join as content fields",
            annotated,
            "effective.ofType(dateTime)",
            "struct<value:string,id:string,extension:"
                + extension
                + ",__start:timestamp,__end:timestamp>",
            "[2024-03-15T10:30+10:00,null,ArraySeq(["
                + SOURCE
                + ",device]),"
                + "2024-03-15 00:30:00.0,2024-03-15 00:30:59.999999]"),
        arguments(
            "§3.1 the same, unannotated",
            unannotated,
            "effective.ofType(dateTime)",
            "struct<value:string,id:string,extension:" + extension + ">",
            "[2024-03-15T10:30+10:00,null,ArraySeq([" + SOURCE + ",device])]"),
        arguments(
            "§3.1 a decimal: its text, id, extension and number",
            annotated,
            "valueQuantity.value",
            "struct<value:string,id:string,extension:" + extension + ",__numeric:decimal(38,6)>",
            "[72.5,v1,ArraySeq([http://example.org/method,scale]),72.500000]"),
        arguments(
            "§3.1 a code: the same shape as every type without an annotation kind",
            annotated,
            "status",
            "struct<value:string,id:string>",
            "[final,null]"));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("engineForms")
  void theEngineFormIsAsTheExampleShows(
      @Nonnull final String example,
      @Nonnull final Map<String, Dataset<Row>> data,
      @Nonnull final String expression,
      @Nonnull final String type,
      @Nonnull final String value) {
    final Dataset<Row> result =
        evaluate(data, ResourceType.OBSERVATION, expression, ON).toIdValueDataset();
    assertThat(result.schema().apply("value").dataType().simpleString()).isEqualTo(type);
    assertThat(valueOf(result, "o1")).isEqualTo(value);
  }

  /** §3.1 a repeating string: each value with its id and extensions, paired by position. */
  @Test
  void aRepeatingStringIsPairedWithItsIdAndExtensions() {
    final Dataset<Row> result =
        evaluate(unannotated, ResourceType.PATIENT, "name.given", ON).toIdValueDataset();
    assertThat(result.schema().apply("value").dataType().simpleString())
        .isEqualTo(
            "array<struct<value:string,id:string,"
                + "extension:array<struct<url:string,valueBoolean:boolean>>>>");
    assertThat(valueOf(result, "p1"))
        .isEqualTo(
            "ArraySeq([Anne,null,null], [Mary,n2,ArraySeq(["
                + NICKNAME
                + ",true])],"
                + " [Jo,n3,null], [Sue,null,null])");
    assertThat(valueOf(result, "p2"))
        .isEqualTo("ArraySeq([null,orphan,ArraySeq([" + NICKNAME + ",false])], [B,null,null])");
  }

  // Conversion to System (D8) and merges (D9).

  @Nonnull
  Stream<Arguments> conversions() {
    return Stream.of(
        arguments(
            "a dateTime converted by a function keeps its annotations alone",
            "effective.ofType(dateTime).toDateTime()",
            "struct<value:string,__start:timestamp,__end:timestamp>",
            "[2024-03-15T10:30+10:00,2024-03-15 00:30:00.0,2024-03-15 00:30:59.999999]"),
        arguments(
            "a decimal converted is a bare System.Decimal",
            "valueQuantity.value.toDecimal()",
            "decimal(38,6)",
            "72.500000"),
        arguments(
            "a decimal computed with is a bare System.Decimal",
            "valueQuantity.value + 0",
            "decimal(38,6)",
            "72.500000"),
        arguments("a code converted is its value", "status.toString()", "string", "final"),
        arguments(
            "a merge with a System value drops id and extension",
            "effective.ofType(dateTime) | @2024-03",
            "array<struct<value:string,__start:timestamp,__end:timestamp>>",
            "ArraySeq([2024-03-15T10:30+10:00,2024-03-15 00:30:00.0,2024-03-15 00:30:59.999999],"
                + " [2024-03,2024-03-01 00:00:00.0,2024-03-31 23:59:59.999999])"),
        arguments(
            "a merge of FHIR values unions id and extension",
            "effective.ofType(dateTime) | effective.ofType(dateTime)",
            "array<struct<value:string,id:string,extension:"
                + "array<struct<url:string,valueString:string>>,"
                + "__start:timestamp,__end:timestamp>>",
            "ArraySeq([2024-03-15T10:30+10:00,null,ArraySeq(["
                + SOURCE
                + ",device]),"
                + "2024-03-15 00:30:00.0,2024-03-15 00:30:59.999999])"),
        arguments(
            "a merge of a FHIR code with a System string drops id and extension",
            "status | 'x'",
            "array<string>",
            "ArraySeq(final, x)"));
  }

  /**
   * With the switch on, a primitive that carries an extension converts to a System value without
   * its id and extension, and keeps its stored annotations; a merge whose result is System drops
   * them; a merge of FHIR values unions them.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("conversions")
  void aConversionOrAMergeDropsOrKeepsTheIdAndExtension(
      @Nonnull final String example,
      @Nonnull final String expression,
      @Nonnull final String type,
      @Nonnull final String value) {
    final Dataset<Row> result =
        evaluate(annotated, ResourceType.OBSERVATION, expression, ON).toIdValueDataset();
    assertThat(result.schema().apply("value").dataType().simpleString()).isEqualTo(type);
    assertThat(valueOf(result, "o1")).isEqualTo(value);
  }

  /**
   * A merge of FHIR values of different paths unions their ids and extensions by name, and the
   * deduplication of a union compares values alone, keeping the first with its own id.
   */
  @Test
  void aMergeOfFhirValuesUnionsTheirIdsAndExtensions() {
    final Dataset<Row> merged =
        evaluate(unannotated, ResourceType.PATIENT, "gender | name.given", ON).toIdValueDataset();
    assertThat(merged.schema().apply("value").dataType().simpleString())
        .isEqualTo(
            "array<struct<value:string,id:string,extension:"
                + "array<struct<url:string,valueBoolean:boolean,valueString:string>>>>");
    assertThat(valueOf(merged, "p1"))
        .isEqualTo(
            "ArraySeq([female,g1,ArraySeq(["
                + SOURCE
                + ",null,device])], [Anne,null,null],"
                + " [Mary,n2,ArraySeq(["
                + NICKNAME
                + ",true,null])], [Jo,n3,null],"
                + " [Sue,null,null])");
    // The elements without a value are kept once by a deduplication.
    assertThat(rows(unannotated, ResourceType.PATIENT, "(name.given | name.given).id", ON))
        .isEqualTo("p1=ArraySeq(n2, n3) p2=ArraySeq(orphan) p3=ArraySeq() p4=ArraySeq(first)");
    assertThat(rows(unannotated, ResourceType.PATIENT, "name.given.distinct().id", ON))
        .isEqualTo("p1=ArraySeq(n2, n3) p2=ArraySeq(orphan) p3=ArraySeq() p4=ArraySeq(first)");
  }

  // Output (D10).

  /** A view outputs a primitive as its value alone, with the switch on. */
  @Test
  void aViewOutputsAPrimitiveAsItsValue() {
    final Dataset<Row> result =
        view(
            FhirView.ofResource("Patient")
                .select(
                    columns(
                        Column.single("id", "id"),
                        Column.single("gender", "gender"),
                        Column.single("born", "birthDate"),
                        Column.collectionBuilder("given", "name.given").build()),
                    forEach("name", Column.single("first", "given.first()")))
                .build());
    assertThat(result.schema().simpleString())
        .isEqualTo("struct<id:string,gender:string,born:string,given:array<string>,first:string>");
    assertThat(
            result.filter(col("id").equalTo("p1")).collectAsList().stream()
                .map(Row::toString)
                .sorted()
                .toList())
        .containsExactly(
            "[p1,female,1970-03,ArraySeq(Anne, Mary, Jo, Sue),Anne]",
            "[p1,female,1970-03,ArraySeq(Anne, Mary, Jo, Sue),Sue]");
  }

  // Plans.

  /**
   * A singular primitive outside a lambda reads from its metadata group only the fields it needs:
   * its id and extensions, which decide whether it is an element where it has no value, and the
   * annotations only where it is compared, never the group itself.
   */
  @Test
  void aSingularPrimitiveReadsOnlyTheFieldsItNeeds() {
    final Dataset<Row> output =
        evaluate(annotated, ResourceType.PATIENT, "birthDate", ON).toIdExternalValueDataset();
    assertThat(readSchema(output))
        .contains("_birthDate:struct<extension:")
        .doesNotContain("__start")
        .doesNotContain("__end");
    final Dataset<Row> compared =
        evaluate(annotated, ResourceType.PATIENT, "birthDate < @1975", ON)
            .toIdExternalValueDataset();
    assertThat(readSchema(compared)).contains("__start:timestamp,__end:timestamp>");
    final Dataset<Row> asked =
        evaluate(annotated, ResourceType.PATIENT, "gender.id", ON).toIdExternalValueDataset();
    assertThat(readSchema(asked)).contains("_gender:struct<id:string,extension:");
  }

  /**
   * A repeating primitive is paired with its metadata by position, every field read from the groups
   * by name, never the groups themselves, and nothing tests whether the groups exist. On Spark
   * 4.0.2 nothing is pruned through the lambda that leaves out values without their value, so only
   * the shape is asserted.
   */
  @Test
  void aRepeatingPrimitiveIsPairedByNameAndPosition() {
    final Dataset<Row> result =
        evaluate(
                annotated,
                ResourceType.OBSERVATION,
                "effective.ofType(Timing).event.where($this > @2019-06).count()",
                ON)
            .toIdExternalValueDataset();
    assertThat(rows(result)).isEqualTo("o1=0 o2=0 o3=2 o4=0");
    final LogicalPlan plan = result.queryExecution().optimizedPlan();
    final List<ArraysZip> zips =
        all(plan).filter(ArraysZip.class::isInstance).map(ArraysZip.class::cast).toList();
    assertThat(zips)
        .isNotEmpty()
        .allSatisfy(
            zip -> {
              assertThat(CollectionConverters.asJava(zip.names()).stream().map(String::valueOf))
                  .containsExactly("value", "id", "extension", "__start", "__end");
              final List<Expression> inputs = CollectionConverters.asJava(zip.children());
              assertThat(inputs.subList(1, inputs.size()))
                  .allSatisfy(
                      input -> {
                        assertThat(input).isInstanceOf(Coalesce.class);
                        assertThat(input.children().head())
                            .isInstanceOf(GetArrayStructFields.class);
                      });
            });
    assertThat(all(plan).map(Expression::sql))
        .noneMatch(sql -> sql.matches("(?s).*isnotnull\\(_event\\).*"));
  }

  // Helpers.

  @Nonnull
  private static Dataset<Row> read(
      @Nonnull final FhirReader reader,
      @Nonnull final String resourceType,
      @Nonnull final List<String> resources) {
    return reader
        .json()
        .read(resourceType, SparkSession.active().createDataset(resources, Encoders.STRING()));
  }

  @Nonnull
  private static Dataset<Row> store(@Nonnull final String name, @Nonnull final Dataset<Row> data) {
    return PrunedSchemaReader.write(data, tempDir.resolve(name).toString()).read();
  }

  @Nonnull
  private CollectionDataset evaluate(
      @Nonnull final Map<String, Dataset<Row>> data,
      @Nonnull final ResourceType resourceType,
      @Nonnull final String expression,
      @Nonnull final FhirpathConfiguration configuration) {
    return DatasetEvaluatorBuilder.create(resourceType, fhirEncoders.getContext())
        .withDataset(data.get(resourceType.toCode()))
        .withConfiguration(configuration)
        .build()
        .evaluate(PARSER.parse(expression));
  }

  /** The rows of an expression's output, as {@code id=value}, sorted by id. */
  @Nonnull
  private String rows(
      @Nonnull final Map<String, Dataset<Row>> data,
      @Nonnull final ResourceType resourceType,
      @Nonnull final String expression,
      @Nonnull final FhirpathConfiguration configuration) {
    return rows(evaluate(data, resourceType, expression, configuration).toIdExternalValueDataset());
  }

  @Nonnull
  private static String rows(@Nonnull final Dataset<Row> result) {
    return String.join(
        " ",
        result.collectAsList().stream()
            .map(row -> row.getString(0) + "=" + row.get(1))
            .sorted()
            .toList());
  }

  @Nonnull
  private static String valueOf(@Nonnull final Dataset<Row> result, @Nonnull final String id) {
    return String.valueOf(result.filter(col("id").equalTo(id)).collectAsList().get(0).get(1));
  }

  @Nonnull
  private Dataset<Row> view(@Nonnull final FhirView view) {
    final DataSource source =
        new DataSource() {
          @Nonnull
          @Override
          public Dataset<Row> read(@Nullable final String resourceCode) {
            return unannotated.get(resourceCode);
          }

          @Nonnull
          @Override
          public Set<String> getResourceTypes() {
            return unannotated.keySet();
          }
        };
    return new FhirViewExecutor(
            fhirEncoders.getContext(), source, QueryConfiguration.builder().build(), true)
        .buildQuery(view);
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
  private static Stream<LogicalPlan> nodes(@Nonnull final LogicalPlan plan) {
    return Stream.concat(
        Stream.of(plan),
        CollectionConverters.asJava(plan.children()).stream()
            .flatMap(PrimitiveMetadataTest::nodes));
  }

  /** Every expression in a plan, at every depth. */
  @Nonnull
  private static Stream<Expression> all(@Nonnull final LogicalPlan plan) {
    return nodes(plan)
        .flatMap(node -> CollectionConverters.asJava(node.expressions()).stream())
        .flatMap(PrimitiveMetadataTest::descendants);
  }

  @Nonnull
  private static Stream<Expression> descendants(@Nonnull final Expression expression) {
    return Stream.concat(
        Stream.of(expression),
        CollectionConverters.asJava(expression.children()).stream()
            .flatMap(PrimitiveMetadataTest::descendants));
  }
}
