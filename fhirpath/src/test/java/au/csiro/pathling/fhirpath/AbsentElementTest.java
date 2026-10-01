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

package au.csiro.pathling.fhirpath;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhirpath.evaluation.DatasetEvaluator;
import au.csiro.pathling.fhirpath.evaluation.DatasetEvaluatorBuilder;
import au.csiro.pathling.fhirpath.parser.Parser;
import au.csiro.pathling.test.SpringBootUnitTest;
import au.csiro.pathling.test.datasource.PrunedSchemaReader;
import au.csiro.pathling.test.layout.LayoutDatasets;
import au.csiro.pathling.test.layout.TestLayout;
import ca.uhn.fhir.parser.IParser;
import jakarta.annotation.Nonnull;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.instance.model.api.IBaseResource;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.ContactPoint;
import org.hl7.fhir.r4.model.Enumerations.AdministrativeGender;
import org.hl7.fhir.r4.model.Enumerations.PublicationStatus;
import org.hl7.fhir.r4.model.Enumerations.ResourceType;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Observation.ObservationComponentComponent;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Quantity;
import org.hl7.fhir.r4.model.Questionnaire;
import org.hl7.fhir.r4.model.Questionnaire.QuestionnaireItemType;
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
 * Tests traversal to elements that the definitions describe but the input schema does not carry
 * (FR-024, FR-026 and FR-027), written ahead of the tolerant traversal that T110 emits.
 *
 * <p>The input is encoded, written to Parquet and read back twice: once with the schema it was
 * written with, and once with chosen elements removed from the schema, as they are from a pruned
 * table. The removed elements are never populated in the source, so every answer over the pruned
 * schema is the answer over the full schema. Each case is therefore run twice. The run over the
 * full schema is the control, and passed before T110; the run over the pruned schema is the test,
 * and passes since T110.
 *
 * <p>The cases cover a missing top-level column and a missing nested field, each at singular and
 * repeating cardinality, for primitive and complex elements.
 *
 * <p>The input is built in the active test layout. The new layout's schema is already pruned to
 * what the source populates, so there both runs read the elements as absent, and the control is a
 * control only on the previous layout. The parity tests for decision 72 compare the new layout's
 * shape with the released encoder explicitly, whatever the active layout.
 *
 * @author Piotr Szul
 */
@SpringBootUnitTest
@TestInstance(Lifecycle.PER_CLASS)
class AbsentElementTest {

  /**
   * The name of the known divergence from the released encoder that decision 72 records, so that a
   * parity test over both layouts can refer to it rather than discover it.
   */
  static final String KNOWN_DIVERGENCE_DECISION_72 =
      "Known divergence (decision 72): a structure whose only content is a repeating primitive's"
          + " metadata is kept by the layout and dropped by the released encoder";

  private static final Parser PARSER = new Parser();

  @Autowired SparkSession spark;

  @Autowired FhirEncoders fhirEncoders;

  @TempDir static Path tempDir;

  private PrunedSchemaReader patients;

  private PrunedSchemaReader observations;

  private PrunedSchemaReader questionnaires;

  private PrunedSchemaReader itemlessQuestionnaires;

  @BeforeAll
  void setUp() {
    patients =
        PrunedSchemaReader.write(
            encodeInLayout("Patient", patient1(), patient2()),
            tempDir.resolve("Patient").toString(),
            fhirEncoders.of("Patient").schema());
    observations =
        PrunedSchemaReader.write(
            encodeInLayout("Observation", observation1(), observation2()),
            tempDir.resolve("Observation").toString(),
            fhirEncoders.of("Observation").schema());
    questionnaires =
        PrunedSchemaReader.write(
            encodeInLayout("Questionnaire", questionnaire1(), questionnaire2()),
            tempDir.resolve("Questionnaire").toString(),
            fhirEncoders.of("Questionnaire").schema());
    itemlessQuestionnaires =
        PrunedSchemaReader.write(
            encodeInLayout("Questionnaire", questionnaire2()),
            tempDir.resolve("ItemlessQuestionnaire").toString(),
            fhirEncoders.of("Questionnaire").schema());
  }

  // T101: traversal to an element the definitions describe but the schema lacks yields empty.

  @Nonnull
  Stream<Arguments> absentPatientElements() {
    return Stream.of(
        // A missing top-level column: a singular primitive, a repeating complex element and a
        // singular complex element.
        arguments("active", "active"),
        arguments("telecom", "telecom"),
        arguments("telecom", "telecom.value"),
        arguments("managingOrganization", "managingOrganization"),
        arguments("managingOrganization", "managingOrganization.reference"),
        // A missing nested field under a repeating parent: a singular primitive, a repeating
        // primitive and a singular complex element.
        arguments("name.family", "name.family"),
        arguments("name.prefix", "name.prefix"),
        arguments("name.period", "name.period"),
        arguments("name.period", "name.period.start"),
        // A missing nested field under a singular parent.
        arguments("maritalStatus.text", "maritalStatus.text"),
        // A missing repeating complex field under a repeating parent.
        arguments("contact.name", "contact.name"),
        arguments("contact.name", "contact.name.given"));
  }

  @ParameterizedTest(name = "{1} over the full schema")
  @MethodSource("absentPatientElements")
  void traversalToUnpopulatedElementIsEmptyOverFullSchema(
      @Nonnull final String prunedPath, @Nonnull final String expression) {
    assertEmpty(patients.read(), expression);
  }

  @ParameterizedTest(name = "{1} with {0} absent from the schema")
  @MethodSource("absentPatientElements")
  void traversalToAbsentElementIsEmpty(
      @Nonnull final String prunedPath, @Nonnull final String expression) {
    assertEmpty(patients.readWithout(prunedPath), expression);
  }

  @ParameterizedTest(name = "{0} is absent from the pruned schema")
  @MethodSource("absentPatientElements")
  void prunedSchemaLacksTheElement(
      @Nonnull final String prunedPath, @Nonnull final String expression) {
    // A control on the setup: the element is one the definitions describe, and the pruned schema
    // does not carry it. The previous layout's full schema carries it, while the new layout prunes
    // it itself, because the source never populates it.
    assertThat(hasElement(fhirEncoders.of("Patient").schema(), prunedPath)).isTrue();
    assertThat(hasElement(patients.read().schema(), prunedPath))
        .isEqualTo(!TestLayout.active().isPof());
    assertThat(hasElement(patients.readWithout(prunedPath).schema(), prunedPath)).isFalse();
  }

  @Test
  void repeatingPrimitiveMetadataOnlyIsKnownDivergenceFromReleasedEncoder() {
    // The layout keeps a name holding only `_given`, storing `given` as a null array of the type
    // the definitions give it, so its fitted schema carries `given` and nothing else in `name`.
    final Dataset<Row> layout =
        layoutPatient(
            "{\"id\":\"p1\",\"name\":[{\"given\":null}]}",
            DataTypes.createArrayType(DataTypes.StringType),
            "given");
    // The released encoder reads through HAPI's parser, which discards an `_given` array with no
    // `given` array beside it, and so the name that held only it.
    final Dataset<Row> released =
        parseAndEncode(
            "{\"resourceType\":\"Patient\",\"id\":\"p1\","
                + "\"name\":[{\"_given\":[{\"id\":\"a\"},{\"id\":\"b\"}]}]}");

    assertThat(evaluate(layout, "Patient", "name.count()"))
        .as(KNOWN_DIVERGENCE_DECISION_72 + ": the layout answers 1")
        .containsExactly("p1=1");
    assertThat(evaluate(released, "Patient", "name.count()"))
        .as(KNOWN_DIVERGENCE_DECISION_72 + ": the released encoder answers 0")
        .containsExactly("p1=0");
    // Against the specification rather than the released encoder: fhirpath.js answers 2, counting
    // a value-less primitive per `_given` entry, while the layout stores `given` as a null array
    // and
    // answers 0 until M5. T061 decides the alignment that settles it.
    assertThat(evaluate(layout, "Patient", "name.given.count()"))
        .as(KNOWN_DIVERGENCE_DECISION_72 + ": the layout answers 0 for the value-less primitives")
        .containsExactly("p1=0");
  }

  @Test
  void singularPrimitiveMetadataOnlyAgreesWithReleasedEncoder() {
    // A name holding only `_family` is kept by both: the layout stores `family` as a typed null,
    // and HAPI's parser keeps a singular primitive that carries only its metadata.
    final Dataset<Row> layout =
        layoutPatient(
            "{\"id\":\"p1\",\"name\":[{\"family\":null}]}", DataTypes.StringType, "family");
    final Dataset<Row> released =
        parseAndEncode(
            "{\"resourceType\":\"Patient\",\"id\":\"p1\","
                + "\"name\":[{\"_family\":{\"id\":\"a\"}}]}");

    assertThat(evaluate(layout, "Patient", "name.count()")).containsExactly("p1=1");
    assertThat(evaluate(released, "Patient", "name.count()")).containsExactly("p1=1");
    assertThat(evaluate(layout, "Patient", "name.family.count()")).containsExactly("p1=0");
  }

  // T102: traversal to an element the definitions do not describe yields empty (FR-025, as
  // amended by decision 76). The engine resolves the name against the definitions before it emits
  // any column, so the answer does not depend on what the schema carries.

  @Nonnull
  Stream<Arguments> undescribedPatientElements() {
    return Stream.of(
        // At the root, under a repeating parent, under a singular parent, and past an undescribed
        // step.
        arguments("undescribed"),
        arguments("name.undescribed"),
        arguments("maritalStatus.undescribed"),
        arguments("undescribed.family"),
        // Beneath elements the pruned schema lacks, at the root and nested.
        arguments("telecom.undescribed"),
        arguments("name.period.undescribed"),
        // Beside a populated element, in a union.
        arguments("(name.given | name.undescribed).where($this = 'none')"));
  }

  @ParameterizedTest(name = "{0} over the full schema")
  @MethodSource("undescribedPatientElements")
  void traversalToUndescribedElementIsEmptyOverFullSchema(@Nonnull final String expression) {
    assertEmpty(patients.read(), expression);
  }

  @ParameterizedTest(name = "{0} over a pruned schema")
  @MethodSource("undescribedPatientElements")
  void traversalToUndescribedElementIsEmptyOverPrunedSchema(@Nonnull final String expression) {
    assertEmpty(patients.readWithout("active", "telecom", "name.period"), expression);
  }

  // T103: selecting a choice variant absent from the schema yields empty.

  @Nonnull
  Stream<Arguments> absentChoiceVariants() {
    return Stream.of(
        // A missing top-level variant, primitive and complex.
        arguments("valueString", "value.ofType(string)"),
        arguments("valueBoolean", "value.ofType(boolean)"),
        arguments("valueCodeableConcept", "value.ofType(CodeableConcept)"),
        arguments("valueCodeableConcept", "value.ofType(CodeableConcept).text"),
        // A missing variant nested under a repeating parent.
        arguments("component.valueString", "component.value.ofType(string)"),
        arguments("component.valueCodeableConcept", "component.value.ofType(CodeableConcept)"));
  }

  @ParameterizedTest(name = "{1} over the full schema")
  @MethodSource("absentChoiceVariants")
  void unpopulatedChoiceVariantIsEmptyOverFullSchema(
      @Nonnull final String prunedPath, @Nonnull final String expression) {
    assertEmpty(observations.read(), "Observation", expression);
  }

  @ParameterizedTest(name = "{1} with {0} absent from the schema")
  @MethodSource("absentChoiceVariants")
  void absentChoiceVariantIsEmpty(
      @Nonnull final String prunedPath, @Nonnull final String expression) {
    assertEmpty(observations.readWithout(prunedPath), "Observation", expression);
  }

  @Nonnull
  Stream<Arguments> populatedVariantsBesideAbsentOnes() {
    return Stream.of(
        arguments("value.ofType(Quantity).value", "o1=5.000000", "o2=null"),
        arguments("component.value.ofType(Quantity).value.first()", "o1=3.000000", "o2=null"));
  }

  @ParameterizedTest(name = "{0} over the full schema")
  @MethodSource("populatedVariantsBesideAbsentOnes")
  void populatedVariantOverFullSchema(
      @Nonnull final String expression,
      @Nonnull final String expected1,
      @Nonnull final String expected2) {
    assertThat(evaluate(observations.read(), "Observation", expression))
        .containsExactlyInAnyOrder(expected1, expected2);
  }

  // This passes before T110, because selecting one variant reads no other variant's column.
  @ParameterizedTest(name = "{0} with its sibling variants absent from the schema")
  @MethodSource("populatedVariantsBesideAbsentOnes")
  void populatedVariantBesideAbsentVariants(
      @Nonnull final String expression,
      @Nonnull final String expected1,
      @Nonnull final String expected2) {
    final Dataset<Row> pruned =
        observations.readWithout(
            "valueString",
            "valueBoolean",
            "valueCodeableConcept",
            "component.valueString",
            "component.valueCodeableConcept");
    assertThat(evaluate(pruned, "Observation", expression))
        .containsExactlyInAnyOrder(expected1, expected2);
  }

  // FR-026: the type of a choice element is read from its variants through the tolerant traversal,
  // so a variant absent from the schema is taken to be unpopulated rather than failing.

  @Nonnull
  Stream<Arguments> choiceTypesBesideAbsentVariants() {
    return Stream.of(
        arguments("value.type().name", "o1=Quantity", "o2=null"),
        arguments("component.value.type().name.first()", "o1=Quantity", "o2=null"),
        arguments("component.where(value.type().name = 'Quantity').count()", "o1=1", "o2=0"));
  }

  @ParameterizedTest(name = "{0} over the full schema")
  @MethodSource("choiceTypesBesideAbsentVariants")
  void choiceTypeOverFullSchema(
      @Nonnull final String expression,
      @Nonnull final String expected1,
      @Nonnull final String expected2) {
    assertThat(evaluate(observations.read(), "Observation", expression))
        .containsExactlyInAnyOrder(expected1, expected2);
  }

  @ParameterizedTest(name = "{0} with valueInteger absent from the schema")
  @MethodSource("choiceTypesBesideAbsentVariants")
  void choiceTypeBesideAbsentVariants(
      @Nonnull final String expression,
      @Nonnull final String expected1,
      @Nonnull final String expected2) {
    final Dataset<Row> pruned = observations.readWithout("valueInteger", "component.valueInteger");
    assertThat(evaluate(pruned, "Observation", expression))
        .containsExactlyInAnyOrder(expected1, expected2);
  }

  // T110a: recursive traversal stops where it reaches an element absent from the schema, and
  // yields what it collected up to there.

  @Nonnull
  Stream<Arguments> absentRecursionRoots() {
    return Stream.of(
        arguments("repeat(item)"), arguments("repeatAll(item)"), arguments("item.repeat(item)"));
  }

  @ParameterizedTest(name = "{0} over the full schema")
  @MethodSource("absentRecursionRoots")
  void recursionFromUnpopulatedRootOverFullSchema(@Nonnull final String expression) {
    assertEmpty(itemlessQuestionnaires.read(), "Questionnaire", expression);
  }

  @ParameterizedTest(name = "{0} with item absent from the schema")
  @MethodSource("absentRecursionRoots")
  void recursionFromAbsentRootIsEmpty(@Nonnull final String expression) {
    assertEmpty(itemlessQuestionnaires.readWithout("item"), "Questionnaire", expression);
  }

  @Nonnull
  Stream<Arguments> recursionsBesideAbsentLevels() {
    return Stream.of(
        arguments("repeat(item).linkId.count()", "q1=1", "q2=0"),
        arguments("repeatAll(item).linkId.count()", "q1=1", "q2=0"),
        arguments("repeat(item | item.item).linkId.count()", "q1=1", "q2=0"),
        arguments("repeat(item).enableWhen.count()", "q1=0", "q2=0"));
  }

  @ParameterizedTest(name = "{0} over the full schema")
  @MethodSource("recursionsBesideAbsentLevels")
  void recursionBesideUnpopulatedLevelOverFullSchema(
      @Nonnull final String expression,
      @Nonnull final String expected1,
      @Nonnull final String expected2) {
    assertThat(evaluate(questionnaires.read(), "Questionnaire", expression))
        .containsExactlyInAnyOrder(expected1, expected2);
  }

  @ParameterizedTest(name = "{0} with item.enableWhen absent from the schema")
  @MethodSource("recursionsBesideAbsentLevels")
  void recursionBesideAbsentLevelCollectsThePopulatedOnes(
      @Nonnull final String expression,
      @Nonnull final String expected1,
      @Nonnull final String expected2) {
    assertThat(evaluate(questionnaires.readWithout("item.enableWhen"), "Questionnaire", expression))
        .containsExactlyInAnyOrder(expected1, expected2);
  }

  // T104: combining an absent element with a populated one succeeds (FR-027).

  @Nonnull
  Stream<Arguments> combinationsWithAbsentElements() {
    return Stream.of(
        // Union and combination of an absent primitive with a populated one, in both orders.
        arguments("(name.family | name.given).count()", "p1=2", "p2=2"),
        arguments("(name.given | name.family).count()", "p1=2", "p2=2"),
        arguments("name.family.combine(name.given).count()", "p1=2", "p2=2"),
        arguments("name.given.combine(name.family).first()", "p1=Ann", "p2=Cal"),
        // Union and combination of an absent complex element with a populated one of the same type.
        arguments("(telecom | contact.telecom).count()", "p1=1", "p2=0"),
        arguments("telecom.combine(contact.telecom).first().value", "p1=123", "p2=null"),
        arguments("(contact.telecom | telecom).first().value", "p1=123", "p2=null"),
        // Membership of a populated element in an absent one, and of an absent one in a populated
        // one. Conditional selection cannot be expressed, because the engine does not implement
        // iif().
        arguments("name.given.first() in name.family", "p1=false", "p2=false"),
        arguments("name.given contains name.family.first()", "p1=null", "p2=null"),
        // Comparison of an absent element with a populated one.
        arguments("(name.family.first() = name.given.first()).empty()", "p1=true", "p2=true"),
        arguments("(name.given.first() > name.family.first()).empty()", "p1=true", "p2=true"),
        arguments("(active != (gender = 'female')).empty()", "p1=true", "p2=true"));
  }

  @ParameterizedTest(name = "{0} over the full schema")
  @MethodSource("combinationsWithAbsentElements")
  void combinationWithUnpopulatedElementOverFullSchema(
      @Nonnull final String expression,
      @Nonnull final String expected1,
      @Nonnull final String expected2) {
    assertThat(evaluate(patients.read(), "Patient", expression))
        .containsExactlyInAnyOrder(expected1, expected2);
  }

  @ParameterizedTest(name = "{0} with the absent elements absent from the schema")
  @MethodSource("combinationsWithAbsentElements")
  void combinationWithAbsentElementSucceeds(
      @Nonnull final String expression,
      @Nonnull final String expected1,
      @Nonnull final String expected2) {
    final Dataset<Row> pruned = patients.readWithout("active", "telecom", "name.family");
    assertThat(evaluate(pruned, "Patient", expression))
        .containsExactlyInAnyOrder(expected1, expected2);
  }

  // Helpers.

  private void assertEmpty(@Nonnull final Dataset<Row> dataset, @Nonnull final String expression) {
    assertEmpty(dataset, "Patient", expression);
  }

  private void assertEmpty(
      @Nonnull final Dataset<Row> dataset,
      @Nonnull final String resourceType,
      @Nonnull final String expression) {
    final List<String> ids = dataset.select("id").as(Encoders.STRING()).collectAsList();
    assertThat(evaluate(dataset, resourceType, expression + ".empty()"))
        .containsExactlyInAnyOrderElementsOf(ids.stream().map(id -> id + "=true").toList());
    assertThat(evaluate(dataset, resourceType, expression + ".count()"))
        .containsExactlyInAnyOrderElementsOf(ids.stream().map(id -> id + "=0").toList());
  }

  /**
   * Evaluates an expression over each resource in the dataset, returning one {@code id=value} entry
   * per resource.
   */
  @Nonnull
  private List<String> evaluate(
      @Nonnull final Dataset<Row> dataset,
      @Nonnull final String resourceType,
      @Nonnull final String expression) {
    final DatasetEvaluator evaluator =
        DatasetEvaluatorBuilder.create(
                ResourceType.fromCode(resourceType), fhirEncoders.getContext())
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
  private Dataset<Row> encodeInLayout(
      @Nonnull final String resourceType, @Nonnull final IBaseResource... resources) {
    return LayoutDatasets.fromResources(
        spark, fhirEncoders, TestLayout.active(), resourceType, Arrays.asList(resources));
  }

  /** Encodes resources with the released encoder, whatever the active layout. */
  @Nonnull
  private Dataset<Row> encode(
      @Nonnull final String resourceType, @Nonnull final IBaseResource... resources) {
    return spark.createDataset(Arrays.asList(resources), fhirEncoders.of(resourceType)).toDF();
  }

  @Nonnull
  private Dataset<Row> parseAndEncode(@Nonnull final String json) {
    final IParser parser = fhirEncoders.getContext().newJsonParser();
    return encode("Patient", parser.parseResource(json));
  }

  /**
   * Reads a Patient in the layout's fitted shape, where {@code name} carries only the one named
   * field, of the given type.
   */
  @Nonnull
  private Dataset<Row> layoutPatient(
      @Nonnull final String json, @Nonnull final DataType fieldType, @Nonnull final String field) {
    final StructType schema =
        new StructType()
            .add("id", DataTypes.StringType)
            .add("name", DataTypes.createArrayType(new StructType().add(field, fieldType)));
    return spark.read().schema(schema).json(spark.createDataset(List.of(json), Encoders.STRING()));
  }

  private static boolean hasElement(@Nonnull final StructType schema, @Nonnull final String path) {
    DataType current = schema;
    for (final String step : path.split("\\.")) {
      while (current instanceof final ArrayType array) {
        current = array.elementType();
      }
      if (!(current instanceof final StructType struct)
          || Arrays.stream(struct.fieldNames()).noneMatch(step::equals)) {
        return false;
      }
      current = struct.apply(step).dataType();
    }
    return true;
  }

  // Fixtures. The elements the pruned schemas remove are never populated here.

  @Nonnull
  private static Patient patient1() {
    final Patient patient = new Patient();
    patient.setId("p1");
    patient.setGender(AdministrativeGender.FEMALE);
    patient.addName().addGiven("Ann").addGiven("Bea");
    patient.setMaritalStatus(
        new CodeableConcept(
            new Coding("http://terminology.hl7.org/CodeSystem/v3-MaritalStatus", "M", null)));
    patient
        .addContact()
        .addTelecom(
            new ContactPoint().setSystem(ContactPoint.ContactPointSystem.PHONE).setValue("123"));
    return patient;
  }

  @Nonnull
  private static Patient patient2() {
    final Patient patient = new Patient();
    patient.setId("p2");
    patient.setGender(AdministrativeGender.MALE);
    patient.addName().addGiven("Cal");
    patient.addName().addGiven("Dan");
    return patient;
  }

  @Nonnull
  private static Questionnaire questionnaire1() {
    final Questionnaire questionnaire = new Questionnaire();
    questionnaire.setId("q1");
    questionnaire.setStatus(PublicationStatus.ACTIVE);
    questionnaire.addItem().setLinkId("1").setType(QuestionnaireItemType.DISPLAY);
    return questionnaire;
  }

  @Nonnull
  private static Questionnaire questionnaire2() {
    final Questionnaire questionnaire = new Questionnaire();
    questionnaire.setId("q2");
    questionnaire.setStatus(PublicationStatus.ACTIVE);
    return questionnaire;
  }

  @Nonnull
  private static Observation observation1() {
    final Observation observation = new Observation();
    observation.setId("o1");
    observation.setValue(new Quantity().setValue(5).setUnit("mg"));
    final ObservationComponentComponent component = observation.addComponent();
    component.setCode(new CodeableConcept().setText("c"));
    component.setValue(new Quantity().setValue(3).setUnit("mg"));
    return observation;
  }

  @Nonnull
  private static Observation observation2() {
    final Observation observation = new Observation();
    observation.setId("o2");
    observation.addComponent().setCode(new CodeableConcept().setText("d"));
    return observation;
  }
}
