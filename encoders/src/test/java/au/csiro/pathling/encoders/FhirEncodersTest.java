/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright 2023 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.encoders;

import static org.apache.spark.sql.functions.col;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import ca.uhn.fhir.parser.IParser;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema;
import org.apache.spark.sql.functions;
import org.hl7.fhir.exceptions.FHIRException;
import org.hl7.fhir.r4.model.Annotation;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.Condition;
import org.hl7.fhir.r4.model.DateTimeType;
import org.hl7.fhir.r4.model.Encounter;
import org.hl7.fhir.r4.model.IntegerType;
import org.hl7.fhir.r4.model.MedicationRequest;
import org.hl7.fhir.r4.model.Observation;
import org.hl7.fhir.r4.model.Patient;
import org.hl7.fhir.r4.model.Quantity;
import org.hl7.fhir.r4.model.Quantity.QuantityComparator;
import org.hl7.fhir.r4.model.Questionnaire;
import org.hl7.fhir.r4.model.QuestionnaireResponse;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Test for FHIR encoders.
 */
class FhirEncodersTest {

  private static final int NESTING_LEVEL_3 = 3;

  private static final FhirEncoders ENCODERS_L0 =
      FhirEncoders.forR4().getOrCreate();

  private static final FhirEncoders ENCODERS_L3 =
      FhirEncoders.forR4().withMaxNestingLevel(NESTING_LEVEL_3).getOrCreate();


  private static SparkSession spark;
  private static final Patient patient = TestData.newPatient();
  private static Dataset<Patient> patientDataset;
  private static Patient decodedPatient;

  private static final Condition condition = TestData.newCondition();
  private static Dataset<Condition> conditionsDataset;
  private static Condition decodedCondition;

  private static final Condition conditionWithVersion = TestData.conditionWithVersion();
  private static final Observation observation = TestData.newObservation();
  private static final MedicationRequest medRequest = TestData.newMedRequest();
  private static final Encounter encounter = TestData.newEncounter();
  private static final Questionnaire questionnaire = TestData.newQuestionnaire();
  private static final QuestionnaireResponse questionnaireResponse = TestData
      .newQuestionnaireResponse();

  private static Dataset<Observation> observationsDataset;
  private static Observation decodedObservation;
  private static Dataset<Condition> conditionsWithVersionDataset;
  private static Dataset<MedicationRequest> medDataset;
  private static MedicationRequest decodedMedRequest;
  private static Condition decodedConditionWithVersion;
  private static Dataset<Encounter> encounterDataset;
  private static Encounter decodedEncounter;
  private static Dataset<Questionnaire> questionnaireDataset;
  private static Questionnaire decodedQuestionnaire;
  private static Dataset<QuestionnaireResponse> questionnaireResponseDataset;
  private static QuestionnaireResponse decodedQuestionnaireResponse;

  /**
   * Set up Spark.
   */
  @BeforeAll
  static void setUp() {
    spark = SparkSession.builder()
        .master("local[*]")
        .appName("testing")
        .config("spark.driver.bindAddress", "localhost")
        .config("spark.driver.host", "localhost")
        .getOrCreate();

    patientDataset = spark.createDataset(List.of(patient), ENCODERS_L0.of(Patient.class));
    decodedPatient = patientDataset.head();

    conditionsDataset = spark.createDataset(List.of(condition), ENCODERS_L0.of(Condition.class));
    decodedCondition = conditionsDataset.head();

    conditionsWithVersionDataset = spark.createDataset(List.of(conditionWithVersion),
        ENCODERS_L0.of(Condition.class));
    decodedConditionWithVersion = conditionsWithVersionDataset.head();

    observationsDataset = spark.createDataset(List.of(observation),
        ENCODERS_L0.of(Observation.class));
    decodedObservation = observationsDataset.head();

    medDataset = spark.createDataset(List.of(medRequest),
        ENCODERS_L0.of(MedicationRequest.class));
    decodedMedRequest = medDataset.head();

    encounterDataset = spark.createDataset(List.of(encounter), ENCODERS_L0.of(Encounter.class));
    decodedEncounter = encounterDataset.head();

    questionnaireDataset = spark.createDataset(List.of(questionnaire),
        ENCODERS_L0.of(Questionnaire.class));
    decodedQuestionnaire = questionnaireDataset.head();

    questionnaireResponseDataset = spark.createDataset(List.of(questionnaireResponse),
        ENCODERS_L0.of(QuestionnaireResponse.class));
    decodedQuestionnaireResponse = questionnaireResponseDataset.head();
  }

  /**
   * Tear down Spark.
   */
  @AfterAll
  static void tearDown() {
    spark.stop();
  }

  @Test
  void testResourceId() {
    assertEquals(condition.getId(),
        conditionsDataset.select("id").head().get(0));
    assertEquals(condition.getId(),
        decodedCondition.getId());
  }

  @Test
  void testResourceWithVersionId() {
    assertEquals("with-version",
        conditionsWithVersionDataset.select("id").head().get(0));

    assertEquals(conditionWithVersion.getId(),
        conditionsWithVersionDataset.select("id_versioned").head().get(0));

    assertEquals(conditionWithVersion.getId(),
        decodedConditionWithVersion.getId());
  }

  @Test
  void testResourceLanguage() {
    assertEquals(condition.getLanguage(),
        conditionsDataset.select("language").head().get(0));
    assertEquals(condition.getLanguage(),
        decodedCondition.getLanguage());
  }

  @Test
  void boundCode() {

    final GenericRowWithSchema verificationStatus = (GenericRowWithSchema) conditionsDataset
        .select("verificationStatus")
        .head()
        .getStruct(0);

    final GenericRowWithSchema coding = (GenericRowWithSchema) verificationStatus
        .getList(verificationStatus.fieldIndex("coding"))
        .get(0);

    assertEquals(1, condition.getVerificationStatus().getCoding().size());
    assertEquals(coding.getString(coding.fieldIndex("system")),
        condition.getVerificationStatus().getCodingFirstRep().getSystem());
    assertEquals(coding.getString(coding.fieldIndex("code")),
        condition.getVerificationStatus().getCodingFirstRep().getCode());
  }

  @Test
  void choiceValue() {

    // Our test condition uses the DateTime choice, so use that type and column.
    assertEquals(((DateTimeType) condition.getOnset()).getValueAsString(),
        conditionsDataset.select("onsetDateTime").head().get(0));

    assertEquals(condition.getOnset().toString(),
        decodedCondition.getOnset().toString());
  }

  @Test
  void narrative() {

    assertEquals(condition.getText().getStatus().toCode(),
        conditionsDataset.select("text.status").head().get(0));
    assertEquals(condition.getText().getStatus(),
        decodedCondition.getText().getStatus());

    assertEquals(condition.getText().getDivAsString(),
        conditionsDataset.select("text.div").head().get(0));
    assertEquals(condition.getText().getDivAsString(),
        decodedCondition.getText().getDivAsString());
  }

  @Test
  void coding() {

    final Coding expectedCoding = condition.getSeverity().getCodingFirstRep();
    final Coding actualCoding = decodedCondition.getSeverity().getCodingFirstRep();

    // Codings are a nested array, so we explode them into a table of the coding
    // fields, so we can easily select and compare individual fields.
    final Dataset<Row> severityCodings = conditionsDataset
        .select(functions.explode(conditionsDataset.col("severity.coding"))
            .alias("coding"))
        .select("coding.*") // Pull all fields in the coding to the top level.
        .cache();

    assertEquals(expectedCoding.getCode(),
        severityCodings.select("code").head().get(0));
    assertEquals(expectedCoding.getCode(),
        actualCoding.getCode());

    assertEquals(expectedCoding.getSystem(),
        severityCodings.select("system").head().get(0));
    assertEquals(expectedCoding.getSystem(),
        actualCoding.getSystem());

    assertEquals(expectedCoding.getUserSelected(),
        severityCodings.select("userSelected").head().get(0));
    assertEquals(expectedCoding.getUserSelected(),
        actualCoding.getUserSelected());

    assertEquals(expectedCoding.getDisplay(),
        severityCodings.select("display").head().get(0));
    assertEquals(expectedCoding.getDisplay(),
        actualCoding.getDisplay());
  }

  @Test
  void reference() {
    final Condition conditionWithReferences = TestData.conditionWithReferencesWithIdentifiers();

    final Dataset<Condition> conditionL3Dataset = spark.createDataset(
        List.of(conditionWithReferences), ENCODERS_L3.of(Condition.class));

    final Condition decodedL3Condition = conditionL3Dataset.head();

    assertEquals(
        RowFactory.create(
            "withReferencesWithIdentifiers",
            "Patient/example",
            "http://terminology.hl7.org/CodeSystem/v2-0203",
            "MR",
            "https://fhir.example.com/identifiers/mrn",
            "urn:id"
        ),
        conditionL3Dataset.select(
            col("id"),
            col("subject.reference"),
            col("subject.identifier.type.coding.system").getItem(0),
            col("subject.identifier.type.coding.code").getItem(0),
            col("subject.identifier.system"),
            col("subject.identifier.value")
        ).head());

    assertEquals("Patient/example",
        decodedL3Condition.getSubject().getReference());

    assertEquals("urn:id",
        decodedL3Condition.getSubject().getIdentifier().getValue());

    // the assigner should be pruned from the reference identifier.
    assertTrue(conditionWithReferences.getSubject().getIdentifier().hasAssigner());
    assertFalse(decodedL3Condition.getSubject().getIdentifier().hasAssigner());
  }


  @Test
  void identifier() {
    final Condition conditionWithIdentifiers = TestData.conditionWithIdentifiersWithReferences();

    final Dataset<Condition> conditionL3Dataset = spark.createDataset(
        List.of(conditionWithIdentifiers), ENCODERS_L3.of(Condition.class));

    final Condition decodedL3Condition = conditionL3Dataset.head();

    assertEquals(
        RowFactory.create(
            "withIdentifiersWithReferences",
            "http://terminology.hl7.org/CodeSystem/v2-0203",
            "MR",
            "https://fhir.example.com/identifiers/mrn",
            "urn:id01",
            "Organization/001",
            "urn:id02"
        ),
        conditionL3Dataset.select(
            col("id"),
            col("identifier.type.coding").getItem(0).getField("system").getItem(0),
            col("identifier.type.coding").getItem(0).getField("code").getItem(0),
            col("identifier.system").getItem(0),
            col("identifier.value").getItem(0),
            col("identifier.assigner.reference").getItem(0),
            col("identifier.assigner.identifier.value").getItem(0)
        ).head());

    // the assigner should be pruned from the reference identifier.
    assertTrue(conditionWithIdentifiers.getIdentifier().get(0).getAssigner().getIdentifier()
        .hasAssigner());
    assertFalse(
        decodedL3Condition.getIdentifier().get(0).getAssigner().getIdentifier().hasAssigner());
  }


  @Test
  void integer() {

    assertEquals(((IntegerType) patient.getMultipleBirth()).getValue(),
        patientDataset.select("multipleBirthInteger").head().get(0));
    assertEquals(((IntegerType) patient.getMultipleBirth()).getValue(),
        ((IntegerType) decodedPatient.getMultipleBirth()).getValue());
  }

  @Test
  void bigDecimal() {

    final BigDecimal originalDecimal = ((Quantity) observation.getValue()).getValue();
    final BigDecimal queriedDecimal = (BigDecimal) observationsDataset.select("valueQuantity.value")
        .head()
        .get(0);
    final int queriedDecimal_scale = observationsDataset.select("valueQuantity.value_scale")
        .head()
        .getInt(0);

    // we expect the values to be the same, but they may differ in scale
    assertEquals(0, originalDecimal.compareTo(queriedDecimal));
    assertEquals(originalDecimal.scale(), queriedDecimal_scale);

    final BigDecimal decodedDecimal = ((Quantity) decodedObservation.getValue()).getValue();
    // here we expect same value,  scale and precision
    assertEquals(originalDecimal, decodedDecimal);

    // Test can represent without loss 18 + 6 decimal places
    assertEquals(TestData.TEST_VERY_BIG_DECIMAL,
        decodedObservation.getReferenceRange().get(0).getHigh().getValue());

    // Test rounding of decimals with scale larger than 6
    assertEquals(TestData.TEST_VERY_SMALL_DECIMAL_SCALE_6,
        decodedObservation.getReferenceRange().get(0).getLow().getValue());
  }


  @Test
  void choiceBigDecimalInQuestionnaire() {

    final BigDecimal originalDecimal = questionnaire.getItemFirstRep().getEnableWhenFirstRep()
        .getAnswerDecimalType().getValue();

    final BigDecimal queriedDecimal = (BigDecimal) questionnaireDataset
        .select(col("item").getItem(0).getField("enableWhen").getItem(0)
            .getField("answerDecimal"))
        .head()
        .get(0);

    final int queriedDecimal_scale = questionnaireDataset
        .select(col("item").getItem(0).getField("enableWhen").getItem(0)
            .getField("answerDecimal_scale"))
        .head()
        .getInt(0);

    // we expect the values to be the same, but they may differ in scale
    assertEquals(0, originalDecimal.compareTo(queriedDecimal));
    assertEquals(originalDecimal.scale(), queriedDecimal_scale);

    final BigDecimal decodedDecimal = decodedQuestionnaire.getItemFirstRep().getEnableWhenFirstRep()
        .getAnswerDecimalType().getValue();
    // here we expect same value,  scale and precision
    assertEquals(originalDecimal, decodedDecimal);

    // Test can represent without loss 18 + 6 decimal places
    assertEquals(TestData.TEST_VERY_BIG_DECIMAL,
        decodedQuestionnaire.getItemFirstRep().getInitialFirstRep().getValueDecimalType()
            .getValue());

    // Nested item should not be present.
    assertTrue(decodedQuestionnaire.getItemFirstRep().getItem().isEmpty());
  }

  @Test
  void choiceBigDecimalInQuestionnaireResponse() {
    final BigDecimal originalDecimal = questionnaireResponse.getItemFirstRep().getAnswerFirstRep()
        .getValueDecimalType().getValue();

    final BigDecimal queriedDecimal = (BigDecimal) questionnaireResponseDataset
        .select(col("item").getItem(0).getField("answer").getItem(0)
            .getField("valueDecimal"))
        .head()
        .get(0);

    final int queriedDecimal_scale = questionnaireResponseDataset
        .select(col("item").getItem(0).getField("answer").getItem(0)
            .getField("valueDecimal_scale"))
        .head()
        .getInt(0);

    assertEquals(0, originalDecimal.compareTo(queriedDecimal));
    assertEquals(originalDecimal.scale(), queriedDecimal_scale);

    final BigDecimal decodedDecimal = decodedQuestionnaireResponse.getItemFirstRep()
        .getAnswerFirstRep().getValueDecimalType().getValue();
    assertEquals(originalDecimal, decodedDecimal);

    assertEquals(TestData.TEST_VERY_SMALL_DECIMAL_SCALE_6,
        decodedQuestionnaireResponse.getItemFirstRep().getAnswerFirstRep().getValueDecimalType()
            .getValue());
    assertEquals(TestData.TEST_VERY_BIG_DECIMAL,
        decodedQuestionnaireResponse.getItemFirstRep().getAnswer().get(1).getValueDecimalType()
            .getValue());
  }

  @Test
  void instant() {
    final Date originalInstant = TestData.TEST_DATE;

    assertEquals(originalInstant,
        observationsDataset.select("issued")
            .head()
            .get(0));

    assertEquals(originalInstant, decodedObservation.getIssued());
  }

  @Test
  void annotation() throws FHIRException {

    final Annotation original = medRequest.getNoteFirstRep();
    final Annotation decoded = decodedMedRequest.getNoteFirstRep();

    assertEquals(original.getText(),
        medDataset.select(functions.expr("note[0].text")).head().get(0));

    assertEquals(original.getText(), decoded.getText());
    assertEquals(original.getAuthorReference().getReference(),
        decoded.getAuthorReference().getReference());

  }

  /**
   * Sanity test with a deep copy to check we didn't break internal state used by copies.
   */
  @Test
  void testCopyDecoded() {
    assertEquals(condition.getId(), decodedCondition.copy().getId());
    assertEquals(medRequest.getId(), decodedMedRequest.copy().getId());
    assertEquals(observation.getId(), decodedObservation.copy().getId());
    assertEquals(patient.getId(), decodedPatient.copy().getId());
  }

  @Test
  void testEmptyAttributes() {
    final Map<String, String> attributes = decodedMedRequest.getText().getDiv().getAttributes();

    assertNotNull(attributes);
    assertEquals(0, attributes.size());
  }

  @Test
  void testFromRdd() {
    // This JavaSparkContext is only thin wrapper around the SparkContext, 
    // which will be closed when SparkSession is closed.
    // It cannot be closed here as it will cause SparkSession used by other tests to fail.

    @SuppressWarnings("resource") final JavaSparkContext context = new JavaSparkContext(
        spark.sparkContext());
    final JavaRDD<Condition> conditionRdd = context.parallelize(List.of(condition));

    final Dataset<Condition> ds = spark.createDataset(conditionRdd.rdd(),
        ENCODERS_L0.of(Condition.class));

    final Condition convertedCondition = ds.head();

    assertEquals(condition.getId(),
        convertedCondition.getId());

  }

  @Test
  void testFromParquet() throws IOException {

    final Path dirPath = Files.createTempDirectory("encoder_test");

    final String path = dirPath.resolve("out.parquet").toString();

    conditionsDataset.write().save(path);

    final Dataset<Condition> ds = spark.read()
        .parquet(path)
        .as(ENCODERS_L0.of(Condition.class));

    final Condition readCondition = ds.head();

    assertEquals(condition.getId(),
        readCondition.getId());
  }

  @Test
  void testEncoderCached() {

    assertSame(ENCODERS_L0.of(Condition.class),
        ENCODERS_L0.of(Condition.class));

    assertSame(ENCODERS_L0.of(Patient.class),
        ENCODERS_L0.of(Patient.class));
  }

  @Test
  void testPrimitiveClassDecoding() {
    assertEquals(encounter.getClass_().getCode(),
        encounterDataset.select("class.code").head().get(0));
    assertEquals(encounter.getClass_().getCode(), decodedEncounter.getClass_().getCode());
  }

  @Test
  void testNestedQuestionnaire() {

    // Build a list of questionnaires with increasing nesting levels
    final List<Questionnaire> questionnaires = IntStream.rangeClosed(0, NESTING_LEVEL_3)
        .mapToObj(i -> TestData.newNestedQuestionnaire(i, 4))
        .toList();

    final Questionnaire questionnaireL0 = questionnaires.get(0);

    // Encode with level 0
    final Dataset<Questionnaire> questionnaireDatasetL0 = spark
        .createDataset(questionnaires,
            ENCODERS_L0.of(Questionnaire.class));
    final List<Questionnaire> decodedQuestionnairesL0 = questionnaireDatasetL0.collectAsList();

    // all questionnaires should be pruned to be the same as level 0
    for (int i = 0; i <= NESTING_LEVEL_3; i++) {
      assertTrue(questionnaireL0.equalsDeep(decodedQuestionnairesL0.get(i)));
    }

    // Encode with level 3
    final Dataset<Questionnaire> questionnaireDatasetL3 = spark
        .createDataset(questionnaires,
            ENCODERS_L3.of(Questionnaire.class));
    final List<Questionnaire> decodedQuestionnairesL3 = questionnaireDatasetL3.collectAsList();
    // All questionnaires should be fully encoded
    for (int i = 0; i <= NESTING_LEVEL_3; i++) {
      assertTrue(questionnaires.get(i).equalsDeep(decodedQuestionnairesL3.get(i)));
    }

    assertEquals(Stream.of("Item/0", "Item/0", "Item/0", "Item/0").map(RowFactory::create)
            .toList(),
        questionnaireDatasetL3.select(col("item").getItem(0).getField("linkId"))
            .collectAsList());

    assertEquals(Stream.of(null, "Item/1.0", "Item/1.0", "Item/1.0").map(RowFactory::create)
            .toList(),
        questionnaireDatasetL3
            .select(col("item")
                .getItem(1).getField("item")
                .getItem(0).getField("linkId"))
            .collectAsList());

    assertEquals(Stream.of(null, null, "Item/2.1.0", "Item/2.1.0").map(RowFactory::create)
            .toList(),
        questionnaireDatasetL3
            .select(col("item")
                .getItem(2).getField("item")
                .getItem(1).getField("item")
                .getItem(0).getField("linkId"))
            .collectAsList());

    assertEquals(Stream.of(null, null, null, "Item/3.2.1.0").map(RowFactory::create)
            .toList(),
        questionnaireDatasetL3
            .select(col("item")
                .getItem(3).getField("item")
                .getItem(2).getField("item")
                .getItem(1).getField("item")
                .getItem(0).getField("linkId"))
            .collectAsList());
  }

  @Test
  void testQuantityComparator() {
    final QuantityComparator originalComparator = observation.getValueQuantity().getComparator();
    final String queriedComparator = observationsDataset.select("valueQuantity.comparator").head()
        .getString(0);

    assertEquals(originalComparator.toCode(), queriedComparator);
  }


  @Test
  void traversalToChoiceFieldsFromJson() {
    // this tests for the issue reported in: https://github.com/aehrc/pathling/issues/1770
    final Observation observationWithQuantityValue = (Observation) new Observation()
        .setValue(new Quantity(139.99))
        .setId("obs1");
    final Dataset<Observation> encodedObservations = spark.createDataset(
        List.of(observationWithQuantityValue), ENCODERS_L0.of(Observation.class));
    final Row subjectRow = encodedObservations
        // map() introduces the issue, as it traverses to the choice field
        .map((MapFunction<Observation, Observation>) x -> x, ENCODERS_L0.of(Observation.class))
        .toDF().select("valueQuantity.value")
        .first();
    assertFalse(subjectRow.isNullAt(0));
    assertEquals(new BigDecimal("139.990000"), subjectRow.getDecimal(0));
  }

  @Test
  void nullEncoding() {
    // empty elements of all types should be encoded as nulls
    final Observation emptyObservation = new Observation();
    assertFalse(emptyObservation.hasSubject());
    final Dataset<Observation> encodedObservations = spark.createDataset(
        List.of(emptyObservation),
        ENCODERS_L0.of(Observation.class));
    // 'subject' is a struct 
    // 'identifier' is an array of struct
    //  'status' is a primitive type
    final Row subjectRow = encodedObservations.toDF().select("subject", "identifier", "status")
        .first();
    assertTrue(subjectRow.isNullAt(0));
    assertTrue(subjectRow.isNullAt(1));
    assertTrue(subjectRow.isNullAt(2));
  }


  @Test
  void nullEncodingFromJson() {
    final IParser parser = ENCODERS_L0.getContext().newJsonParser();
    final Observation emptyObservationFromJson = parser.parseResource(Observation.class,
        "{ \"resourceType\": \"Observation\"}");
    assertFalse(emptyObservationFromJson.hasSubject());
    final Dataset<Observation> encodedObservations = spark.createDataset(
        List.of(emptyObservationFromJson),
        ENCODERS_L0.of(Observation.class));
    // 'subject' is a struct 
    // 'identifier' is an array of struct
    //  'status' is a primitive type
    final Row subjectRow = encodedObservations.toDF().select("subject", "identifier", "status")
        .first();
    assertTrue(subjectRow.isNullAt(0));
    assertTrue(subjectRow.isNullAt(1));
    assertTrue(subjectRow.isNullAt(2));
  }


}
