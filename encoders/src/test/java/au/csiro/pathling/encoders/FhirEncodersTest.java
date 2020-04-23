/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright © 2018-2020, Commonwealth Scientific
 * and Industrial Research Organisation (CSIRO) ABN 41 687 119 230. Licensed
 * under the CSIRO Open Source Software Licence Agreement.
 */

package au.csiro.pathling.encoders;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.hl7.fhir.exceptions.FHIRException;
import org.hl7.fhir.r4.model.*;
import org.hl7.fhir.r4.model.Provenance.ProvenanceEntityComponent;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test for FHIR encoders.
 */
public class FhirEncodersTest {

  private static final FhirEncoders encoders =
      FhirEncoders.forR4().getOrCreate();
  private static SparkSession spark;
  private static Patient patient = TestData.newPatient();
  private static Dataset<Patient> patientDataset;
  private static Patient decodedPatient;

  private static Condition condition = TestData.newCondition();
  private static Dataset<Condition> conditionsDataset;
  private static Condition decodedCondition;

  private static Observation observation = TestData.newObservation();
  private static Dataset<Observation> observationsDataset;
  private static Observation decodedObservation;

  private static MedicationRequest medRequest = TestData.newMedRequest();
  private static Dataset<MedicationRequest> medDataset;
  private static MedicationRequest decodedMedRequest;

  private static Encounter encounter = TestData.newEncounter();
  private static Dataset<Encounter> encounterDataset;
  private static Encounter decodedEncounter;

  /**
   * Set up Spark.
   */
  @BeforeClass
  public static void setUp() {
    spark = SparkSession.builder()
        .master("local[*]")
        .appName("testing")
        .getOrCreate();

    patientDataset = spark.createDataset(ImmutableList.of(patient),
        encoders.of(Patient.class));
    decodedPatient = patientDataset.head();

    conditionsDataset = spark.createDataset(ImmutableList.of(condition),
        encoders.of(Condition.class));
    decodedCondition = conditionsDataset.head();

    observationsDataset = spark.createDataset(ImmutableList.of(observation),
        encoders.of(Observation.class));
    decodedObservation = observationsDataset.head();

    medDataset = spark.createDataset(ImmutableList.of(medRequest),
        encoders.of(MedicationRequest.class, Medication.class, Provenance.class));
    decodedMedRequest = medDataset.head();

    encounterDataset = spark
        .createDataset(ImmutableList.of(encounter), encoders.of(Encounter.class));
    decodedEncounter = encounterDataset.head();
  }

  /**
   * Tear down Spark.
   */
  @AfterClass
  public static void tearDown() {
    spark.stop();
    spark = null;
  }

  @Test
  public void testResourceId() {

    Assert.assertEquals(condition.getId(),
        conditionsDataset.select("id").head().get(0));
    Assert.assertEquals(condition.getId(),
        decodedCondition.getId());
  }

  @Test
  public void testResourceLanguage() {

    Assert.assertEquals(condition.getLanguage(),
        conditionsDataset.select("language").head().get(0));
    Assert.assertEquals(condition.getLanguage(),
        decodedCondition.getLanguage());
  }

  @Test
  public void boundCode() {
    Row coding = (Row) conditionsDataset.select("verificationStatus")
        .head()
        .getStruct(0)
        .getList(1)
        .get(0);

    Assert.assertEquals(condition.getVerificationStatus().getCoding().size(), 1);
    Assert.assertEquals(condition.getVerificationStatus().getCodingFirstRep().getSystem(),
        coding.getString(1));
    Assert.assertEquals(condition.getVerificationStatus().getCodingFirstRep().getCode(),
        coding.getString(3));
  }

  @Test
  public void choiceValue() {

    // Our test condition uses the DateTime choice, so use that type and column.
    Assert.assertEquals(((DateTimeType) condition.getOnset()).getValueAsString(),
        conditionsDataset.select("onsetDateTime").head().get(0));

    Assert.assertEquals(condition.getOnset().toString(),
        decodedCondition.getOnset().toString());
  }

  @Test
  public void narrative() {

    Assert.assertEquals(condition.getText().getStatus().toCode(),
        conditionsDataset.select("text.status").head().get(0));
    Assert.assertEquals(condition.getText().getStatus(),
        decodedCondition.getText().getStatus());

    Assert.assertEquals(condition.getText().getDivAsString(),
        conditionsDataset.select("text.div").head().get(0));
    Assert.assertEquals(condition.getText().getDivAsString(),
        decodedCondition.getText().getDivAsString());
  }

  @Test
  public void coding() {

    Coding expectedCoding = condition.getSeverity().getCodingFirstRep();
    Coding actualCoding = decodedCondition.getSeverity().getCodingFirstRep();

    // Codings are a nested array, so we explode them into a table of the coding
    // fields so we can easily select and compare individual fields.
    Dataset<Row> severityCodings = conditionsDataset
        .select(functions.explode(conditionsDataset.col("severity.coding"))
            .alias("coding"))
        .select("coding.*") // Pull all fields in the coding to the top level.
        .cache();

    Assert.assertEquals(expectedCoding.getCode(),
        severityCodings.select("code").head().get(0));
    Assert.assertEquals(expectedCoding.getCode(),
        actualCoding.getCode());

    Assert.assertEquals(expectedCoding.getSystem(),
        severityCodings.select("system").head().get(0));
    Assert.assertEquals(expectedCoding.getSystem(),
        actualCoding.getSystem());

    Assert.assertEquals(expectedCoding.getUserSelected(),
        severityCodings.select("userSelected").head().get(0));
    Assert.assertEquals(expectedCoding.getUserSelected(),
        actualCoding.getUserSelected());

    Assert.assertEquals(expectedCoding.getDisplay(),
        severityCodings.select("display").head().get(0));
    Assert.assertEquals(expectedCoding.getDisplay(),
        actualCoding.getDisplay());
  }

  @Test
  public void reference() {

    Assert.assertEquals(condition.getSubject().getReference(),
        conditionsDataset.select("subject.reference").head().get(0));
    Assert.assertEquals(condition.getSubject().getReference(),
        decodedCondition.getSubject().getReference());
  }

  @Test
  public void integer() {

    Assert.assertEquals(((IntegerType) patient.getMultipleBirth()).getValue(),
        patientDataset.select("multipleBirthInteger").head().get(0));
    Assert.assertEquals(((IntegerType) patient.getMultipleBirth()).getValue(),
        ((IntegerType) decodedPatient.getMultipleBirth()).getValue());
  }

  @Test
  public void bigDecimal() {

    BigDecimal originalDecimal = ((Quantity) observation.getValue()).getValue();

    // Use compareTo since equals checks scale as well.
    Assert.assertTrue(originalDecimal.compareTo(
        (BigDecimal) observationsDataset.select("valueQuantity.value")
            .head()
            .get(0)) == 0);

    Assert.assertEquals(originalDecimal.compareTo(
        ((Quantity) decodedObservation
            .getValue())
            .getValue()), 0);
  }

  @Test
  public void annotation() throws FHIRException {

    Annotation original = medRequest.getNoteFirstRep();
    Annotation decoded = decodedMedRequest.getNoteFirstRep();

    Assert.assertEquals(original.getText(),
        medDataset.select(functions.expr("note[0].text")).head().get(0));

    Assert.assertEquals(original.getText(), decoded.getText());
    Assert.assertEquals(original.getAuthorReference().getReference(),
        decoded.getAuthorReference().getReference());

  }

  @Test
  public void contained() throws FHIRException {

    // Contained resources should be put to the Contained list in order of the Encoder arguments
    Assert.assertTrue(decodedMedRequest.getContained().get(0) instanceof Medication);

    Medication originalMedication = (Medication) medRequest.getContained().get(0);
    Medication decodedMedication = (Medication) decodedMedRequest.getContained().get(0);

    Assert.assertEquals(originalMedication.getId(), decodedMedication.getId());
    Assert.assertEquals(originalMedication.getIngredientFirstRep()
            .getItemCodeableConcept()
            .getCodingFirstRep()
            .getCode(),
        decodedMedication.getIngredientFirstRep()
            .getItemCodeableConcept()
            .getCodingFirstRep()
            .getCode());

    Assert.assertTrue(decodedMedRequest.getContained().get(1) instanceof Provenance);

    Provenance decodedProvenance = (Provenance) decodedMedRequest.getContained().get(1);
    Provenance originalProvenance = (Provenance) medRequest.getContained().get(1);

    Assert.assertEquals(originalProvenance.getId(), decodedProvenance.getId());
    Assert.assertEquals(originalProvenance.getTargetFirstRep().getReference(),
        decodedProvenance.getTargetFirstRep().getReference());

    ProvenanceEntityComponent originalEntity = originalProvenance.getEntityFirstRep();
    ProvenanceEntityComponent decodedEntity = decodedProvenance.getEntityFirstRep();

    Assert.assertEquals(originalEntity.getRole(), decodedEntity.getRole());
    Assert.assertEquals(originalEntity.getWhat().getReference(),
        decodedEntity.getWhat().getReference());
  }

  /**
   * Sanity test with a deep copy to check we didn't break internal state used by copies.
   */
  @Test
  public void testCopyDecoded() {
    Assert.assertEquals(condition.getId(), decodedCondition.copy().getId());
    Assert.assertEquals(medRequest.getId(), decodedMedRequest.copy().getId());
    Assert.assertEquals(observation.getId(), decodedObservation.copy().getId());
    Assert.assertEquals(patient.getId(), decodedPatient.copy().getId());
  }

  @Test
  public void testEmptyAttributes() {
    Map<String, String> attributes = decodedMedRequest.getText().getDiv().getAttributes();

    Assert.assertNotNull(attributes);
    Assert.assertEquals(0, attributes.size());
  }

  @Test
  public void testFromRdd() {

    JavaSparkContext context = new JavaSparkContext(spark.sparkContext());

    JavaRDD<Condition> conditionRdd = context.parallelize(ImmutableList.of(condition));

    Dataset<Condition> ds = spark.createDataset(conditionRdd.rdd(),
        encoders.of(Condition.class));

    Condition convertedCondition = ds.head();

    Assert.assertEquals(condition.getId(),
        convertedCondition.getId());
  }

  @Test
  public void testFromParquet() throws IOException {

    Path dirPath = Files.createTempDirectory("encoder_test");

    String path = dirPath.resolve("out.parquet").toString();

    conditionsDataset.write().save(path);

    Dataset<Condition> ds = spark.read()
        .parquet(path)
        .as(encoders.of(Condition.class));

    Condition readCondition = ds.head();

    Assert.assertEquals(condition.getId(),
        readCondition.getId());
  }

  @Test
  public void testEncoderCached() throws IOException {

    Assert.assertSame(encoders.of(Condition.class),
        encoders.of(Condition.class));

    Assert.assertSame(encoders.of(Patient.class),
        encoders.of(Patient.class));
  }

  @Test
  public void testPrimitiveClassDecoding() {
    Assert.assertEquals(encounter.getClass_().getCode(),
        encounterDataset.select("class.code").head().get(0));
    Assert.assertEquals(encounter.getClass_().getCode(), decodedEncounter.getClass_().getCode());
  }
}
