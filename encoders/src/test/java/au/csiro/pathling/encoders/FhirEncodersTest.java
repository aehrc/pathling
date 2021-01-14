/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright © 2018-2021, Commonwealth Scientific
 * and Industrial Research Organisation (CSIRO) ABN 41 687 119 230. Licensed
 * under the CSIRO Open Source Software Licence Agreement.
 */

package au.csiro.pathling.encoders;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Date;
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
  private static Dataset<Observation> observationsDataset;
  private static Observation decodedObservation;
  private static Dataset<Condition> conditionsWithVersionDataset;
  private static Dataset<MedicationRequest> medDataset;
  private static MedicationRequest decodedMedRequest;
  private static Condition decodedConditionWithVersion;
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

    conditionsWithVersionDataset = spark.createDataset(ImmutableList.of(conditionWithVersion),
        encoders.of(Condition.class));
    decodedConditionWithVersion = conditionsWithVersionDataset.head();

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
  public void testResourceWithVersionId() {
    Assert.assertEquals("with-version",
        conditionsWithVersionDataset.select("id").head().get(0));

    // A current limitation of the implementation is that technical version information is not
    // preserved on the way out.
    //
    // TODO: Work out a way to preserve the technical version information when serializing the data
    //  back out.
    Assert.assertEquals("with-version",
        decodedConditionWithVersion.getId());
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
    final Row coding = (Row) conditionsDataset.select("verificationStatus")
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

    final Coding expectedCoding = condition.getSeverity().getCodingFirstRep();
    final Coding actualCoding = decodedCondition.getSeverity().getCodingFirstRep();

    // Codings are a nested array, so we explode them into a table of the coding
    // fields so we can easily select and compare individual fields.
    final Dataset<Row> severityCodings = conditionsDataset
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

    final BigDecimal originalDecimal = ((Quantity) observation.getValue()).getValue();
    final BigDecimal queriedDecimal = (BigDecimal) observationsDataset.select("valueQuantity.value")
        .head()
        .get(0);
    final int queriedDecimal_scale = observationsDataset.select("valueQuantity.value_scale")
        .head()
        .getInt(0);

    // we expect the values to be the same but they may differ in scale
    Assert.assertEquals(0, originalDecimal.compareTo(queriedDecimal));
    Assert.assertEquals(originalDecimal.scale(), queriedDecimal_scale);

    final BigDecimal decodedDecimal = ((Quantity) decodedObservation.getValue()).getValue();
    // here we expect same value,  scale and precision
    Assert.assertEquals(originalDecimal, decodedDecimal);

    // Test can represent without loss 18 + 6 decimal places
    Assert.assertEquals(TestData.TEST_VERY_BIG_DECIMAL,
        decodedObservation.getReferenceRange().get(0).getHigh().getValue());

    // Test rounding of decimals with scale larger than 6
    Assert.assertEquals(TestData.TEST_VERY_SMALL_DECIMAL_SCALE_6,
        decodedObservation.getReferenceRange().get(0).getLow().getValue());
  }

  @Test
  public void instant() {
    final Date originalInstant = TestData.TEST_DATE;

    Assert.assertEquals(originalInstant,
        observationsDataset.select("issued")
            .head()
            .get(0));

    Assert.assertEquals(originalInstant, decodedObservation.getIssued());
  }

  @Test
  public void annotation() throws FHIRException {

    final Annotation original = medRequest.getNoteFirstRep();
    final Annotation decoded = decodedMedRequest.getNoteFirstRep();

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

    final Medication originalMedication = (Medication) medRequest.getContained().get(0);
    final Medication decodedMedication = (Medication) decodedMedRequest.getContained().get(0);

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

    final Provenance decodedProvenance = (Provenance) decodedMedRequest.getContained().get(1);
    final Provenance originalProvenance = (Provenance) medRequest.getContained().get(1);

    Assert.assertEquals(originalProvenance.getId(), decodedProvenance.getId());
    Assert.assertEquals(originalProvenance.getTargetFirstRep().getReference(),
        decodedProvenance.getTargetFirstRep().getReference());

    final ProvenanceEntityComponent originalEntity = originalProvenance.getEntityFirstRep();
    final ProvenanceEntityComponent decodedEntity = decodedProvenance.getEntityFirstRep();

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
    final Map<String, String> attributes = decodedMedRequest.getText().getDiv().getAttributes();

    Assert.assertNotNull(attributes);
    Assert.assertEquals(0, attributes.size());
  }

  @Test
  public void testFromRdd() {

    final JavaSparkContext context = new JavaSparkContext(spark.sparkContext());

    final JavaRDD<Condition> conditionRdd = context.parallelize(ImmutableList.of(condition));

    final Dataset<Condition> ds = spark.createDataset(conditionRdd.rdd(),
        encoders.of(Condition.class));

    final Condition convertedCondition = ds.head();

    Assert.assertEquals(condition.getId(),
        convertedCondition.getId());
  }

  @Test
  public void testFromParquet() throws IOException {

    final Path dirPath = Files.createTempDirectory("encoder_test");

    final String path = dirPath.resolve("out.parquet").toString();

    conditionsDataset.write().save(path);

    final Dataset<Condition> ds = spark.read()
        .parquet(path)
        .as(encoders.of(Condition.class));

    final Condition readCondition = ds.head();

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
