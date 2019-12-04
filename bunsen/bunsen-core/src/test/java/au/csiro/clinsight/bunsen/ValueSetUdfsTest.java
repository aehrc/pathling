/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright © Australian e-Health Research Centre,
 * CSIRO. All rights reserved.
 */

package au.csiro.clinsight.bunsen;

import au.csiro.clinsight.bunsen.codes.Hierarchies;
import au.csiro.clinsight.bunsen.codes.broadcast.BroadcastableValueSets;
import au.csiro.clinsight.bunsen.codes.systems.Loinc;
import au.csiro.clinsight.bunsen.codes.systems.Snomed;
import ca.uhn.fhir.context.FhirContext;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.nio.file.Files;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.CodeableConcept;
import org.hl7.fhir.r4.model.Condition;
import org.hl7.fhir.r4.model.Encounter;
import org.hl7.fhir.r4.model.Observation;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit test for {@link ValueSetUdfs}.
 */
public class ValueSetUdfsTest {

  private static final FhirEncoders encoders = new FhirEncoders(FhirContext.forR4(),
      new TestDataTypeMappings());

  private static SparkSession spark;

  private static CodeableConcept codeable(String system, String value) {

    CodeableConcept concept = new CodeableConcept();

    concept.addCoding()
        .setSystem(system)
        .setCode(value);

    return concept;
  }

  private static Observation observation(String id, String code) {

    Observation observation = new Observation();

    observation.setId(id);
    observation.setCode(codeable(Loinc.LOINC_CODE_SYSTEM_URI, code));

    return observation;
  }

  private static Condition condition(String id, String code) {

    Condition condition = new Condition();

    // Condition based on example from FHIR:
    // https://www.hl7.org/fhir/condition-example.json.html
    condition.setId(id);

    condition.setCode(codeable(Snomed.SNOMED_CODE_SYSTEM_URI, code));

    return condition;
  }

  private static Encounter encounter(String id, String type, String priority) {

    Encounter encounter = new Encounter();

    encounter.setId(id);

    // CodeableConcept array field
    encounter.setType(ImmutableList.of(codeable("http://www.ama-assn.org/go/cpt", type)));

    // CodeableConcept singleton field
    encounter.setPriority(codeable("http://hl7.org/fhir/v3/ActPriority", priority));

    return encounter;
  }

  /**
   * Sets up Spark and loads test value sets.
   */
  @BeforeClass
  public static void setUp() throws IOException {

    // Create a local spark session using an in-memory metastore.
    // We must also use Hive and set the partition mode to non-strict to
    // support dynamic partitions.
    spark = SparkSession.builder()
        .master("local[2]")
        .appName("UdfsTest")
        .enableHiveSupport()
        .config("javax.jdo.option.ConnectionURL",
            "jdbc:derby:memory:metastore_db;create=true")
        .config("hive.exec.dynamic.partition.mode",
            "nonstrict")
        .config("spark.sql.warehouse.dir",
            Files.createTempDirectory("spark_warehouse").toString())
        .getOrCreate();

    spark.sql("create database ontologies");

    Hierarchies withLoinc = Loinc.withLoincHierarchy(spark,
        Hierarchies.getEmpty(spark),
        "src/test/resources/LOINC_HIERARCHY_SAMPLE.CSV",
        "2.56");

    Hierarchies withLoincAndSnomed = Snomed.withRelationships(spark,
        withLoinc,
        "src/test/resources/SNOMED_RELATIONSHIP_SAMPLE.TXT",
        "20160901");

    MockValueSets mockValueSets = MockValueSets.createWithTestValue(spark, encoders);

    BroadcastableValueSets valueSets = BroadcastableValueSets.newBuilder()
        .addCode("bp",
            Loinc.LOINC_CODE_SYSTEM_URI,
            "8462-4")
        .addCode("albumin",
            Loinc.LOINC_CODE_SYSTEM_URI,
            "14959-1")
        .addReference("types",
            "http://hl7.org/fhir/us/core/ValueSet/us-core-encounter-type")
        .addReference("priorities",
            "http://hl7.org/fhir/ValueSet/v3-ActPriority")
        .addDescendantsOf("leukocytes",
            Loinc.LOINC_CODE_SYSTEM_URI,
            "LP14419-3",
            Loinc.LOINC_HIERARCHY_URI)
        .addDescendantsOf("diabetes",
            Snomed.SNOMED_CODE_SYSTEM_URI,
            "73211009",
            Snomed.SNOMED_HIERARCHY_URI)
        .addDescendantsOf("blood_disorder",
            Snomed.SNOMED_CODE_SYSTEM_URI,
            "266992002",
            Snomed.SNOMED_HIERARCHY_URI)
        .addDescendantsOf("disorder_history",
            Snomed.SNOMED_CODE_SYSTEM_URI,
            "312850006",
            Snomed.SNOMED_HIERARCHY_URI)
        .build(spark, mockValueSets, withLoincAndSnomed);

    ValueSetUdfs.pushUdf(spark, valueSets);

    Dataset<Observation> loincObservations = spark.createDataset(
        ImmutableList.of(
            observation("leukocytes", "5821-4"), // "is a" LP14419-3
            observation("bp", "8462-4")), // Blood pressure
        encoders.of(Observation.class));

    loincObservations.createOrReplaceTempView("test_loinc_obs");

    // Conditions include history of anemia, which includes a cycling ancestor
    // in our test data. This ensures that can be loaded correctly.
    Dataset<Condition> conditions = spark.createDataset(
        ImmutableList.of(
            // "is a" 73211009 (diabetes)
            condition("diabetes", "44054006"),
            // "is a" 266992002 (history of blood disorder) and
            // 312850006 (history of disorder)
            condition("history_of_anemia", "275538002")),
        encoders.of(Condition.class));

    conditions.createOrReplaceTempView("test_snomed_cond");

    Dataset<Encounter> encounters = spark.createDataset(
        ImmutableList.of(
            encounter("emergency", null, "EM"),
            encounter("routine", null, "R"),
            encounter("encounter", "99200", null),
            encounter("non_encounter", "99199", null)),
        encoders.of(Encounter.class));

    encounters.createOrReplaceTempView("test_valueset_encounter");
  }

  /**
   * Tears down Spark.
   */
  @AfterClass
  public static void tearDown() {
    spark.stop();
    spark = null;
  }

  @Test
  public void testLoincHasAncestor() {

    Dataset<Row> results = spark.sql("select id from test_loinc_obs "
        + "where in_valueset(code, 'leukocytes')");

    Assert.assertEquals(1, results.count());
    Assert.assertEquals("leukocytes", results.head().get(0));
  }

  @Test
  public void testSelfValue() {

    Dataset<Row> results = spark.sql("select id from test_loinc_obs "
        + "where in_valueset(code, 'bp')");

    Assert.assertEquals(1, results.count());
    Assert.assertEquals("bp", results.head().get(0));
  }

  @Test
  public void testNomatches() {

    Dataset<Row> results = spark.sql("select id from test_loinc_obs "
        + "where in_valueset(code, 'albumin')");

    Assert.assertEquals(0, results.count());
  }

  @Test
  public void testSnomedHasAncestor() {

    Dataset<Row> results = spark.sql("select id from test_snomed_cond "
        + "where in_valueset(code, 'diabetes')");

    Assert.assertEquals(1, results.count());
    Assert.assertEquals("diabetes", results.head().get(0));
  }

  @Test
  public void testHasCyclicAncestor() {

    Dataset<Row> results = spark.sql("select id from test_snomed_cond "
        + "where in_valueset(code, 'blood_disorder')");

    Assert.assertEquals(1, results.count());
    Assert.assertEquals("history_of_anemia", results.head().get(0));

    Dataset<Row> ancestorResults = spark.sql("select id from test_snomed_cond "
        + "where in_valueset(code, 'disorder_history')");

    Assert.assertEquals(1, ancestorResults.count());
    Assert.assertEquals("history_of_anemia", ancestorResults.head().get(0));
  }

  @Test
  public void testHasValueSetCode() {

    Dataset<Row> encounters = spark.sql("select id from test_valueset_encounter "
        + "where in_valueset(priority, 'priorities')");

    Assert.assertEquals(1, encounters.count());
    Assert.assertEquals("emergency", encounters.head().get(0));
  }

  @Test
  public void testArrayHasValueSetCode() {

    Dataset<Row> encounters = spark.sql("select id from test_valueset_encounter "
        + "where in_valueset(type, 'types')");

    Assert.assertEquals(1, encounters.count());
    Assert.assertEquals("encounter", encounters.head().get(0));
  }
}
