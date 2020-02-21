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

package au.csiro.pathling.encoders.r4;

import au.csiro.pathling.encoders.Bundles;
import au.csiro.pathling.encoders.Bundles.BundleContainer;
import au.csiro.pathling.encoders.FhirEncoders;
import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Condition;
import org.hl7.fhir.r4.model.MedicationRequest;
import org.hl7.fhir.r4.model.Patient;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Tests work working with bundles.
 */
public class BundlesTest {

  private static final FhirEncoders encoders = FhirEncoders.forR4().getOrCreate();
  private static SparkSession spark;
  private static Bundles bundles;
  private static JavaRDD<BundleContainer> bundlesRdd;

  /**
   * Sets up Spark.
   */
  @BeforeClass
  public static void setUp() throws IOException {
    spark = SparkSession.builder()
        .master("local[*]")
        .appName("BundlesTest")
        .config("spark.sql.warehouse.dir",
            Files.createTempDirectory("spark_warehouse").toString())
        .getOrCreate();

    bundles = Bundles.forR4();

    bundlesRdd = bundles.loadFromDirectory(spark,
        "src/test/resources/xml/bundles", 1).cache();
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
  public void testLoadBundleXml() {

    Assert.assertEquals(3,
        bundlesRdd.count());
  }

  @Test
  public void testLoadBundleJson() {

    JavaRDD<BundleContainer> bundles = BundlesTest.bundles.loadFromDirectory(spark,
        "src/test/resources/json/bundles", 1);

    Assert.assertEquals(3,
        bundles.count());
  }

  @Test
  public void testRetrieveBundle() {
    BundleContainer container = bundlesRdd.first();

    Bundle bundle = (Bundle) container.getBundle();

    Assert.assertNotNull(bundle);
    Assert.assertTrue(bundle.getEntry().size() > 0);
  }

  @Test
  public void getGetConditions() {

    Dataset<Condition> conditions = bundles.extractEntry(spark,
        bundlesRdd,
        Condition.class);

    Assert.assertEquals(5, conditions.count());
  }

  private void checkPatients(Dataset<Patient> patients) {
    List<String> patientIds = patients
        .collectAsList()
        .stream()
        .map(Patient::getId)
        .collect(Collectors.toList());

    Assert.assertEquals(3, patientIds.size());

    List<String> expectedIds = ImmutableList.of(
        "Patient/6666001",
        "Patient/1032702",
        "Patient/9995679");

    Assert.assertTrue(patientIds.containsAll(expectedIds));
  }

  private void checkConditions(Dataset<Condition> conditions) {
    List<String> conditionIds = conditions
        .collectAsList()
        .stream()
        .map(Condition::getId)
        .collect(Collectors.toList());

    Assert.assertEquals(5, conditionIds.size());

    List<String> expectedIds = ImmutableList.of(
        "Condition/119",
        "Condition/120",
        "Condition/121",
        "Condition/122",
        "Condition/123");

    Assert.assertTrue(conditionIds.containsAll(expectedIds));
  }

  private void checkContained(Dataset<MedicationRequest> medicationRequests) {
    List<String> medicationIds = medicationRequests
        .select("contained.medication.id")
        .where(functions.col("id").isNotNull())
        .as(Encoders.STRING())
        .collectAsList();

    Assert.assertEquals(2, medicationIds.size());

    List<String> expectedIds = ImmutableList.of(
        "#201",
        "#202");

    Assert.assertTrue(medicationIds.containsAll(expectedIds));
  }

  @Test
  public void testGetResourcesByClass() {

    Dataset<Patient> patients = bundles.extractEntry(spark, bundlesRdd, Patient.class);
    Dataset<Condition> conditions = bundles.extractEntry(spark, bundlesRdd, Condition.class);

    checkPatients(patients);
    checkConditions(conditions);
  }

  @Test
  public void testGetResourcesByName() {

    Dataset<Patient> patients = bundles.extractEntry(spark, bundlesRdd, "Patient");
    Dataset<Condition> conditions = bundles.extractEntry(spark, bundlesRdd, "Condition");

    checkPatients(patients);
    checkConditions(conditions);
  }

  @Test
  public void testGetCaseInsensitive() {

    Dataset<Patient> patients = bundles.extractEntry(spark, bundlesRdd, "patIENt");
    Dataset<Condition> conditions = bundles.extractEntry(spark, bundlesRdd, "conDiTion");

    checkPatients(patients);
    checkConditions(conditions);
  }

  @Test
  public void getContained() {

    Dataset<MedicationRequest> medicationRequests = bundles.extractEntry(spark,
        bundlesRdd,
        "MedicationRequest",
        "Medication");

    checkContained(medicationRequests);
  }

  @Test
  public void testXmlBundleStrings() {

    JavaRDD<String> xmlBundlesRdd = spark.sparkContext()
        .wholeTextFiles("src/test/resources/xml/bundles", 1)
        .toJavaRDD()
        .map(tuple -> tuple._2());

    Dataset<String> xmlBundles = spark.createDataset(xmlBundlesRdd.rdd(),
        Encoders.STRING());

    xmlBundles.write().saveAsTable("xml_bundle_table");

    JavaRDD<BundleContainer> bundles = BundlesTest.bundles.fromXml(
        spark.sql("select value from xml_bundle_table"), "value");

    Dataset<Patient> patients = BundlesTest.bundles.extractEntry(spark,
        bundles,
        Patient.class);

    checkPatients(patients);
  }

  @Test
  public void testJsonBundleStrings() {

    JavaRDD<String> jsonBundlesRdd = spark.sparkContext()
        .wholeTextFiles("src/test/resources/json/bundles", 1)
        .toJavaRDD()
        .map(tuple -> tuple._2());

    Dataset<String> jsonBundles = spark.createDataset(jsonBundlesRdd.rdd(),
        Encoders.STRING());

    jsonBundles.write().saveAsTable("json_bundle_table");

    JavaRDD<BundleContainer> bundlesRdd = bundles.fromJson(
        spark.sql("select value from json_bundle_table"), "value");

    Dataset<Patient> patients = BundlesTest.bundles.extractEntry(spark,
        bundlesRdd,
        Patient.class);

    checkPatients(patients);
  }


  @Test
  public void testSaveAsDatabase() {

    bundles.saveAsDatabase(spark,
        bundlesRdd,
        "bundlesdb",
        "patient", "condition", "observation");

    Dataset<Patient> patients = spark
        .sql("select * from bundlesdb.patient")
        .as(encoders.of(Patient.class));

    checkPatients(patients);

    Dataset<Condition> conditions = spark
        .sql("select * from bundlesdb.condition")
        .as(encoders.of(Condition.class));

    checkConditions(conditions);

    // Ensure the included observations are present as well.
    Assert.assertEquals(72,
        spark.sql("select * from bundlesdb.observation")
            .count());
  }
}
