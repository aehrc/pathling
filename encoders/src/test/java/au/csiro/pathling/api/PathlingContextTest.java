/*
 * This is a modified version of the Bunsen library, originally published at
 * https://github.com/cerner/bunsen.
 *
 * Bunsen is copyright 2017 Cerner Innovation, Inc., and is licensed under
 * the Apache License, version 2.0 (http://www.apache.org/licenses/LICENSE-2.0).
 *
 * These modifications are copyright © 2018-2022, Commonwealth Scientific
 * and Industrial Research Organisation (CSIRO) ABN 41 687 119 230. Licensed
 * under the CSIRO Open Source Software Licence Agreement.
 *
 */

package au.csiro.pathling.api;

import static au.csiro.pathling.encoders.SchemaAsserts.assertFieldNotPresent;
import static au.csiro.pathling.encoders.SchemaAsserts.assertFieldPresent;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.hl7.fhir.r4.model.Condition;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import scala.collection.mutable.WrappedArray;

public class PathlingContextTest {

  private static SparkSession spark;

  /**
   * Set up Spark.
   */
  @BeforeClass
  public static void setUp() {
    spark = SparkSession.builder()
        .master("local[*]")
        .appName("testing")
        .config("spark.driver.bindAddress", "localhost")
        .config("spark.driver.host", "localhost")
        .getOrCreate();
  }

  /**
   * Tear down Spark.
   */
  @AfterClass
  public static void tearDown() {
    spark.stop();
  }


  @Test
  public void testEncodeResourcesFromJsonBundle() {

    final Dataset<String> bundlesDF = spark.read().option("wholetext", true)
        .textFile("src/test/resources/data/bundles/R4/json");

    final PathlingContext pathling = PathlingContext.of(spark);

    final Dataset<Row> patientsDataframe = pathling.encodeBundle(bundlesDF.toDF(),
        "Patient", FhirMimeTypes.FHIR_JSON);
    assertEquals(5, patientsDataframe.count());

    final Dataset<Condition> conditionsDataframe = pathling.encodeBundle(bundlesDF, Condition.class,
        FhirMimeTypes.FHIR_JSON);
    assertEquals(107, conditionsDataframe.count());
  }


  @Test
  public void testEncodeResourcesFromXmlBundle() {

    final Dataset<String> bundlesDF = spark.read().option("wholetext", true)
        .textFile("src/test/resources/data/bundles/R4/xml");

    final PathlingContext pathling = PathlingContext.of(spark);
    final Dataset<Condition> conditionsDataframe = pathling.encodeBundle(bundlesDF, Condition.class,
        FhirMimeTypes.FHIR_XML);
    assertEquals(107, conditionsDataframe.count());
  }


  @Test
  public void testEncodeResourcesFromJson() {
    final Dataset<String> jsonResources = spark.read()
        .textFile("src/test/resources/data/resources/R4/json");

    final PathlingContext pathling = PathlingContext.of(spark);

    final Dataset<Row> patientsDataframe = pathling.encode(jsonResources.toDF(), "Patient",
        FhirMimeTypes.FHIR_JSON);
    assertEquals(9, patientsDataframe.count());

    final Dataset<Condition> conditionsDataframe = pathling.encode(jsonResources, Condition.class,
        FhirMimeTypes.FHIR_JSON);
    assertEquals(71, conditionsDataframe.count());
  }

  @Test
  public void testEncoderOptions() {
    final Dataset<Row> jsonResourcesDF = spark.read()
        .text("src/test/resources/data/resources/R4/json");

    // Test the defaults
    final Row defaultRow = PathlingContext.of(spark).encode(jsonResourcesDF, "Questionnaire")
        .head();
    assertFieldNotPresent("_extension", defaultRow.schema());
    final Row defaultItem = (Row) defaultRow.getList(defaultRow.fieldIndex("item")).get(0);
    assertFieldNotPresent("item", defaultItem.schema());

    // Test explicit options
    // Nested items
    final Row rowWithNesting = PathlingContext
        .of(spark, null, 1, null, null)
        .encode(jsonResourcesDF, "Questionnaire").head();
    assertFieldNotPresent("_extension", rowWithNesting.schema());
    // Test item nesting
    final Row itemWithNesting = (Row) rowWithNesting
        .getList(rowWithNesting.fieldIndex("item")).get(0);
    assertFieldPresent("item", itemWithNesting.schema());
    final Row nestedItem = (Row) itemWithNesting
        .getList(itemWithNesting.fieldIndex("item")).get(0);
    assertFieldNotPresent("item", nestedItem.schema());

    // Test explicit options
    // Extensions and open types
    final Row rowWithExtensions = PathlingContext
        .of(spark, null, null, true,
            Arrays.asList("boolean", "string", "Address"))
        .encode(jsonResourcesDF, "Patient").head();
    assertFieldPresent("_extension", rowWithExtensions.schema());

    final Map<Integer, WrappedArray<Row>> extensions = rowWithExtensions
        .getJavaMap(rowWithExtensions.fieldIndex("_extension"));

    // get the first extension of some extensio set
    final Row extension = (Row) extensions.values().toArray(WrappedArray[]::new)[0].apply(0);
    assertFieldPresent("valueString", extension.schema());
    assertFieldPresent("valueAddress", extension.schema());
    assertFieldPresent("valueBoolean", extension.schema());
    assertFieldNotPresent("valueInteger", extension.schema());
  }


  @Test
  public void testEncodeResourceStream() throws Exception {
    final PathlingContext pathling = PathlingContext.of(spark);

    final Dataset<Row> jsonResources = spark.readStream()
        .text("src/test/resources/data/resources/R4/json");

    assertTrue(jsonResources.isStreaming());

    final Dataset<Row> patientsStream = pathling.encode(jsonResources, "Patient",
        FhirMimeTypes.FHIR_JSON);

    assertTrue(patientsStream.isStreaming());

    final StreamingQuery patientsQuery = patientsStream
        .writeStream()
        .queryName("patients")
        .format("memory")
        .start();

    patientsQuery.processAllAvailable();
    final long patientsCount = spark.sql("select count(*) from patients").head().getLong(0);
    assertEquals(patientsCount, patientsCount);

    final StreamingQuery conditionQuery = pathling.encode(jsonResources, "Condition",
            FhirMimeTypes.FHIR_JSON)
        .groupBy()
        .count()
        .writeStream()
        .outputMode(OutputMode.Complete())
        .queryName("countCondition")
        .format("memory")
        .start();

    conditionQuery.processAllAvailable();
    final long conditionsCount = spark.sql("select * from countCondition").head().getLong(0);
    assertEquals(71, conditionsCount);
  }
}