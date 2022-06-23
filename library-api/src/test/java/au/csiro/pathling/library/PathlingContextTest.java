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

package au.csiro.pathling.library;

import static org.apache.spark.sql.functions.col;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.mockito.Mockito.withSettings;

import au.csiro.pathling.encoders.FhirEncoders;
import au.csiro.pathling.fhir.TerminologyServiceFactory;
import au.csiro.pathling.fhirpath.encoding.CodingEncoding;
import au.csiro.pathling.fhirpath.encoding.SimpleCoding;
import au.csiro.pathling.terminology.ConceptTranslator;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.test.SchemaAsserts;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.streaming.OutputMode;
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.hl7.fhir.r4.model.Condition;
import org.hl7.fhir.r4.model.Enumerations.ConceptMapEquivalence;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import scala.collection.mutable.WrappedArray;

public class PathlingContextTest {

  private static SparkSession spark;
  private static final String testDataUrl = "target/encoders-tests/data";

  /**
   * Set up Spark.
   */
  @BeforeAll
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
  @AfterAll
  public static void tearDown() {
    spark.stop();
  }


  @Test
  public void testEncodeResourcesFromJsonBundle() {

    final Dataset<String> bundlesDF = spark.read().option("wholetext", true)
        .textFile(testDataUrl + "/bundles/R4/json");

    final PathlingContext pathling = PathlingContext.create(spark, null, null, true, null, null);

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
        .textFile(testDataUrl + "/bundles/R4/xml");

    final PathlingContext pathling = PathlingContext.create(spark, null, null, true, null, null);
    final Dataset<Condition> conditionsDataframe = pathling.encodeBundle(bundlesDF, Condition.class,
        FhirMimeTypes.FHIR_XML);
    assertEquals(107, conditionsDataframe.count());
  }


  @Test
  public void testEncodeResourcesFromJson() {
    final Dataset<String> jsonResources = spark.read()
        .textFile(testDataUrl + "/resources/R4/json");

    final PathlingContext pathling = PathlingContext.create(spark, null, null, true, null, null);

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
        .text(testDataUrl + "/resources/R4/json");

    // Test the defaults
    final Row defaultRow = PathlingContext.create(spark, null, null, null, null, null)
        .encode(jsonResourcesDF, "Questionnaire")
        .head();
    SchemaAsserts.assertFieldNotPresent("_extension", defaultRow.schema());
    final Row defaultItem = (Row) defaultRow.getList(defaultRow.fieldIndex("item")).get(0);
    SchemaAsserts.assertFieldNotPresent("item", defaultItem.schema());

    // Test explicit options
    // Nested items
    final Row rowWithNesting = PathlingContext
        .create(spark, null, 1, null, null, null)
        .encode(jsonResourcesDF, "Questionnaire").head();
    SchemaAsserts.assertFieldNotPresent("_extension", rowWithNesting.schema());
    // Test item nesting
    final Row itemWithNesting = (Row) rowWithNesting
        .getList(rowWithNesting.fieldIndex("item")).get(0);
    SchemaAsserts.assertFieldPresent("item", itemWithNesting.schema());
    final Row nestedItem = (Row) itemWithNesting
        .getList(itemWithNesting.fieldIndex("item")).get(0);
    SchemaAsserts.assertFieldNotPresent("item", nestedItem.schema());

    // Test explicit options
    // Extensions and open types
    final Row rowWithExtensions = PathlingContext
        .create(spark, null, null, true,
            Arrays.asList("boolean", "string", "Address"), null)
        .encode(jsonResourcesDF, "Patient").head();
    SchemaAsserts.assertFieldPresent("_extension", rowWithExtensions.schema());

    final Map<Integer, WrappedArray<Row>> extensions = rowWithExtensions
        .getJavaMap(rowWithExtensions.fieldIndex("_extension"));

    // get the first extension of some extension set
    final Row extension = (Row) extensions.values().toArray(WrappedArray[]::new)[0].apply(0);
    SchemaAsserts.assertFieldPresent("valueString", extension.schema());
    SchemaAsserts.assertFieldPresent("valueAddress", extension.schema());
    SchemaAsserts.assertFieldPresent("valueBoolean", extension.schema());
    SchemaAsserts.assertFieldNotPresent("valueInteger", extension.schema());
  }

  @Test
  public void testEncodeResourceStream() throws Exception {
    final PathlingContext pathling = PathlingContext.create(spark, null, null, true, null, null);

    final Dataset<Row> jsonResources = spark.readStream().text(testDataUrl + "/resources/R4/json");

    Assertions.assertTrue(jsonResources.isStreaming());

    final Dataset<Row> patientsStream = pathling.encode(jsonResources, "Patient",
        FhirMimeTypes.FHIR_JSON);

    Assertions.assertTrue(patientsStream.isStreaming());

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

  @Test
  void testMemberOf() {
    final String valueSetUri = "urn:test:456";
    final SimpleCoding coding1 = new SimpleCoding("urn:test:123", "ABC");
    final SimpleCoding coding2 = new SimpleCoding("urn:test:123", "DEF");

    final TerminologyServiceFactory terminologyServiceFactory = mock(
        TerminologyServiceFactory.class, withSettings().serializable());
    final TerminologyService terminologyService = mock(TerminologyService.class,
        withSettings().serializable());
    when(terminologyServiceFactory.buildService(any())).thenReturn(terminologyService);
    when(terminologyService.intersect(eq(valueSetUri), any()))
        .thenReturn(new HashSet<>(List.of(coding1)));

    final PathlingContext pathlingContext = PathlingContext.create(spark,
        FhirEncoders.forR4().getOrCreate(), terminologyServiceFactory);

    final Row row1 = RowFactory.create("foo", CodingEncoding.encode(coding1.toCoding()));
    final Row row2 = RowFactory.create("bar", CodingEncoding.encode(coding2.toCoding()));
    final List<Row> datasetRows = List.of(row1, row2);
    final StructType schema = DataTypes.createStructType(
        new StructField[]{DataTypes.createStructField("id", DataTypes.StringType, true),
            DataTypes.createStructField("coding", CodingEncoding.codingStructType(), true)});
    final Dataset<Row> codingDataFrame = spark.createDataFrame(datasetRows, schema);
    final Column codingColumn = col("coding");
    final Dataset<Row> result = pathlingContext.memberOf(codingDataFrame, codingColumn,
        valueSetUri, "result");

    final List<Row> rows = result.select("id", "result").collectAsList();
    assertEquals(RowFactory.create("foo", true), rows.get(0));
    assertEquals(RowFactory.create("bar", false), rows.get(1));
  }

  @Test
  void testTranslate() {
    final String conceptMapUri = "urn:test:456";
    final List<ConceptMapEquivalence> equivalences = List.of(ConceptMapEquivalence.EQUIVALENT);
    final SimpleCoding coding1 = new SimpleCoding("urn:test:123", "ABC");
    final SimpleCoding coding2 = new SimpleCoding("urn:test:123", "DEF");

    final TerminologyServiceFactory terminologyServiceFactory = mock(
        TerminologyServiceFactory.class, withSettings().serializable());
    final TerminologyService terminologyService = mock(TerminologyService.class,
        withSettings().serializable());
    final ConceptTranslator conceptTranslator = mock(ConceptTranslator.class,
        withSettings().serializable());
    when(terminologyServiceFactory.buildService(any())).thenReturn(terminologyService);
    when(terminologyService.translate(any(), eq(conceptMapUri), eq(false), eq(equivalences)))
        .thenReturn(conceptTranslator);
    when(conceptTranslator.translate(any())).thenReturn(List.of(coding2.toCoding()));

    final PathlingContext pathlingContext = PathlingContext.create(spark,
        FhirEncoders.forR4().getOrCreate(), terminologyServiceFactory);

    final Row row1 = RowFactory.create("foo", CodingEncoding.encode(coding1.toCoding()));
    final Row row2 = RowFactory.create("bar", CodingEncoding.encode(coding2.toCoding()));
    final List<Row> datasetRows = List.of(row1, row2);
    final StructType schema = DataTypes.createStructType(
        new StructField[]{DataTypes.createStructField("id", DataTypes.StringType, true),
            DataTypes.createStructField("coding", CodingEncoding.codingStructType(), true)});
    final Dataset<Row> codingDataFrame = spark.createDataFrame(datasetRows, schema);
    final Column codingColumn = col("coding");
    final Dataset<Row> result = pathlingContext.translate(codingDataFrame, codingColumn,
        conceptMapUri, false, ConceptMapEquivalence.EQUIVALENT.toCode(), "result");

    final List<Row> rows = result.select("id", "result").collectAsList();
    assertEquals(RowFactory.create("foo", CodingEncoding.encode(coding2.toCoding())), rows.get(0));
  }

}
