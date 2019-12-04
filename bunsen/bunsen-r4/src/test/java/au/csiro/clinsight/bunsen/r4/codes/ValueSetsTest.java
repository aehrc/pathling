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

package au.csiro.clinsight.bunsen.r4.codes;

import au.csiro.clinsight.bunsen.codes.Value;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.nio.file.Files;
import java.text.MessageFormat;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.ValueSet;
import org.hl7.fhir.r4.model.ValueSet.ConceptSetComponent;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit test for storing, loading, and manipulating ValueSets.
 */
public class ValueSetsTest {

  private static SparkSession spark;

  /**
   * Sets up Spark.
   */
  @BeforeClass
  public static void setUp() throws IOException {

    // Create a local spark session using an in-memory metastore.
    // We must also use Hive and set the partition mode to non-strict to
    // support dynamic partitions.
    spark = SparkSession.builder()
        .master("local[2]")
        .appName("ConceptMapsTest")
        .enableHiveSupport()
        .config("javax.jdo.option.ConnectionURL",
            "jdbc:derby:memory:metastore_db;create=true")
        .config("hive.exec.dynamic.partition.mode",
            "nonstrict")
        .config("spark.sql.warehouse.dir",
            Files.createTempDirectory("spark_warehouse").toString())
        .getOrCreate();

    spark.sql("create database mappingtestdb");
  }

  /**
   * Tears down Spark.
   */
  @AfterClass
  public static void tearDown() {
    spark.stop();
    spark = null;
  }

  private static ValueSet valueSet(String valueSetUrl, String valueSetVersion) {

    return valueSet(valueSetUrl, valueSetVersion, "a");
  }

  private static ValueSet valueSet(String valueSetUrl, String valueSetVersion, String... codes) {

    ValueSet valueSet = new ValueSet();

    valueSet.setUrl(valueSetUrl);
    valueSet.setVersion(valueSetVersion);
    valueSet.setExperimental(true);

    ConceptSetComponent inclusion = valueSet.getCompose().addInclude();

    inclusion.setSystem("urn:cerner:system").setVersion("1");

    for (String code : codes) {

      inclusion.addConcept().setCode(code);
    }

    return valueSet;
  }

  private static void checkValueSet(ValueSet valueSet, String url, String version) {

    Assert.assertNotNull(
        MessageFormat.format("Could not find value set for url {0} and version {1}", url, version),
        valueSet);

    Assert.assertEquals(url, valueSet.getUrl());
    Assert.assertEquals(version, valueSet.getVersion());

    ConceptSetComponent inclusion = valueSet.getCompose().getIncludeFirstRep();

    Assert.assertEquals("urn:cerner:system", inclusion.getSystem());
    Assert.assertEquals("1", inclusion.getVersion());
    Assert.assertEquals("a", inclusion.getConceptFirstRep().getCode());

    Assert.assertEquals(1, valueSet.getCompose().getInclude().size());
  }

  @Test
  public void testCreateSimpleValueSets() {
    ValueSets valueSets = ValueSets.getEmpty(spark)
        .withValueSets(valueSet("urn:cerner:valueset:valueset1", "1"),
            valueSet("urn:cerner:valueset:valueset2", "1"));

    Dataset<Value> values = valueSets.getValues();

    Assert.assertEquals(2, values.count());

    ValueSet firstValueSet = valueSets.getValueSet("urn:cerner:valueset:valueset1", "1");
    checkValueSet(firstValueSet, "urn:cerner:valueset:valueset1", "1");

    ValueSet secondValueSet = valueSets.getValueSet("urn:cerner:valueset:valueset2", "1");
    checkValueSet(secondValueSet, "urn:cerner:valueset:valueset2", "1");
  }

  @Test
  public void testAppendValueSets() {
    ValueSets original = ValueSets.getEmpty(spark)
        .withValueSets(valueSet("urn:cerner:valueset:valueset1", "1"),
            valueSet("urn:cerner:valueset:valueset2", "1"));

    ValueSets valueSets = original.withValueSets(valueSet("urn:cerner:valueset:valueset3", "1"));

    Assert.assertEquals(2, original.getValues().count());
    Assert.assertEquals(3, valueSets.getValues().count());

    ValueSet firstValueSet = valueSets.getValueSet("urn:cerner:valueset:valueset1", "1");
    checkValueSet(firstValueSet, "urn:cerner:valueset:valueset1", "1");

    ValueSet secondValueSet = valueSets.getValueSet("urn:cerner:valueset:valueset2", "1");
    checkValueSet(secondValueSet, "urn:cerner:valueset:valueset2", "1");

    ValueSet newValueSet = valueSets.getValueSet("urn:cerner:valueset:valueset3", "1");
    checkValueSet(newValueSet, "urn:cerner:valueset:valueset3", "1");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testIncludingDuplicateValueSetsThrowsException() {

    ValueSets.getEmpty(spark)
        .withValueSets(valueSet("urn:cerner:valueset:valueset", "1"),
            valueSet("urn:cerner:valueset:valueset", "1"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAddingDuplicateValueSetsThrowsException() {

    ValueSets valueSets = ValueSets.getEmpty(spark)
        .withValueSets(valueSet("urn:cerner:valueset:valueset", "1"));

    valueSets.withValueSets(valueSet("urn:cerner:valueset:valueset", "1"));
  }

  @Test
  public void testWithValueSetsFromDirectoryXml() {

    ValueSets valueSets = ValueSets.getEmpty(spark)
        .withValueSetsFromDirectory("src/test/resources/xml/valuesets");

    ValueSet marriedValueSet = valueSets.getValueSet(
        "urn:cerner:bunsen:valueset:married_maritalstatus",
        "0.0.1");

    Assert.assertNotNull(marriedValueSet);
    Assert.assertEquals("urn:cerner:bunsen:valueset:married_maritalstatus",
        marriedValueSet.getUrl());
    Assert.assertEquals("0.0.1", marriedValueSet.getVersion());
  }

  @Test
  public void testWithValueSetsFromDirectoryJson() {

    ValueSets valueSets = ValueSets.getEmpty(spark)
        .withValueSetsFromDirectory("src/test/resources/json/valuesets");

    ValueSet marriedValueSet = valueSets.getValueSet(
        "urn:cerner:bunsen:valueset:married_maritalstatus",
        "0.0.1");

    Assert.assertNotNull(marriedValueSet);
    Assert.assertEquals("urn:cerner:bunsen:valueset:married_maritalstatus",
        marriedValueSet.getUrl());
    Assert.assertEquals("0.0.1", marriedValueSet.getVersion());
  }

  @Test
  public void testWithDisjointValueSetsFromDirectory() {

    String database = "test_valuesets_disjoint";
    spark.sql("CREATE DATABASE " + database);

    ValueSets.getEmpty(spark)
        .withValueSetsFromDirectory("src/test/resources/xml/valuesets")
        .writeToDatabase(database);

    ValueSets valueSets = ValueSets.getFromDatabase(spark, database)
        .withDisjointValueSetsFromDirectory("src/test/resources/xml/valuesets", database);

    ValueSet marriedValueSet = valueSets.getValueSet(
        "urn:cerner:bunsen:valueset:married_maritalstatus",
        "0.0.1");

    Assert.assertEquals(1, valueSets.getValueSets().count());

    Assert.assertNotNull(marriedValueSet);
    Assert.assertEquals("urn:cerner:bunsen:valueset:married_maritalstatus",
        marriedValueSet.getUrl());
    Assert.assertEquals("0.0.1", marriedValueSet.getVersion());

  }

  @Test
  public void testWriteToNewTables() {

    String database = "test_valuesets_write";
    spark.sql("CREATE DATABASE " + database);

    ValueSets valueSets = ValueSets.getEmpty(spark)
        .withValueSetsFromDirectory("src/test/resources/xml/valuesets");

    valueSets.writeToDatabase(database);

    ValueSets reloadedValueSets = ValueSets.getFromDatabase(spark, database);

    ValueSet marriedValueSet = reloadedValueSets
        .getValueSet("urn:cerner:bunsen:valueset:married_maritalstatus", "0.0.1");

    Assert.assertNotNull(marriedValueSet);
    Assert.assertEquals("urn:cerner:bunsen:valueset:married_maritalstatus",
        marriedValueSet.getUrl());
    Assert.assertEquals("0.0.1", marriedValueSet.getVersion());
    Assert.assertEquals(1, marriedValueSet.getCompose().getInclude().size());
  }

  @Test
  public void testValueSetsIncludesNoConcepts() {

    ValueSets valueSets = ValueSets.getEmpty(spark)
        .withValueSets(valueSet("urn:cerner:valueset:valueset1", "1"),
            valueSet("urn:cerner:valueset:valueset2", "1"));

    valueSets.getValueSets()
        .collectAsList()
        .forEach(valueSet -> valueSet.getCompose()
            .getInclude()
            .forEach(inclusion -> Assert.assertTrue(inclusion.getConcept().isEmpty())));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWritingDuplicateValueSetsThrowsException() {

    String database = "duplicate_valuesets_write";
    spark.sql("CREATE DATABASE " + database);

    ValueSets valueSets = ValueSets.getEmpty(spark)
        .withValueSetsFromDirectory("src/test/resources/xml/valuesets");

    valueSets.writeToDatabase(database);

    ValueSets reloadedValueSets = ValueSets.getFromDatabase(spark, database);

    reloadedValueSets.writeToDatabase(database);
  }

  @Test
  public void testGetLatest() {

    String database = "test_get_latest";
    spark.sql("CREATE DATABASE " + database);

    ValueSets.getEmpty(spark)
        .withValueSets(
            valueSet("urn:cerner:valueset:newvalueset", "1"),
            valueSet("urn:cerner:valueset:newvalueset", "2"),
            valueSet("urn:cerner:valueset:othervalueset", "1"))
        .writeToDatabase(database);

    Dataset<Value> latest = ValueSets.getFromDatabase(spark, database)
        .getLatestValues(ImmutableSet.of("urn:cerner:valueset:newvalueset",
            "urn:cerner:valueset:othervalueset"),
            true);

    latest.cache();

    Assert.assertEquals(2, latest.count());

    Assert.assertEquals(0, latest.where(
        "valueSetUri == 'urn:cerner:valueset:newvalueset' AND valueSetVersion == '1'")
        .count());

    Assert.assertEquals(1, latest.where(
        "valueSetUri == 'urn:cerner:valueset:newvalueset' AND valueSetVersion == '2'")
        .count());

    Assert.assertEquals(1, latest.where(
        "valueSetUri == 'urn:cerner:valueset:othervalueset' AND valueSetVersion == '1'")
        .count());
  }

  @Test
  public void testGetLatestExperimental() {

    String database = "test_get_latest_experimental";
    spark.sql("CREATE DATABASE " + database);

    ValueSets.getEmpty(spark)
        .withValueSets(
            valueSet("urn:cerner:valueset:expvalueset", "1").setExperimental(false),
            valueSet("urn:cerner:valueset:expvalueset", "2"),
            valueSet("urn:cerner:valueset:otherexpvalueset", "1"))
        .writeToDatabase(database);

    ValueSets valueSets = ValueSets.getFromDatabase(spark, database);

    Dataset<Value> latestWithExperimental = valueSets.getLatestValues(
        ImmutableSet.of("urn:cerner:valueset:expvalueset"),
        true);

    // We include experimental versions, so we should see that.
    Assert.assertEquals(1,
        latestWithExperimental
            .where("valueSetUri == 'urn:cerner:valueset:expvalueset' and valueSetVersion == '2'")
            .count());

    Dataset<Value> latestWithoutExperimental = valueSets.getLatestValues(
        ImmutableSet.of("urn:cerner:valueset:expvalueset"),
        false);

    // Version 1 is not experimental, so we should see it.
    Assert.assertEquals(1,
        latestWithoutExperimental
            .where("valueSetUri == 'urn:cerner:valueset:expvalueset' and valueSetVersion == '1'")
            .count());

    // Loading a map with only experimental versions should find nothing.
    Dataset<Value> onlyExperimentalValueSets = valueSets.getLatestValues(
        ImmutableSet.of("urn:cerner:valueset:otherexpvalueset"),
        false);

    Assert.assertEquals(0, onlyExperimentalValueSets.count());
  }

  @Test
  public void testExpandValues() {

    ValueSet valueSet = ValueSets.getEmpty(spark)
        .withValueSets(valueSet("urn:cerner:valueset:valueset", "1"))
        .getValueSet("urn:cerner:valueset:valueset", "1");

    List<Value> values = ValueSets.expandValues(valueSet);

    Value expectedValue = new Value("urn:cerner:valueset:valueset",
        "1",
        "urn:cerner:system",
        "1",
        "a");

    Assert.assertEquals(1, values.size());
    Assert.assertEquals(expectedValue, values.get(0));
  }
}
