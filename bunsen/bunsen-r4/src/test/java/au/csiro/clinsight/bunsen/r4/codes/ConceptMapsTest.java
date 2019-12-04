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

import au.csiro.clinsight.bunsen.codes.Mapping;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.exceptions.FHIRException;
import org.hl7.fhir.r4.model.ConceptMap;
import org.hl7.fhir.r4.model.UriType;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit test for storing, loading, and manipulating ConceptMaps.
 */
public class ConceptMapsTest {

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


  private static final ConceptMap conceptMap(String url, String version) {
    ConceptMap conceptMap = new ConceptMap();

    conceptMap.setUrl(url);
    conceptMap.setVersion(version);
    conceptMap.setExperimental(true);
    conceptMap.setSource(new UriType("urn:source:valueset"));
    conceptMap.setTarget(new UriType("urn:target:valueset"));
    conceptMap.addGroup()
        .setSource("urn:source:system")
        .setTarget("urn:target:system")
        .addElement()
        .setCode("urn:source:code:a")
        .addTarget().setCode("urn:target:code:1");

    return conceptMap;
  }

  private static void checkMap(ConceptMap map, String url, String version) throws FHIRException {

    Assert.assertNotNull("Could not find concept map + " + url, map);

    Assert.assertEquals(url, map.getUrl());
    Assert.assertEquals(version, map.getVersion());

    Assert.assertEquals("urn:source:valueset", map.getSourceUriType().asStringValue());
    Assert.assertEquals("urn:target:valueset", map.getTargetUriType().asStringValue());

    Assert.assertEquals(1, map.getGroup().size());
  }

  @Test
  public void testCreateSimpleMappings() throws FHIRException {

    ConceptMaps maps = ConceptMaps.getEmpty(spark)
        .withConceptMaps(conceptMap("urn:cerner:map:testmap", "1"),
            conceptMap("urn:cerner:map:othermap", "1"));

    Dataset<Mapping> mappings = maps.getMappings();

    Assert.assertEquals(2, mappings.count());

    ConceptMap firstMap = maps.getConceptMap("urn:cerner:map:testmap", "1");
    checkMap(firstMap, "urn:cerner:map:testmap", "1");

    ConceptMap secondMap = maps.getConceptMap("urn:cerner:map:othermap", "1");
    checkMap(secondMap, "urn:cerner:map:othermap", "1");
  }

  @Test
  public void testLoadExpandedMappings() throws FHIRException {

    ConceptMap map = conceptMap("urn:cerner:map:testmap", "1");

    // Explicitly create a mapping dataset to simulate an ETL load from an external source.
    Mapping mapping = new Mapping();

    mapping.setConceptMapUri(map.getUrl());
    mapping.setConceptMapVersion(map.getVersion());
    mapping.setSourceValueSet("urn:source:valueset");
    mapping.setTargetValue("urn:target:valueset");
    mapping.setSourceSystem("urn:source:system");
    mapping.setSourceValue("urn:source:code:a");
    mapping.setTargetSystem("urn:target:system");
    mapping.setTargetValue("urn:target:code:1");

    Dataset<Mapping> mappings = spark.createDataset(Arrays.asList(mapping),
        ConceptMaps.getMappingEncoder());

    ConceptMaps maps = ConceptMaps.getEmpty(spark)
        .withExpandedMap(map, mappings);

    Dataset<Mapping> loadedMappings = maps.getMappings();

    Assert.assertEquals(1, loadedMappings.count());

    Mapping loadedMapping = loadedMappings.head();

    Assert.assertEquals(mapping, loadedMapping);
  }

  @Test
  public void testAppendMappings() throws FHIRException {

    ConceptMaps original = ConceptMaps.getEmpty(spark)
        .withConceptMaps(conceptMap("urn:cerner:map:testmap", "1"),
            conceptMap("urn:cerner:map:othermap", "1"));

    ConceptMaps maps = original.withConceptMaps(
        conceptMap("urn:cerner:map:newmap", "1"));

    // The original should be unchanged.
    Assert.assertEquals(2, original.getMappings().count());
    Assert.assertEquals(3, maps.getMappings().count());

    ConceptMap firstMap = maps.getConceptMap("urn:cerner:map:testmap", "1");
    checkMap(firstMap, "urn:cerner:map:testmap", "1");

    ConceptMap secondMap = maps.getConceptMap("urn:cerner:map:othermap", "1");
    checkMap(secondMap, "urn:cerner:map:othermap", "1");

    ConceptMap newMap = maps.getConceptMap("urn:cerner:map:newmap", "1");
    checkMap(newMap, "urn:cerner:map:newmap", "1");
  }

  @Test(expected = IllegalArgumentException.class)
  public void testIncludingDuplicateConceptMapThrowsException() {

    ConceptMaps.getEmpty(spark)
        .withConceptMaps(conceptMap("urn:cerner:map:testmap", "1"),
            conceptMap("urn:cerner:map:testmap", "1"));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAddingDuplicateConceptMapsThrowsException() throws FHIRException {

    ConceptMaps maps = ConceptMaps.getEmpty(spark)
        .withConceptMaps(conceptMap("urn:cerner:map:testmap", "1"));

    maps.withConceptMaps(conceptMap("urn:cerner:map:testmap", "1"));
  }

  @Test
  public void testWithMapsFromDirectoryXml() {

    ConceptMaps maps = ConceptMaps.getEmpty(spark)
        .withMapsFromDirectory("src/test/resources/xml/conceptmaps");

    ConceptMap genderMap = maps.getConceptMap(
        "urn:cerner:poprec:fhir:conceptmap:demographics:gender",
        "0.0.1");

    Assert.assertNotNull(genderMap);

    Assert.assertEquals("urn:cerner:poprec:fhir:conceptmap:demographics:gender",
        genderMap.getUrl());

    Assert.assertEquals("0.0.1", genderMap.getVersion());
  }

  @Test
  public void testWithMapsFromDirectoryJson() {

    ConceptMaps maps = ConceptMaps.getEmpty(spark)
        .withMapsFromDirectory("src/test/resources/json/conceptmaps");

    ConceptMap genderMap = maps.getConceptMap(
        "urn:cerner:poprec:fhir:conceptmap:demographics:gender",
        "0.0.1");

    Assert.assertNotNull(genderMap);

    Assert.assertEquals("urn:cerner:poprec:fhir:conceptmap:demographics:gender",
        genderMap.getUrl());

    Assert.assertEquals("0.0.1", genderMap.getVersion());
  }

  @Test
  public void testWithDisjointMapsFromDirectory() {

    String database = "test_conceptmaps_disjoint";
    spark.sql("CREATE DATABASE " + database);

    ConceptMaps.getEmpty(spark)
        .withMapsFromDirectory("src/test/resources/xml/conceptmaps")
        .writeToDatabase(database);

    ConceptMaps maps = ConceptMaps.getFromDatabase(spark, database)
        .withDisjointMapsFromDirectory("src/test/resources/xml/conceptmaps", database);

    ConceptMap genderMap = maps.getConceptMap(
        "urn:cerner:poprec:fhir:conceptmap:demographics:gender",
        "0.0.1");

    Assert.assertEquals(1, maps.getMaps().count());

    Assert.assertNotNull(genderMap);

    Assert.assertEquals("urn:cerner:poprec:fhir:conceptmap:demographics:gender",
        genderMap.getUrl());

    Assert.assertEquals("0.0.1", genderMap.getVersion());
  }

  @Test
  public void testWriteToNewTables() {

    spark.sql("create database test_mapping_write");

    ConceptMaps maps = ConceptMaps.getEmpty(spark)
        .withMapsFromDirectory("src/test/resources/xml/conceptmaps");

    maps.writeToDatabase("test_mapping_write");

    ConceptMaps reloadedMaps = ConceptMaps.getFromDatabase(spark, "test_mapping_write");

    ConceptMap genderMap = reloadedMaps.getConceptMap(
        "urn:cerner:poprec:fhir:conceptmap:demographics:gender",
        "0.0.1");

    Assert.assertNotNull(genderMap);

    Assert.assertEquals("urn:cerner:poprec:fhir:conceptmap:demographics:gender",
        genderMap.getUrl());

    Assert.assertEquals("0.0.1", genderMap.getVersion());

    Assert.assertEquals(3, genderMap.getGroup().size());
  }

  @Test
  public void testGetLatest() {

    String database = "test_get_latest";
    spark.sql("create database " + database);

    ConceptMaps.getEmpty(spark)
        .withConceptMaps(
            conceptMap("urn:cerner:map:newmap", "1"),
            conceptMap("urn:cerner:map:newmap", "2"),
            conceptMap("urn:cerner:map:othermap", "1"))
        .writeToDatabase(database);

    Dataset<Mapping> latest = ConceptMaps.getFromDatabase(spark, database)
        .getLatestMappings(
            ImmutableSet.of("urn:cerner:map:newmap",
                "urn:cerner:map:othermap"),
            true);

    latest.cache();

    Assert.assertEquals(2, latest.count());

    Assert.assertEquals(0,
        latest.where("conceptMapUri == 'urn:cerner:map:newmap' and conceptMapVersion == '1'")
            .count());

    Assert.assertEquals(1,
        latest.where("conceptMapUri == 'urn:cerner:map:newmap' and conceptMapVersion == '2'")
            .count());

    Assert.assertEquals(1,
        latest.where("conceptMapUri == 'urn:cerner:map:othermap' and conceptMapVersion == '1'")
            .count());
  }

  @Test
  public void testGetLatestExperimental() {

    String database = "test_get_latest_experimental";
    spark.sql("create database " + database);

    ConceptMaps.getEmpty(spark)
        .withConceptMaps(
            conceptMap("urn:cerner:map:expmap", "1").setExperimental(false),
            conceptMap("urn:cerner:map:expmap", "2"),
            conceptMap("urn:cerner:map:otherexpmap", "1"))
        .writeToDatabase(database);

    ConceptMaps conceptMaps = ConceptMaps.getFromDatabase(spark, database);

    Dataset<Mapping> latestWithExperimental = conceptMaps.getLatestMappings(
        ImmutableSet.of("urn:cerner:map:expmap"),
        true);

    // We include experimental versions, so we should see that.
    Assert.assertEquals(1,
        latestWithExperimental
            .where("conceptMapUri == 'urn:cerner:map:expmap' and conceptMapVersion == '2'")
            .count());

    Dataset<Mapping> latestWithoutExperimental = conceptMaps.getLatestMappings(
        ImmutableSet.of("urn:cerner:map:expmap"),
        false);

    // Version 1 is not experimental, so we should see it.
    Assert.assertEquals(1,
        latestWithoutExperimental
            .where("conceptMapUri == 'urn:cerner:map:expmap' and conceptMapVersion == '1'")
            .count());

    // Loading a map with only experimental versions should find nothing.
    Dataset<Mapping> onlyExperimentalMaps = conceptMaps.getLatestMappings(
        ImmutableSet.of("urn:cerner:map:otherexpmap"),
        false);

    Assert.assertEquals(0, onlyExperimentalMaps.count());
  }

  @Test
  public void testExpandMappings() {
    ConceptMap conceptMap = ConceptMaps.getEmpty(spark)
        .withConceptMaps(conceptMap("urn:cerner:conceptmap:map", "1"))
        .getConceptMap("urn:cerner:conceptmap:map", "1");

    List<Mapping> mappings = ConceptMaps.expandMappings(conceptMap);

    Mapping expectedValue = new Mapping("urn:cerner:conceptmap:map",
        "1",
        "urn:source:valueset",
        "urn:target:valueset",
        "urn:source:system",
        "urn:source:code:a",
        "urn:target:system",
        "urn:target:code:1",
        Mapping.EQUIVALENT);

    Assert.assertEquals(1, mappings.size());
    Assert.assertEquals(expectedValue, mappings.get(0));
  }
}
