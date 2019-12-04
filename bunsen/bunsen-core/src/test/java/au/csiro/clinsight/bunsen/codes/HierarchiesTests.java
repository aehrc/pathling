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

package au.csiro.clinsight.bunsen.codes;

import au.csiro.clinsight.bunsen.codes.Hierarchies.HierarchicalElement;
import au.csiro.clinsight.bunsen.codes.systems.Loinc;
import au.csiro.clinsight.bunsen.codes.systems.Snomed;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.nio.file.Files;
import java.util.List;
import java.util.Set;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit tests for storing, loading, and manipulating hierarchies.
 */
public class HierarchiesTests {

  private static SparkSession spark;

  private static final String HIERARCHY_URI = Hierarchies.HIERARCHY_URI_PREFIX + "testhierarchy";

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
  }

  /**
   * Tears down Spark.
   */
  @AfterClass
  public static void tearDown() {
    spark.stop();
    spark = null;
  }

  private static HierarchicalElement element(String ancestor, String descendant) {
    return new HierarchicalElement("urn:cerner:system",
        ancestor,
        "urn:cerner:system",
        descendant);
  }

  private static Ancestor ancestor(String ancestor, String descendant) {
    return new Ancestor(HIERARCHY_URI,
        "1",
        "urn:cerner:system",
        descendant,
        "urn:cerner:system",
        ancestor);
  }

  private static Dataset<HierarchicalElement> getElements() {
    return spark.createDataset(ImmutableList.of(
        element("a", "b"),
        element("a", "d"),
        element("b", "c"),
        element("e", "f")),
        Hierarchies.getHierarchicalElementEncoder());
  }

  private static final Set<Ancestor> ANCESTOR_CLOSURE = ImmutableSet.of(
      ancestor("a", "b"),
      ancestor("a", "c"),
      ancestor("a", "d"),
      ancestor("b", "c"),
      ancestor("e", "f"));

  @Test
  public void testCreateHierarchy() {

    Hierarchies hierarchies = Hierarchies.getEmpty(spark)
        .withHierarchyElements(HIERARCHY_URI, "1", getElements());

    List<Ancestor> ancestors = hierarchies.getAncestors().collectAsList();
    List<UrlAndVersion> members = hierarchies.getMembers().collectAsList();

    Assert.assertEquals(5, ancestors.size());
    Assert.assertTrue(ANCESTOR_CLOSURE.containsAll(ancestors));

    Assert.assertEquals(1, members.size());
    Assert.assertEquals(new UrlAndVersion(HIERARCHY_URI, "1"), members.get(0));
  }

  @Test
  public void testAppendHierarchies() {

    Hierarchies withLoinc = Loinc.withLoincHierarchy(spark,
        Hierarchies.getEmpty(spark),
        "src/test/resources/LOINC_HIERARCHY_SAMPLE.CSV",
        "2.56");

    Hierarchies withSnomed = Snomed.withRelationships(spark,
        withLoinc,
        "src/test/resources/SNOMED_RELATIONSHIP_SAMPLE.TXT",
        "20160901");

    Hierarchies hierarchies = Hierarchies.getEmpty(spark)
        .withHierarchyElements(HIERARCHY_URI, "1", getElements())
        .withHierarchies(withSnomed);

    List<Ancestor> ancestors = hierarchies.getAncestors().collectAsList();
    List<UrlAndVersion> members = hierarchies.getMembers().collectAsList();

    Set<UrlAndVersion> expected = ImmutableSet.of(
        new UrlAndVersion(Loinc.LOINC_HIERARCHY_URI, "2.56"),
        new UrlAndVersion(Snomed.SNOMED_HIERARCHY_URI, "20160901"),
        new UrlAndVersion(HIERARCHY_URI, "1"));

    Assert.assertEquals(35, ancestors.size());

    Assert.assertEquals(3, members.size());
    Assert.assertTrue(expected.containsAll(members));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAppendDuplicateHierarchyThrowsException() {

    Hierarchies hierarchies = Hierarchies.getEmpty(spark)
        .withHierarchyElements(HIERARCHY_URI, "1", getElements());

    hierarchies.withHierarchyElements(HIERARCHY_URI, "1", getElements());
  }

  @Test(expected = IllegalArgumentException.class)
  public void testAppendDuplicateHierarchiesThrowsException() {

    Hierarchies hierarchies = Hierarchies.getEmpty(spark)
        .withHierarchyElements(HIERARCHY_URI, "1", getElements());

    hierarchies.withHierarchies(hierarchies);
  }

  @Test
  public void testWriteToNewTables() {

    String database = "test_hierarchies_write";
    spark.sql("CREATE DATABASE " + database);

    Hierarchies.getEmpty(spark)
        .withHierarchyElements(HIERARCHY_URI, "1", getElements())
        .writeToDatabase(database);

    Hierarchies hierarchies = Hierarchies.getFromDatabase(spark, database);

    List<Ancestor> ancestors = hierarchies.getAncestors().collectAsList();
    List<UrlAndVersion> members = hierarchies.getMembers().collectAsList();

    Assert.assertEquals(5, ancestors.size());
    Assert.assertTrue(ANCESTOR_CLOSURE.containsAll(ancestors));

    Assert.assertEquals(1, members.size());
    Assert.assertEquals(new UrlAndVersion(HIERARCHY_URI, "1"), members.get(0));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testWritingDuplicateHierarchiesThrowsException() {

    String database = "duplicate_hierarchies_write";
    spark.sql("CREATE DATABASE " + database);

    Hierarchies hierarchies = Hierarchies.getEmpty(spark)
        .withHierarchyElements(HIERARCHY_URI, "1", getElements());

    hierarchies.writeToDatabase(database);

    Hierarchies reloaded = Hierarchies.getFromDatabase(spark, database);

    reloaded.writeToDatabase(database);
  }
}
