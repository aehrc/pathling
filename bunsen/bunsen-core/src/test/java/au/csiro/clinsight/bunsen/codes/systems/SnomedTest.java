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

package au.csiro.clinsight.bunsen.codes.systems;

import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.lit;

import au.csiro.clinsight.bunsen.codes.Hierarchies.HierarchicalElement;
import java.util.List;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit test to import SNOMED mappings.
 */
public class SnomedTest {

  private static SparkSession spark;

  private static Dataset<HierarchicalElement> snomedValues;

  /**
   * Sets up Spark and loads the SNOMED mappings for testing.
   */
  @BeforeClass
  public static void setUp() {

    spark = SparkSession.builder()
        .master("local[2]")
        .appName("SnomedTest")
        .getOrCreate();

    snomedValues = Snomed.readRelationshipFile(spark,
        "src/test/resources/SNOMED_RELATIONSHIP_SAMPLE.TXT");

    snomedValues.cache();
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
  public void testHasParent() {

    List<HierarchicalElement> values = snomedValues
        .where(col("descendantValue")
            .equalTo(lit("44054006")))
        .collectAsList();

    Assert.assertEquals(1, values.size());
    Assert.assertEquals("73211009",
        values.get(0).getAncestorValue());
  }

  @Test
  public void checkSystems() {

    // All imported rows should have the expected system
    Assert.assertEquals(snomedValues.count(),
        snomedValues
            .where(col("ancestorSystem").equalTo(lit(Snomed.SNOMED_CODE_SYSTEM_URI))
                .and(col("descendantSystem").equalTo(lit(Snomed.SNOMED_CODE_SYSTEM_URI))))
            .count());
  }
}
