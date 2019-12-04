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
 * Unit test to import LOINC mappings.
 */
public class LoincTest {

  private static SparkSession spark;

  private static Dataset<HierarchicalElement> loincValues;

  /**
   * Sets up Spark.
   */
  @BeforeClass
  public static void setUp() throws Exception {

    spark = SparkSession.builder()
        .master("local[2]")
        .appName("SnomedTest")
        .getOrCreate();

    loincValues = Loinc.readMultiaxialHierarchyFile(spark,
        "src/test/resources/LOINC_HIERARCHY_SAMPLE.CSV");

    loincValues.cache();
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

    List<HierarchicalElement> values = loincValues
        .where(col("descendantValue")
            .equalTo(lit("LP14559-6")))
        .collectAsList();

    Assert.assertEquals(1, values.size());
    Assert.assertEquals("LP31755-9",
        values.get(0).getAncestorValue());
  }

  @Test
  public void checkSystems() {

    // All imported rows should have the expected system
    Assert.assertEquals(loincValues.count(),
        loincValues
            .where(col("ancestorSystem").equalTo(lit(Loinc.LOINC_CODE_SYSTEM_URI))
                .and(col("descendantSystem").equalTo(lit(Loinc.LOINC_CODE_SYSTEM_URI))))
            .count());
  }
}
