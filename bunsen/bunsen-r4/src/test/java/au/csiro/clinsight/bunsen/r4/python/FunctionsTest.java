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

package au.csiro.clinsight.bunsen.r4.python;

import ca.uhn.fhir.context.FhirContext;
import au.csiro.clinsight.bunsen.FhirEncoders;
import com.google.common.collect.ImmutableList;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.Bundle;
import org.hl7.fhir.r4.model.Condition;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test for common FHIR functions.
 */
public class FunctionsTest {

  private static final FhirContext CONTEXT = FhirContext.forR4();

  private static SparkSession spark;

  private static FhirEncoders encoders = FhirEncoders.forR4().getOrCreate();

  private static Condition condition;

  private static Dataset<Condition> conditions;

  /**
   * Sets up Spark.
   */
  @BeforeClass
  public static void setUp() {
    spark = SparkSession.builder()
        .master("local[*]")
        .appName("testing")
        .getOrCreate();

    condition = new Condition();

    condition.setId("Condition/testid");

    conditions = spark.createDataset(ImmutableList.of(condition),
        encoders.of(Condition.class));
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
  public void resourceToJson() {

    Dataset<String> jsonDs = Functions.toJson(conditions, "condition");

    String conditionJson = jsonDs.first();

    Condition parsedCondition = (Condition) CONTEXT.newJsonParser()
        .parseResource(conditionJson);

    Assert.assertEquals(condition.getId(), parsedCondition.getId());
  }

  @Test
  public void bundleToJson() {

    String jsonBundle = Functions.toJsonBundle(conditions);

    Bundle bundle = (Bundle) CONTEXT.newJsonParser().parseResource(jsonBundle);

    Condition parsedCondition = (Condition) bundle.getEntryFirstRep().getResource();

    Assert.assertEquals(condition.getId(), parsedCondition.getId());
  }
}
