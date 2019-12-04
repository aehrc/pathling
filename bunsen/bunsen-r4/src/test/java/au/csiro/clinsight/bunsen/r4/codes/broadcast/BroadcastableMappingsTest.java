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

package au.csiro.clinsight.bunsen.r4.codes.broadcast;

import au.csiro.clinsight.bunsen.r4.codes.ConceptMaps;
import au.csiro.clinsight.bunsen.codes.broadcast.BroadcastableConceptMap;
import au.csiro.clinsight.bunsen.codes.broadcast.BroadcastableMappings;
import com.google.common.collect.ImmutableMap;
import java.util.Collections;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.SparkSession;
import org.hl7.fhir.r4.model.ConceptMap;
import org.hl7.fhir.r4.model.ConceptMap.ConceptMapGroupComponent;
import org.hl7.fhir.r4.model.ConceptMap.ConceptMapGroupUnmappedComponent;
import org.hl7.fhir.r4.model.ConceptMap.ConceptMapGroupUnmappedMode;
import org.hl7.fhir.r4.model.UriType;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Unit test of broadcast mappings.
 */
public class BroadcastableMappingsTest {

  private static SparkSession spark;

  private static Broadcast<BroadcastableMappings> broadcast;
  private static Broadcast<BroadcastableMappings> broadcastLatest;

  /**
   * Sets up Spark and concept maps for testing.
   */
  @BeforeClass
  public static void setUp() {
    spark = SparkSession.builder()
        .master("local[2]")
        .appName("BroadcastableMappingsTest")
        .getOrCreate();

    ConceptMap conceptMap = new ConceptMap();

    conceptMap.setUrl("uri:test:concept:map")
        .setVersion("0")
        .setSource(new UriType("uri:test:source:valueset"))
        .setTarget(new UriType("uri:test:target:valueset"));

    ConceptMapGroupComponent group = conceptMap.addGroup()
        .setSource("uri:test:source:system")
        .setTarget("uri:test:target:system");

    group.addElement().setCode("abc").addTarget().setCode("123");
    group.addElement().setCode("def").addTarget().setCode("456");

    ConceptMap delegatingMap = new ConceptMap();

    delegatingMap.setUrl("uri:test:concept:delegating")
        .setVersion("0")
        .setSource(new UriType("uri:test:source:valueset"))
        .setTarget(new UriType("uri:test:target:valueset"));

    delegatingMap.addGroup()
        .setSource("uri:test:source:system")
        .setTarget("uri:test:target:system")
        .setUnmapped(new ConceptMapGroupUnmappedComponent()
            .setMode(ConceptMapGroupUnmappedMode.OTHERMAP)
            .setUrl("uri:test:concept:map"));

    broadcast = ConceptMaps.getEmpty(spark)
        .withConceptMaps(conceptMap, delegatingMap)
        .broadcast(ImmutableMap.of(
            "uri:test:concept:map", "0",
            "uri:test:concept:delegating", "0"));

    ConceptMap conceptMapLatest = new ConceptMap();

    conceptMapLatest.setUrl("uri:test:concept:map")
        .setVersion("1")
        .setSource(new UriType("uri:test:source:valueset"))
        .setTarget(new UriType("uri:test:target:valueset"));

    ConceptMapGroupComponent groupLatest = conceptMapLatest.addGroup()
        .setSource("uri:test:source:system")
        .setTarget("uri:test:target:system");

    groupLatest.addElement().setCode("abc").addTarget().setCode("123");
    groupLatest.addElement().setCode("def").addTarget().setCode("xyz");

    ConceptMaps maps = ConceptMaps.getEmpty(spark)
        .withConceptMaps(conceptMap, conceptMapLatest);

    broadcastLatest = maps.broadcast(maps.getLatestVersions(true));
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
  public void testFoundMapping() {

    BroadcastableConceptMap broadcastableConceptMap =
        broadcast.getValue()
            .getBroadcastConceptMap("uri:test:concept:map");

    BroadcastableConceptMap.CodeValue value = broadcastableConceptMap
        .getTarget("uri:test:source:system", "abc").get(0);

    Assert.assertEquals("uri:test:target:system", value.getSystem());
    Assert.assertEquals("123", value.getValue());
  }

  @Test
  public void testDelegateMapping() {

    BroadcastableConceptMap broadcastableConceptMap =
        broadcast.getValue()
            .getBroadcastConceptMap("uri:test:concept:delegating");

    BroadcastableConceptMap.CodeValue value = broadcastableConceptMap
        .getTarget("uri:test:source:system", "abc").get(0);

    Assert.assertEquals("uri:test:target:system", value.getSystem());
    Assert.assertEquals("123", value.getValue());
  }

  @Test
  public void testNoValue() {

    Assert.assertEquals(broadcast.getValue()
            .getBroadcastConceptMap("uri:test:concept:map")
            .getTarget("uri:test:source:system", "nosuchvalue"),
        Collections.emptyList());
  }

  @Test
  public void testNoSystem() {

    Assert.assertEquals(broadcast.getValue()
            .getBroadcastConceptMap("uri:test:concept:map")
            .getTarget("uri:test:source:nosuchsystem", "abc"),
        Collections.emptyList());
  }

  @Test
  public void testBroadcastLatest() {

    BroadcastableConceptMap broadcastableConceptMap = broadcastLatest.getValue()
        .getBroadcastConceptMap("uri:test:concept:map");

    BroadcastableConceptMap.CodeValue value = broadcastableConceptMap
        .getTarget("uri:test:source:system", "abc").get(0);

    Assert.assertEquals("uri:test:target:system", value.getSystem());
    Assert.assertEquals("123", value.getValue());

    value = broadcastableConceptMap.getTarget("uri:test:source:system", "def").get(0);

    Assert.assertEquals("uri:test:target:system", value.getSystem());
    Assert.assertEquals("xyz", value.getValue());
  }
}
