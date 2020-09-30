/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function.subsumes;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.fhirpath.encoding.SimpleCoding;
import au.csiro.pathling.fhirpath.function.subsumes.Closure.CodingSet;
import java.util.*;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("UnitTest")
public class ClosureTest {

  public static final SimpleCoding CODING1_UNVERSIONED = new SimpleCoding("uuid:system1", "code");
  public static final SimpleCoding CODING1_VERSION1 =
      new SimpleCoding("uuid:system1", "code", "version1");
  public static final SimpleCoding CODING1_VERSION2 =
      new SimpleCoding("uuid:system1", "code", "version2");

  public static final SimpleCoding CODING2_UNVERSIONED = new SimpleCoding("uuid:system2", "code");
  public static final SimpleCoding CODING2_VERSION1 =
      new SimpleCoding("uuid:system2", "code", "version1");
  public static final SimpleCoding CODING2_VERSION2 =
      new SimpleCoding("uuid:system2", "code", "version2");

  public static final SimpleCoding CODING3_UNVERSIONED = new SimpleCoding("uuid:system1", "code1");
  public static final SimpleCoding CODING3_VERSION1 =
      new SimpleCoding("uuid:system1", "code1", "version1");
  public static final SimpleCoding CODING3_VERSION2 =
      new SimpleCoding("uuid:system1", "code1", "version2");

  private static Set<SimpleCoding> setOf(SimpleCoding... codings) {
    return new HashSet<SimpleCoding>(Arrays.asList(codings));
  }

  @Test
  public void testVersionedCodingSet() {
    CodingSet versionedCodingSet = new Closure.CodingSet(setOf(CODING1_VERSION1));
    assertTrue(versionedCodingSet.contains(CODING1_UNVERSIONED));
    assertTrue(versionedCodingSet.contains(CODING1_VERSION1));
    assertFalse(versionedCodingSet.contains(CODING1_VERSION2));
    assertFalse(versionedCodingSet.contains(CODING2_VERSION1));
  }

  @Test
  public void testUncersionedCodingSet() {
    CodingSet unversionedCodingSet = new Closure.CodingSet(setOf(CODING1_UNVERSIONED));
    assertTrue(unversionedCodingSet.contains(CODING1_UNVERSIONED));
    assertTrue(unversionedCodingSet.contains(CODING1_VERSION1));
    assertTrue(unversionedCodingSet.contains(CODING1_VERSION2));
    assertFalse(unversionedCodingSet.contains(CODING2_VERSION1));
  }

  @Test
  public void testEmptyClosure() {
    Closure emptyClosure = Closure.fromMappings(Collections.emptyList());
    checkBasicEqualities(emptyClosure);
  }

  private void checkBasicEqualities(Closure closure) {
    assertFalse(closure.anyRelates(Collections.emptyList(), Collections.emptyList()));
    assertFalse(closure.anyRelates(Arrays.asList(CODING1_UNVERSIONED), Collections.emptyList()))
    ;
    assertFalse(closure.anyRelates(Collections.emptyList(), Arrays.asList(CODING1_UNVERSIONED)))
    ;

    assertTrue(
        closure.anyRelates(Arrays.asList(CODING1_UNVERSIONED), Arrays.asList(CODING1_UNVERSIONED)))
    ;
    assertTrue(
        closure.anyRelates(Arrays.asList(CODING1_VERSION1), Arrays.asList(CODING1_UNVERSIONED)))
    ;
    assertTrue(
        closure.anyRelates(Arrays.asList(CODING1_UNVERSIONED), Arrays.asList(CODING1_VERSION1)))
    ;
    assertTrue(closure.anyRelates(Arrays.asList(CODING1_VERSION1), Arrays.asList(CODING1_VERSION1)))
    ;
    assertFalse(
        closure.anyRelates(Arrays.asList(CODING1_VERSION1), Arrays.asList(CODING1_VERSION2)))
    ;
    assertFalse(
        closure.anyRelates(Arrays.asList(CODING1_UNVERSIONED), Arrays.asList(CODING3_UNVERSIONED)))
    ;
    assertFalse(
        closure.anyRelates(Arrays.asList(CODING1_VERSION1), Arrays.asList(CODING3_VERSION1)))
    ;
  }

  @Test
  public void testUnversionedClosure() {
    Closure versionedClosure =
        Closure.fromMappings(Arrays.asList(Mapping.of(CODING1_UNVERSIONED, CODING2_UNVERSIONED)));
    // in addition to all equalities

    checkBasicEqualities(versionedClosure);

    assertTrue(versionedClosure.anyRelates(Arrays.asList(CODING1_UNVERSIONED),
        Arrays.asList(CODING2_UNVERSIONED)));

    assertTrue(versionedClosure.anyRelates(Arrays.asList(CODING1_VERSION1),
        Arrays.asList(CODING2_UNVERSIONED)));

    assertTrue(versionedClosure.anyRelates(Arrays.asList(CODING1_VERSION1),
        Arrays.asList(CODING2_VERSION2)));

    assertTrue(versionedClosure.anyRelates(Arrays.asList(CODING1_UNVERSIONED),
        Arrays.asList(CODING2_VERSION2)));

    assertFalse(versionedClosure.anyRelates(Arrays.asList(CODING1_UNVERSIONED),
        Arrays.asList(CODING3_UNVERSIONED)));

  }

  @Test
  public void testVersionedClosure() {

    List<Mapping> mappings = Arrays.asList(Mapping.of(CODING1_VERSION1, CODING2_VERSION1));
    Closure versionedClosure = Closure.fromMappings(mappings);
    // in addition to all equalities
    checkBasicEqualities(versionedClosure);

    assertTrue(versionedClosure.anyRelates(Arrays.asList(CODING1_UNVERSIONED),
        Arrays.asList(CODING2_UNVERSIONED)));

    assertTrue(versionedClosure.anyRelates(Arrays.asList(CODING1_VERSION1),
        Arrays.asList(CODING2_UNVERSIONED)));

    assertTrue(versionedClosure.anyRelates(Arrays.asList(CODING1_VERSION1),
        Arrays.asList(CODING2_VERSION1)));

    assertTrue(versionedClosure.anyRelates(Arrays.asList(CODING1_UNVERSIONED),
        Arrays.asList(CODING2_VERSION1)));

    assertFalse(versionedClosure.anyRelates(Arrays.asList(CODING1_VERSION1),
        Arrays.asList(CODING2_VERSION2)));

    assertFalse(versionedClosure.anyRelates(Arrays.asList(CODING1_UNVERSIONED),
        Arrays.asList(CODING3_UNVERSIONED)));

  }
}
