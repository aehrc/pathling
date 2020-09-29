/*
 * Copyright © 2018-2020, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.fhirpath.function.subsumes;

import static org.assertj.core.api.Assertions.assertThat;

import au.csiro.pathling.fhirpath.encoding.SimpleCoding;
import au.csiro.pathling.fhirpath.function.subsumes.Closure.CodingSet;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.Test;
import org.junit.jupiter.api.Tag;

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
    assertThat(versionedCodingSet.contains(CODING1_UNVERSIONED)).isTrue();
    assertThat(versionedCodingSet.contains(CODING1_VERSION1)).isTrue();
    assertThat(versionedCodingSet.contains(CODING1_VERSION2)).isFalse();
    assertThat(versionedCodingSet.contains(CODING2_VERSION1)).isFalse();
  }

  @Test
  public void testUncersionedCodingSet() {
    CodingSet unversionedCodingSet = new Closure.CodingSet(setOf(CODING1_UNVERSIONED));
    assertThat(unversionedCodingSet.contains(CODING1_UNVERSIONED)).isTrue();
    assertThat(unversionedCodingSet.contains(CODING1_VERSION1)).isTrue();
    assertThat(unversionedCodingSet.contains(CODING1_VERSION2)).isTrue();
    assertThat(unversionedCodingSet.contains(CODING2_VERSION1)).isFalse();
  }

  @Test
  public void testEmptyClosure() {
    Closure emptyClosure = Closure.fromMappings(Collections.emptyList());
    checkBasicEqualities(emptyClosure);
  }

  private void checkBasicEqualities(Closure closure) {
    assertThat(closure.anyRelates(Collections.emptyList(), Collections.emptyList())).isFalse();
    assertThat(closure.anyRelates(Arrays.asList(CODING1_UNVERSIONED), Collections.emptyList()))
        .isFalse();
    assertThat(closure.anyRelates(Collections.emptyList(), Arrays.asList(CODING1_UNVERSIONED)))
        .isFalse();

    assertThat(
        closure.anyRelates(Arrays.asList(CODING1_UNVERSIONED), Arrays.asList(CODING1_UNVERSIONED)))
        .isTrue();
    assertThat(
        closure.anyRelates(Arrays.asList(CODING1_VERSION1), Arrays.asList(CODING1_UNVERSIONED)))
        .isTrue();
    assertThat(
        closure.anyRelates(Arrays.asList(CODING1_UNVERSIONED), Arrays.asList(CODING1_VERSION1)))
        .isTrue();
    assertThat(closure.anyRelates(Arrays.asList(CODING1_VERSION1), Arrays.asList(CODING1_VERSION1)))
        .isTrue();
    assertThat(closure.anyRelates(Arrays.asList(CODING1_VERSION1), Arrays.asList(CODING1_VERSION2)))
        .isFalse();
    assertThat(
        closure.anyRelates(Arrays.asList(CODING1_UNVERSIONED), Arrays.asList(CODING3_UNVERSIONED)))
        .isFalse();
    assertThat(closure.anyRelates(Arrays.asList(CODING1_VERSION1), Arrays.asList(CODING3_VERSION1)))
        .isFalse();
  }

  @Test
  public void testUnversionedClosure() {
    Closure versionedClosure =
        Closure.fromMappings(Arrays.asList(Mapping.of(CODING1_UNVERSIONED, CODING2_UNVERSIONED)));
    // in addition to all equalities

    checkBasicEqualities(versionedClosure);

    assertThat(versionedClosure.anyRelates(Arrays.asList(CODING1_UNVERSIONED),
        Arrays.asList(CODING2_UNVERSIONED))).isTrue();

    assertThat(versionedClosure.anyRelates(Arrays.asList(CODING1_VERSION1),
        Arrays.asList(CODING2_UNVERSIONED))).isTrue();

    assertThat(versionedClosure.anyRelates(Arrays.asList(CODING1_VERSION1),
        Arrays.asList(CODING2_VERSION2))).isTrue();

    assertThat(versionedClosure.anyRelates(Arrays.asList(CODING1_UNVERSIONED),
        Arrays.asList(CODING2_VERSION2))).isTrue();

    assertThat(versionedClosure.anyRelates(Arrays.asList(CODING1_UNVERSIONED),
        Arrays.asList(CODING3_UNVERSIONED))).isFalse();

  }

  @Test
  public void testVersionedClosure() {

    List<Mapping> mappings = Arrays.asList(Mapping.of(CODING1_VERSION1, CODING2_VERSION1));
    Closure versionedClosure = Closure.fromMappings(mappings);
    // in addition to all equalities
    checkBasicEqualities(versionedClosure);

    assertThat(versionedClosure.anyRelates(Arrays.asList(CODING1_UNVERSIONED),
        Arrays.asList(CODING2_UNVERSIONED))).isTrue();

    System.out.println(
        versionedClosure.expand(new HashSet<SimpleCoding>(Arrays.asList(CODING1_VERSION1))));

    assertThat(versionedClosure.anyRelates(Arrays.asList(CODING1_VERSION1),
        Arrays.asList(CODING2_UNVERSIONED))).isTrue();

    assertThat(versionedClosure.anyRelates(Arrays.asList(CODING1_VERSION1),
        Arrays.asList(CODING2_VERSION1))).isTrue();

    assertThat(versionedClosure.anyRelates(Arrays.asList(CODING1_UNVERSIONED),
        Arrays.asList(CODING2_VERSION1))).isTrue();

    assertThat(versionedClosure.anyRelates(Arrays.asList(CODING1_VERSION1),
        Arrays.asList(CODING2_VERSION2))).isFalse();

    assertThat(versionedClosure.anyRelates(Arrays.asList(CODING1_UNVERSIONED),
        Arrays.asList(CODING3_UNVERSIONED))).isFalse();

  }
}
