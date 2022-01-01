/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.terminology;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.fhirpath.encoding.SimpleCoding;
import au.csiro.pathling.terminology.Relation.CodingSet;
import au.csiro.pathling.terminology.Relation.Entry;
import java.util.*;
import javax.annotation.Nonnull;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("UnitTest")
public class RelationTest {

  private static final SimpleCoding CODING1_UNVERSIONED = new SimpleCoding("uuid:system1", "code");
  private static final SimpleCoding CODING1_VERSION1 =
      new SimpleCoding("uuid:system1", "code", "version1");
  private static final SimpleCoding CODING1_VERSION2 =
      new SimpleCoding("uuid:system1", "code", "version2");

  private static final SimpleCoding CODING2_UNVERSIONED = new SimpleCoding("uuid:system2", "code");
  private static final SimpleCoding CODING2_VERSION1 =
      new SimpleCoding("uuid:system2", "code", "version1");
  private static final SimpleCoding CODING2_VERSION2 =
      new SimpleCoding("uuid:system2", "code", "version2");

  private static final SimpleCoding CODING3_UNVERSIONED = new SimpleCoding("uuid:system1", "code1");
  private static final SimpleCoding CODING3_VERSION1 =
      new SimpleCoding("uuid:system1", "code1", "version1");

  @Nonnull
  private static Set<SimpleCoding> setOf(@Nonnull final SimpleCoding... codings) {
    return new HashSet<>(Arrays.asList(codings));
  }

  @Test
  public void testVersionedCodingSet() {
    final CodingSet versionedCodingSet = new CodingSet(setOf(CODING1_VERSION1));
    assertTrue(versionedCodingSet.contains(CODING1_UNVERSIONED));
    assertTrue(versionedCodingSet.contains(CODING1_VERSION1));
    assertFalse(versionedCodingSet.contains(CODING1_VERSION2));
    assertFalse(versionedCodingSet.contains(CODING2_VERSION1));
  }

  @Test
  public void testUnversionedCodingSet() {
    final CodingSet unversionedCodingSet = new CodingSet(setOf(CODING1_UNVERSIONED));
    assertTrue(unversionedCodingSet.contains(CODING1_UNVERSIONED));
    assertTrue(unversionedCodingSet.contains(CODING1_VERSION1));
    assertTrue(unversionedCodingSet.contains(CODING1_VERSION2));
    assertFalse(unversionedCodingSet.contains(CODING2_VERSION1));
  }

  @Test
  public void testEmptyClosure() {
    final Relation emptyRelation = Relation.fromMappings(Collections.emptyList());
    checkBasicEqualities(emptyRelation);
  }

  private void checkBasicEqualities(final Relation relation) {
    assertFalse(relation.anyRelates(Collections.emptyList(), Collections.emptyList()));
    assertFalse(
        relation.anyRelates(Collections.singletonList(CODING1_UNVERSIONED), Collections.emptyList()))
    ;
    assertFalse(relation.anyRelates(Collections.emptyList(),
        Collections.singletonList(CODING1_UNVERSIONED)))
    ;

    assertTrue(
        relation.anyRelates(Collections.singletonList(CODING1_UNVERSIONED),
            Collections.singletonList(CODING1_UNVERSIONED)))
    ;
    assertTrue(
        relation.anyRelates(Collections.singletonList(CODING1_VERSION1),
            Collections.singletonList(CODING1_UNVERSIONED)))
    ;
    assertTrue(
        relation.anyRelates(Collections.singletonList(CODING1_UNVERSIONED),
            Collections.singletonList(CODING1_VERSION1)))
    ;
    assertTrue(relation.anyRelates(Collections.singletonList(CODING1_VERSION1),
        Collections.singletonList(CODING1_VERSION1)))
    ;
    assertFalse(
        relation.anyRelates(Collections.singletonList(CODING1_VERSION1),
            Collections.singletonList(CODING1_VERSION2)))
    ;
    assertFalse(
        relation.anyRelates(Collections.singletonList(CODING1_UNVERSIONED),
            Collections.singletonList(CODING3_UNVERSIONED)))
    ;
    assertFalse(
        relation.anyRelates(Collections.singletonList(CODING1_VERSION1),
            Collections.singletonList(CODING3_VERSION1)))
    ;
  }

  @Test
  public void testUnversionedClosure() {
    final Relation versionedRelation =
        Relation.fromMappings(
            Collections.singletonList(Entry.of(CODING1_UNVERSIONED, CODING2_UNVERSIONED)));
    // in addition to all equalities

    checkBasicEqualities(versionedRelation);

    assertTrue(versionedRelation.anyRelates(Collections.singletonList(CODING1_UNVERSIONED),
        Collections.singletonList(CODING2_UNVERSIONED)));

    assertTrue(versionedRelation.anyRelates(Collections.singletonList(CODING1_VERSION1),
        Collections.singletonList(CODING2_UNVERSIONED)));

    assertTrue(versionedRelation.anyRelates(Collections.singletonList(CODING1_VERSION1),
        Collections.singletonList(CODING2_VERSION2)));

    assertTrue(versionedRelation.anyRelates(Collections.singletonList(CODING1_UNVERSIONED),
        Collections.singletonList(CODING2_VERSION2)));

    assertFalse(versionedRelation.anyRelates(Collections.singletonList(CODING1_UNVERSIONED),
        Collections.singletonList(CODING3_UNVERSIONED)));

  }

  @Test
  public void testVersionedClosure() {

    final List<Entry> entries = Collections
        .singletonList(Entry.of(CODING1_VERSION1, CODING2_VERSION1));
    final Relation versionedRelation = Relation.fromMappings(entries);
    // in addition to all equalities
    checkBasicEqualities(versionedRelation);

    assertTrue(versionedRelation.anyRelates(Collections.singletonList(CODING1_UNVERSIONED),
        Collections.singletonList(CODING2_UNVERSIONED)));

    assertTrue(versionedRelation.anyRelates(Collections.singletonList(CODING1_VERSION1),
        Collections.singletonList(CODING2_UNVERSIONED)));

    assertTrue(versionedRelation.anyRelates(Collections.singletonList(CODING1_VERSION1),
        Collections.singletonList(CODING2_VERSION1)));

    assertTrue(versionedRelation.anyRelates(Collections.singletonList(CODING1_UNVERSIONED),
        Collections.singletonList(CODING2_VERSION1)));

    assertFalse(versionedRelation.anyRelates(Collections.singletonList(CODING1_VERSION1),
        Collections.singletonList(CODING2_VERSION2)));

    assertFalse(versionedRelation.anyRelates(Collections.singletonList(CODING1_UNVERSIONED),
        Collections.singletonList(CODING3_UNVERSIONED)));

  }
}
