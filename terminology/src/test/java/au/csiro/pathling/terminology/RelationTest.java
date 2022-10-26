/*
 * Copyright 2022 Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package au.csiro.pathling.terminology;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.fhirpath.encoding.SimpleCoding;
import au.csiro.pathling.terminology.Relation.CodingSet;
import au.csiro.pathling.terminology.Relation.Entry;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import javax.annotation.Nonnull;
import org.junit.jupiter.api.Test;

class RelationTest {

  static final SimpleCoding CODING1_UNVERSIONED = new SimpleCoding("uuid:system1", "code");
  static final SimpleCoding CODING1_VERSION1 =
      new SimpleCoding("uuid:system1", "code", "version1");
  static final SimpleCoding CODING1_VERSION2 =
      new SimpleCoding("uuid:system1", "code", "version2");

  static final SimpleCoding CODING2_UNVERSIONED = new SimpleCoding("uuid:system2", "code");
  static final SimpleCoding CODING2_VERSION1 =
      new SimpleCoding("uuid:system2", "code", "version1");
  static final SimpleCoding CODING2_VERSION2 =
      new SimpleCoding("uuid:system2", "code", "version2");

  static final SimpleCoding CODING3_UNVERSIONED = new SimpleCoding("uuid:system1", "code1");
  static final SimpleCoding CODING3_VERSION1 =
      new SimpleCoding("uuid:system1", "code1", "version1");

  @Nonnull
  static Set<SimpleCoding> setOf(@Nonnull final SimpleCoding... codings) {
    return new HashSet<>(Arrays.asList(codings));
  }

  @Test
  void testVersionedCodingSet() {
    final CodingSet versionedCodingSet = new CodingSet(setOf(CODING1_VERSION1));
    assertTrue(versionedCodingSet.contains(CODING1_UNVERSIONED));
    assertTrue(versionedCodingSet.contains(CODING1_VERSION1));
    assertFalse(versionedCodingSet.contains(CODING1_VERSION2));
    assertFalse(versionedCodingSet.contains(CODING2_VERSION1));
  }

  @Test
  void testUnversionedCodingSet() {
    final CodingSet unversionedCodingSet = new CodingSet(setOf(CODING1_UNVERSIONED));
    assertTrue(unversionedCodingSet.contains(CODING1_UNVERSIONED));
    assertTrue(unversionedCodingSet.contains(CODING1_VERSION1));
    assertTrue(unversionedCodingSet.contains(CODING1_VERSION2));
    assertFalse(unversionedCodingSet.contains(CODING2_VERSION1));
  }

  @Test
  void testEmptyClosure() {
    final Relation emptyRelation = Relation.fromMappings(Collections.emptyList());
    checkBasicEqualities(emptyRelation);
  }

  void checkBasicEqualities(final Relation relation) {
    assertFalse(relation.anyRelates(Collections.emptyList(), Collections.emptyList()));
    assertFalse(
        relation.anyRelates(Collections.singletonList(CODING1_UNVERSIONED),
            Collections.emptyList()))
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
  void testUnversionedClosure() {
    final Relation versionedRelation =
        Relation.fromMappings(
            Collections.singletonList(Entry.of(CODING1_UNVERSIONED, CODING2_UNVERSIONED)));
    // in addition to all equalities

    checkBasicEqualities(versionedRelation);

    assertTrue(
        versionedRelation.anyRelates(Collections.singletonList(CODING1_UNVERSIONED),
            Collections.singletonList(CODING2_UNVERSIONED)));

    assertTrue(versionedRelation.anyRelates(Collections.singletonList(CODING1_VERSION1),
        Collections.singletonList(CODING2_UNVERSIONED)));

    assertTrue(versionedRelation.anyRelates(Collections.singletonList(CODING1_VERSION1),
        Collections.singletonList(CODING2_VERSION2)));

    assertTrue(
        versionedRelation.anyRelates(Collections.singletonList(CODING1_UNVERSIONED),
            Collections.singletonList(CODING2_VERSION2)));

    assertFalse(
        versionedRelation.anyRelates(Collections.singletonList(CODING1_UNVERSIONED),
            Collections.singletonList(CODING3_UNVERSIONED)));

  }

  @Test
  void testVersionedClosure() {

    final List<Entry> entries = Collections
        .singletonList(Entry.of(CODING1_VERSION1, CODING2_VERSION1));
    final Relation versionedRelation = Relation.fromMappings(entries);
    // in addition to all equalities
    checkBasicEqualities(versionedRelation);

    assertTrue(
        versionedRelation.anyRelates(Collections.singletonList(CODING1_UNVERSIONED),
            Collections.singletonList(CODING2_UNVERSIONED)));

    assertTrue(versionedRelation.anyRelates(Collections.singletonList(CODING1_VERSION1),
        Collections.singletonList(CODING2_UNVERSIONED)));

    assertTrue(versionedRelation.anyRelates(Collections.singletonList(CODING1_VERSION1),
        Collections.singletonList(CODING2_VERSION1)));

    assertTrue(
        versionedRelation.anyRelates(Collections.singletonList(CODING1_UNVERSIONED),
            Collections.singletonList(CODING2_VERSION1)));

    assertFalse(versionedRelation.anyRelates(Collections.singletonList(CODING1_VERSION1),
        Collections.singletonList(CODING2_VERSION2)));

    assertFalse(
        versionedRelation.anyRelates(Collections.singletonList(CODING1_UNVERSIONED),
            Collections.singletonList(CODING3_UNVERSIONED)));

  }
}
