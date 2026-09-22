/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.terminology.expand;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import org.hl7.fhir.r4.model.UriType;
import org.hl7.fhir.r4.model.ValueSet;
import org.hl7.fhir.r4.model.ValueSet.ValueSetExpansionComponent;
import org.hl7.fhir.r4.model.ValueSet.ValueSetExpansionContainsComponent;
import org.junit.jupiter.api.Test;

/**
 * Tests for {@link ValueSetExpansion#fromResource}: the flattening of a FHIR expansion into a
 * membership, its deduplication and abstract-entry rules, the completeness and validity checks on
 * the supplied expansion, the member limit, and the provenance carried across.
 *
 * @author John Grimes
 */
class ValueSetExpansionTest {

  private static final String URL = "http://example.org/ValueSet/cardiovascular-disease";
  private static final String SNOMED = "http://snomed.info/sct";
  private static final String ICD10 = "http://hl7.org/fhir/sid/icd-10";
  private static final int NO_LIMIT = Integer.MAX_VALUE;

  @Nonnull
  private static ValueSet valueSet(@Nonnull final ValueSetExpansionContainsComponent... entries) {
    final ValueSet valueSet = new ValueSet();
    valueSet.setUrl(URL);
    final ValueSetExpansionComponent expansion = valueSet.getExpansion();
    for (final ValueSetExpansionContainsComponent entry : entries) {
      expansion.addContains(entry);
    }
    return valueSet;
  }

  @Nonnull
  private static ValueSetExpansionContainsComponent entry(
      @Nullable final String system, @Nullable final String code) {
    final ValueSetExpansionContainsComponent entry = new ValueSetExpansionContainsComponent();
    entry.setSystem(system);
    entry.setCode(code);
    return entry;
  }

  @Nonnull
  private static ValueSetExpansionContainsComponent entry(
      @Nullable final String system,
      @Nullable final String version,
      @Nullable final String code,
      @Nullable final String display) {
    final ValueSetExpansionContainsComponent entry = entry(system, code);
    entry.setVersion(version);
    entry.setDisplay(display);
    return entry;
  }

  @Nonnull
  private static ValueSetExpansionContainsComponent abstractEntry(
      @Nonnull final ValueSetExpansionContainsComponent... children) {
    final ValueSetExpansionContainsComponent entry = new ValueSetExpansionContainsComponent();
    entry.setAbstract(true);
    for (final ValueSetExpansionContainsComponent child : children) {
      entry.addContains(child);
    }
    return entry;
  }

  @Nonnull
  private static ValueSetMember member(@Nonnull final String system, @Nonnull final String code) {
    return new ValueSetMember(system, null, code, null, null);
  }

  @Test
  void flattensNestedContainsAtEveryDepth() {
    final ValueSetExpansionContainsComponent deep = entry(SNOMED, "3");
    final ValueSetExpansionContainsComponent middle = entry(SNOMED, "2");
    middle.addContains(deep);
    final ValueSetExpansionContainsComponent top = entry(SNOMED, "1");
    top.addContains(middle);

    final ValueSetExpansion expansion = ValueSetExpansion.fromResource(valueSet(top), NO_LIMIT);

    assertEquals(
        List.of(member(SNOMED, "1"), member(SNOMED, "2"), member(SNOMED, "3")),
        expansion.getMembers());
  }

  @Test
  void skipsAbstractEntryButRecursesIntoIt() {
    final ValueSet valueSet =
        valueSet(abstractEntry(entry(SNOMED, "22298006"), entry(ICD10, "I21")));

    final ValueSetExpansion expansion = ValueSetExpansion.fromResource(valueSet, NO_LIMIT);

    assertEquals(List.of(member(SNOMED, "22298006"), member(ICD10, "I21")), expansion.getMembers());
  }

  @Test
  void keepsDuplicateAcrossParentsOnce() {
    final ValueSet valueSet =
        valueSet(
            abstractEntry(entry(SNOMED, "1")),
            abstractEntry(entry(SNOMED, "1")),
            entry(SNOMED, "1"));

    final ValueSetExpansion expansion = ValueSetExpansion.fromResource(valueSet, NO_LIMIT);

    assertEquals(List.of(member(SNOMED, "1")), expansion.getMembers());
  }

  @Test
  void keepsSameCodeUnderTwoVersionsTwice() {
    final ValueSet valueSet =
        valueSet(entry(SNOMED, "v1", "1", null), entry(SNOMED, "v2", "1", null));

    final ValueSetExpansion expansion = ValueSetExpansion.fromResource(valueSet, NO_LIMIT);

    assertEquals(
        List.of(
            new ValueSetMember(SNOMED, "v1", "1", null, null),
            new ValueSetMember(SNOMED, "v2", "1", null, null)),
        expansion.getMembers());
  }

  @Test
  void treatsNullAndNonNullVersionAsDistinct() {
    final ValueSet valueSet =
        valueSet(entry(SNOMED, null, "1", null), entry(SNOMED, "v1", "1", null));

    final ValueSetExpansion expansion = ValueSetExpansion.fromResource(valueSet, NO_LIMIT);

    assertEquals(2, expansion.getMembers().size());
  }

  @Test
  void carriesNullDisplayWhereAbsent() {
    final ValueSetExpansion expansion =
        ValueSetExpansion.fromResource(valueSet(entry(SNOMED, "1")), NO_LIMIT);

    assertNull(expansion.getMembers().get(0).getDisplay());
  }

  @Test
  void carriesDisplayWherePresent() {
    final ValueSetExpansion expansion =
        ValueSetExpansion.fromResource(
            valueSet(entry(SNOMED, null, "22298006", "Myocardial infarction")), NO_LIMIT);

    assertEquals("Myocardial infarction", expansion.getMembers().get(0).getDisplay());
  }

  @Test
  void carriesInactiveAsNullFalseOrTrue() {
    final ValueSetExpansionContainsComponent absent = entry(SNOMED, "1");
    final ValueSetExpansionContainsComponent explicitFalse = entry(SNOMED, "2");
    explicitFalse.setInactive(false);
    final ValueSetExpansionContainsComponent explicitTrue = entry(SNOMED, "3");
    explicitTrue.setInactive(true);

    final ValueSetExpansion expansion =
        ValueSetExpansion.fromResource(valueSet(absent, explicitFalse, explicitTrue), NO_LIMIT);

    final List<ValueSetMember> members = expansion.getMembers();
    assertNull(members.get(0).getInactive());
    assertEquals(Boolean.FALSE, members.get(1).getInactive());
    assertEquals(Boolean.TRUE, members.get(2).getInactive());
  }

  @Test
  void rejectsExpansionCarryingOffset() {
    final ValueSet valueSet = valueSet(entry(SNOMED, "1"));
    valueSet.getExpansion().setOffset(0);

    final ValueSetExpansionException e =
        assertThrows(
            ValueSetExpansionException.class,
            () -> ValueSetExpansion.fromResource(valueSet, NO_LIMIT));

    assertTrue(e.getMessage().contains("offset"), e.getMessage());
  }

  @Test
  void rejectsTotalGreaterThanEntriesAtAllDepths() {
    final ValueSet valueSet = valueSet(abstractEntry(entry(SNOMED, "1"), entry(SNOMED, "2")));
    valueSet.getExpansion().setTotal(4);

    final ValueSetExpansionException e =
        assertThrows(
            ValueSetExpansionException.class,
            () -> ValueSetExpansion.fromResource(valueSet, NO_LIMIT));

    assertTrue(e.getMessage().contains("total 4"), e.getMessage());
    assertTrue(e.getMessage().contains("3 entries"), e.getMessage());
  }

  @Test
  void acceptsTotalEqualToEntriesAtAllDepthsCountingAbstract() {
    final ValueSet valueSet = valueSet(abstractEntry(entry(SNOMED, "1"), entry(SNOMED, "2")));
    valueSet.getExpansion().setTotal(3);

    final ValueSetExpansion expansion = ValueSetExpansion.fromResource(valueSet, NO_LIMIT);

    assertEquals(2, expansion.getMembers().size());
  }

  @Test
  void rejectsNonAbstractEntryWithoutCode() {
    final ValueSet valueSet = valueSet(entry(SNOMED, null));

    final ValueSetExpansionException e =
        assertThrows(
            ValueSetExpansionException.class,
            () -> ValueSetExpansion.fromResource(valueSet, NO_LIMIT));

    assertTrue(e.getMessage().contains("no code"), e.getMessage());
  }

  @Test
  void rejectsEntryWithCodeButNoSystem() {
    final ValueSet valueSet = valueSet(entry(null, "1"));

    final ValueSetExpansionException e =
        assertThrows(
            ValueSetExpansionException.class,
            () -> ValueSetExpansion.fromResource(valueSet, NO_LIMIT));

    assertTrue(e.getMessage().contains("no system"), e.getMessage());
  }

  @Test
  void rejectsResourceWithoutExpansion() {
    final ValueSet valueSet = new ValueSet();
    valueSet.setUrl(URL);

    final ValueSetExpansionException e =
        assertThrows(
            ValueSetExpansionException.class,
            () -> ValueSetExpansion.fromResource(valueSet, NO_LIMIT));

    assertTrue(e.getMessage().contains("no expansion"), e.getMessage());
  }

  @Test
  void acceptsExactlyMaxMembers() {
    final ValueSet valueSet = valueSet(entry(SNOMED, "1"), entry(SNOMED, "2"));

    final ValueSetExpansion expansion = ValueSetExpansion.fromResource(valueSet, 2);

    assertEquals(2, expansion.getMembers().size());
  }

  @Test
  void rejectsOneMoreThanMaxMembers() {
    final ValueSet valueSet = valueSet(entry(SNOMED, "1"), entry(SNOMED, "2"), entry(SNOMED, "3"));

    final ExpansionLimitExceededException e =
        assertThrows(
            ExpansionLimitExceededException.class,
            () -> ValueSetExpansion.fromResource(valueSet, 2));

    assertEquals(2, e.getLimit());
  }

  @Test
  void countsDeduplicatedMembersAgainstTheLimit() {
    final ValueSet valueSet = valueSet(entry(SNOMED, "1"), entry(SNOMED, "1"), entry(SNOMED, "2"));

    final ValueSetExpansion expansion = ValueSetExpansion.fromResource(valueSet, 2);

    assertEquals(2, expansion.getMembers().size());
  }

  @Test
  void carriesProvenanceFromResourceAndExpansion() {
    final ValueSet valueSet = valueSet(entry(SNOMED, "1"));
    valueSet.setVersion("2026");
    final ValueSetExpansionComponent expansion = valueSet.getExpansion();
    expansion.setIdentifier("urn:uuid:8e1c5a3e-3d0a-4f4b-9b6f-7f1f0a2b3c4d");
    expansion.setTimestampElement(new org.hl7.fhir.r4.model.DateTimeType("2026-09-22T10:00:00Z"));
    expansion
        .addParameter()
        .setName("version")
        .setValue(
            new UriType(SNOMED + "|http://snomed.info/sct/900000000000207008/version/20260101"));
    expansion.addParameter().setName("version").setValue(new UriType(ICD10 + "|2019"));
    expansion.addParameter().setName("count").setValue(new org.hl7.fhir.r4.model.IntegerType(1));

    final ValueSetExpansion result = ValueSetExpansion.fromResource(valueSet, NO_LIMIT);

    assertEquals(URL, result.getUrl());
    assertEquals("2026", result.getVersion());
    assertEquals("urn:uuid:8e1c5a3e-3d0a-4f4b-9b6f-7f1f0a2b3c4d", result.getIdentifier());
    assertEquals("2026-09-22T10:00:00Z", result.getTimestamp());
    assertEquals(
        List.of(
            SNOMED + "|http://snomed.info/sct/900000000000207008/version/20260101",
            ICD10 + "|2019"),
        result.getCodeSystemVersions());
  }

  @Test
  void carriesNullProvenanceWhereAbsent() {
    final ValueSetExpansion result =
        ValueSetExpansion.fromResource(valueSet(entry(SNOMED, "1")), NO_LIMIT);

    assertNull(result.getVersion());
    assertNull(result.getIdentifier());
    assertNull(result.getTimestamp());
    assertTrue(result.getCodeSystemVersions().isEmpty());
  }

  @Test
  void returnsEmptyMembersForEmptyExpansion() {
    final ValueSetExpansion result = ValueSetExpansion.fromResource(valueSet(), NO_LIMIT);

    assertTrue(result.getMembers().isEmpty());
    assertFalse(result.getMembers().contains(null));
  }
}
