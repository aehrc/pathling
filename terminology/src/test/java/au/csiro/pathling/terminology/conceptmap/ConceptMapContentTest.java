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

package au.csiro.pathling.terminology.conceptmap;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;
import java.util.List;
import org.hl7.fhir.r4.model.ConceptMap;
import org.hl7.fhir.r4.model.ConceptMap.ConceptMapGroupComponent;
import org.hl7.fhir.r4.model.ConceptMap.ConceptMapGroupUnmappedMode;
import org.hl7.fhir.r4.model.ConceptMap.SourceElementComponent;
import org.hl7.fhir.r4.model.ConceptMap.TargetElementComponent;
import org.hl7.fhir.r4.model.Enumerations.ConceptMapEquivalence;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

/**
 * Tests for {@link ConceptMapContent}: the R4 equivalence conversion, the no-mapping row, the
 * deduplication and first-display rule, the version split, the validation faults, the cap, and the
 * {@code fromMappings} factory.
 *
 * @author John Grimes
 */
class ConceptMapContentTest {

  private static final String URL = "http://example.org/ConceptMap/sct-to-icd10";
  private static final String SNOMED = "http://snomed.info/sct";
  private static final String ICD10 = "http://hl7.org/fhir/sid/icd-10";
  private static final int NO_LIMIT = Integer.MAX_VALUE;

  @Nonnull
  private static ConceptMap conceptMap() {
    final ConceptMap conceptMap = new ConceptMap();
    conceptMap.setUrl(URL);
    conceptMap.setVersion("2026");
    return conceptMap;
  }

  @Nonnull
  private static ConceptMapGroupComponent group(
      @Nonnull final ConceptMap conceptMap,
      @Nullable final String source,
      @Nullable final String target) {
    final ConceptMapGroupComponent group = conceptMap.addGroup();
    if (source != null) {
      group.setSource(source);
    }
    if (target != null) {
      group.setTarget(target);
    }
    return group;
  }

  @Nonnull
  private static SourceElementComponent element(
      @Nonnull final ConceptMapGroupComponent group,
      @Nullable final String code,
      @Nullable final String display) {
    final SourceElementComponent element = group.addElement();
    if (code != null) {
      element.setCode(code);
    }
    if (display != null) {
      element.setDisplay(display);
    }
    return element;
  }

  @Nonnull
  private static TargetElementComponent target(
      @Nonnull final SourceElementComponent element,
      @Nullable final String code,
      @Nullable final String display,
      @Nullable final ConceptMapEquivalence equivalence) {
    final TargetElementComponent target = element.addTarget();
    if (code != null) {
      target.setCode(code);
    }
    if (display != null) {
      target.setDisplay(display);
    }
    if (equivalence != null) {
      target.setEquivalence(equivalence);
    }
    return target;
  }

  /** Builds a map with one group from SNOMED CT to ICD-10 and one element with one target. */
  @Nonnull
  private static ConceptMap singleMapping(
      @Nullable final String targetCode, @Nonnull final ConceptMapEquivalence equivalence) {
    final ConceptMap conceptMap = conceptMap();
    final ConceptMapGroupComponent group = group(conceptMap, SNOMED, ICD10);
    group.setTargetVersion("2019");
    target(
        element(group, "22298006", "Myocardial infarction"),
        targetCode,
        "Acute myocardial infarction",
        equivalence);
    return conceptMap;
  }

  @Nonnull
  private static ConceptMapping mapping(
      @Nullable final String targetCode,
      @Nullable final String targetDisplay,
      @Nullable final String relationship) {
    return new ConceptMapping(
        SNOMED,
        null,
        "22298006",
        "Myocardial infarction",
        ICD10,
        "2019",
        targetCode,
        targetDisplay,
        relationship);
  }

  @ParameterizedTest
  @CsvSource({
    "equal,equivalent",
    "equivalent,equivalent",
    "wider,source-is-narrower-than-target",
    "subsumes,source-is-narrower-than-target",
    "narrower,source-is-broader-than-target",
    "specializes,source-is-broader-than-target",
    "relatedto,related-to",
    "inexact,related-to",
    "disjoint,not-related-to",
    "unmatched,"
  })
  void convertsEquivalenceToRelationship(
      @Nonnull final String equivalence, @Nullable final String relationship) {
    final ConceptMap conceptMap = singleMapping("I21", ConceptMapEquivalence.fromCode(equivalence));

    final ConceptMapContent content = ConceptMapContent.fromResource(conceptMap, NO_LIMIT);

    final ConceptMapping expected =
        relationship == null
            ? mapping(null, null, null)
            : mapping("I21", "Acute myocardial infarction", relationship);
    assertEquals(List.of(expected), content.getMappings());
  }

  @Test
  void unmatchedWithoutCodeIsNoMappingRow() {
    final ConceptMap conceptMap = singleMapping(null, ConceptMapEquivalence.UNMATCHED);

    final ConceptMapContent content = ConceptMapContent.fromResource(conceptMap, NO_LIMIT);

    assertEquals(List.of(mapping(null, null, null)), content.getMappings());
    final ConceptMapping row = content.getMappings().get(0);
    assertEquals(ICD10, row.getTargetSystem());
    assertEquals("2019", row.getTargetVersion());
    assertNull(row.getTargetCode());
    assertNull(row.getTargetDisplay());
    assertNull(row.getRelationship());
  }

  @Test
  void twoUnmatchedTargetsAreOneRow() {
    final ConceptMap conceptMap = conceptMap();
    final SourceElementComponent element =
        element(group(conceptMap, SNOMED, ICD10), "22298006", "Myocardial infarction");
    target(element, null, null, ConceptMapEquivalence.UNMATCHED);
    target(element, "I21", null, ConceptMapEquivalence.UNMATCHED);

    final ConceptMapContent content = ConceptMapContent.fromResource(conceptMap, NO_LIMIT);

    assertEquals(1, content.getMappings().size());
    assertNull(content.getMappings().get(0).getTargetCode());
  }

  @Test
  void unmatchedAndMappedTargetsAreTwoRows() {
    final ConceptMap conceptMap = conceptMap();
    final SourceElementComponent element =
        element(group(conceptMap, SNOMED, ICD10), "22298006", "Myocardial infarction");
    target(element, null, null, ConceptMapEquivalence.UNMATCHED);
    target(element, "I21", "Acute myocardial infarction", ConceptMapEquivalence.EQUIVALENT);

    final ConceptMapContent content = ConceptMapContent.fromResource(conceptMap, NO_LIMIT);

    assertEquals(2, content.getMappings().size());
    assertNull(content.getMappings().get(0).getTargetCode());
    assertEquals("I21", content.getMappings().get(1).getTargetCode());
    assertEquals(ConceptMapRelationship.EQUIVALENT, content.getMappings().get(1).getRelationship());
  }

  @Test
  void duplicatesKeepFirstDisplays() {
    final ConceptMap conceptMap = conceptMap();
    final ConceptMapGroupComponent group = group(conceptMap, SNOMED, ICD10);
    final SourceElementComponent first = element(group, "22298006", "First source display");
    target(first, "I21", "First target display", ConceptMapEquivalence.EQUIVALENT);
    target(first, "I21", "Repeated target display", ConceptMapEquivalence.EQUIVALENT);
    final SourceElementComponent repeated = element(group, "22298006", "Repeated source display");
    target(repeated, "I21", "Repeated target display", ConceptMapEquivalence.EQUIVALENT);

    final ConceptMapContent content = ConceptMapContent.fromResource(conceptMap, NO_LIMIT);

    assertEquals(1, content.getMappings().size());
    final ConceptMapping row = content.getMappings().get(0);
    assertEquals("First source display", row.getSourceDisplay());
    assertEquals("First target display", row.getTargetDisplay());
  }

  @Test
  void splitsVersionSuffixWhereVersionElementIsAbsent() {
    final ConceptMap conceptMap = conceptMap();
    final ConceptMapGroupComponent group = group(conceptMap, SNOMED + "|20230601", ICD10 + "|2019");
    target(element(group, "22298006", null), "I21", null, ConceptMapEquivalence.EQUIVALENT);

    final ConceptMapping row =
        ConceptMapContent.fromResource(conceptMap, NO_LIMIT).getMappings().get(0);

    assertEquals(SNOMED, row.getSourceSystem());
    assertEquals("20230601", row.getSourceVersion());
    assertEquals(ICD10, row.getTargetSystem());
    assertEquals("2019", row.getTargetVersion());
  }

  @Test
  void takesUriAsWrittenWhereVersionElementIsPresent() {
    final ConceptMap conceptMap = conceptMap();
    final ConceptMapGroupComponent group = group(conceptMap, SNOMED + "|20230601", ICD10 + "|2019");
    group.setSourceVersion("20240601");
    group.setTargetVersion("2020");
    target(element(group, "22298006", null), "I21", null, ConceptMapEquivalence.EQUIVALENT);

    final ConceptMapping row =
        ConceptMapContent.fromResource(conceptMap, NO_LIMIT).getMappings().get(0);

    assertEquals(SNOMED + "|20230601", row.getSourceSystem());
    assertEquals("20240601", row.getSourceVersion());
    assertEquals(ICD10 + "|2019", row.getTargetSystem());
    assertEquals("2020", row.getTargetVersion());
  }

  @Test
  void groupWithoutTargetHasNullTargetSystemAndVersion() {
    final ConceptMap conceptMap = conceptMap();
    final ConceptMapGroupComponent group = group(conceptMap, SNOMED, null);
    target(element(group, "22298006", null), "I21", null, ConceptMapEquivalence.EQUIVALENT);

    final ConceptMapping row =
        ConceptMapContent.fromResource(conceptMap, NO_LIMIT).getMappings().get(0);

    assertNull(row.getTargetSystem());
    assertNull(row.getTargetVersion());
    assertEquals("I21", row.getTargetCode());
  }

  @Test
  void ignoresUnmapped() {
    final ConceptMap conceptMap = conceptMap();
    final ConceptMapGroupComponent group = group(conceptMap, SNOMED, ICD10);
    group.getUnmapped().setMode(ConceptMapGroupUnmappedMode.FIXED).setCode("R69");

    final ConceptMapContent content = ConceptMapContent.fromResource(conceptMap, NO_LIMIT);

    assertTrue(content.getMappings().isEmpty());
  }

  @Test
  void elementWithoutTargetsContributesNoRows() {
    final ConceptMap conceptMap = conceptMap();
    element(group(conceptMap, SNOMED, ICD10), "22298006", "Myocardial infarction");

    assertTrue(ConceptMapContent.fromResource(conceptMap, NO_LIMIT).getMappings().isEmpty());
  }

  @Test
  void noGroupsIsEmpty() {
    final ConceptMapContent content = ConceptMapContent.fromResource(conceptMap(), NO_LIMIT);

    assertTrue(content.getMappings().isEmpty());
    assertEquals(URL, content.getUrl());
    assertEquals("2026", content.getVersion());
  }

  @Test
  void rejectsGroupWithoutSource() {
    final ConceptMap conceptMap = conceptMap();
    target(
        element(group(conceptMap, null, ICD10), "22298006", null),
        "I21",
        null,
        ConceptMapEquivalence.EQUIVALENT);

    final ConceptMapContentException e =
        assertThrows(
            ConceptMapContentException.class,
            () -> ConceptMapContent.fromResource(conceptMap, NO_LIMIT));

    assertEquals("a group has no source system", e.getMessage());
  }

  @Test
  void rejectsDependsOn() {
    final ConceptMap conceptMap = singleMapping("I21", ConceptMapEquivalence.EQUIVALENT);
    conceptMap
        .getGroupFirstRep()
        .getElementFirstRep()
        .getTargetFirstRep()
        .addDependsOn()
        .setProperty("laterality")
        .setValue("left");

    final ConceptMapContentException e =
        assertThrows(
            ConceptMapContentException.class,
            () -> ConceptMapContent.fromResource(conceptMap, NO_LIMIT));

    assertEquals(
        "the mapping for source code '22298006' depends on other elements (dependsOn)",
        e.getMessage());
  }

  @Test
  void rejectsProduct() {
    final ConceptMap conceptMap = singleMapping("I21", ConceptMapEquivalence.EQUIVALENT);
    conceptMap
        .getGroupFirstRep()
        .getElementFirstRep()
        .getTargetFirstRep()
        .addProduct()
        .setProperty("laterality")
        .setValue("left");

    final ConceptMapContentException e =
        assertThrows(
            ConceptMapContentException.class,
            () -> ConceptMapContent.fromResource(conceptMap, NO_LIMIT));

    assertEquals("the mapping for source code '22298006' has products", e.getMessage());
  }

  @Test
  void rejectsElementWithoutCode() {
    final ConceptMap conceptMap = conceptMap();
    target(
        element(group(conceptMap, SNOMED, ICD10), null, "No code"),
        "I21",
        null,
        ConceptMapEquivalence.EQUIVALENT);

    final ConceptMapContentException e =
        assertThrows(
            ConceptMapContentException.class,
            () -> ConceptMapContent.fromResource(conceptMap, NO_LIMIT));

    assertEquals("an element has no code", e.getMessage());
  }

  @Test
  void rejectsMappedTargetWithoutCode() {
    final ConceptMap conceptMap = singleMapping(null, ConceptMapEquivalence.EQUIVALENT);

    final ConceptMapContentException e =
        assertThrows(
            ConceptMapContentException.class,
            () -> ConceptMapContent.fromResource(conceptMap, NO_LIMIT));

    assertEquals("the mapping for source code '22298006' has no target code", e.getMessage());
  }

  @Test
  void rejectsTargetWithoutEquivalence() {
    final ConceptMap conceptMap = conceptMap();
    target(element(group(conceptMap, SNOMED, ICD10), "22298006", null), "I21", null, null);

    final ConceptMapContentException e =
        assertThrows(
            ConceptMapContentException.class,
            () -> ConceptMapContent.fromResource(conceptMap, NO_LIMIT));

    assertEquals("the mapping for source code '22298006' has no equivalence", e.getMessage());
  }

  @Test
  void reportsFaultBeyondTheCap() {
    final ConceptMap conceptMap = conceptMap();
    final ConceptMapGroupComponent group = group(conceptMap, SNOMED, ICD10);
    target(element(group, "1", null), "A", null, ConceptMapEquivalence.EQUIVALENT);
    target(element(group, "2", null), "B", null, ConceptMapEquivalence.EQUIVALENT);
    target(element(group, "3", null), "C", null, null);

    final ConceptMapContentException e =
        assertThrows(
            ConceptMapContentException.class, () -> ConceptMapContent.fromResource(conceptMap, 1));

    assertEquals("the mapping for source code '3' has no equivalence", e.getMessage());
  }

  @Test
  void acceptsExactlyMaxMappingsAndRejectsOneMore() {
    final ConceptMap conceptMap = conceptMap();
    final ConceptMapGroupComponent group = group(conceptMap, SNOMED, ICD10);
    target(element(group, "1", null), "A", null, ConceptMapEquivalence.EQUIVALENT);
    target(element(group, "2", null), "B", null, ConceptMapEquivalence.EQUIVALENT);
    target(element(group, "3", null), "C", null, ConceptMapEquivalence.EQUIVALENT);

    assertEquals(3, ConceptMapContent.fromResource(conceptMap, 3).getMappings().size());
    final ConceptMapLimitExceededException e =
        assertThrows(
            ConceptMapLimitExceededException.class,
            () -> ConceptMapContent.fromResource(conceptMap, 2));
    assertEquals(2, e.getLimit());
  }

  @Test
  void fromMappingsDeduplicatesAndCaps() {
    final ConceptMapping first = mapping("I21", "First", ConceptMapRelationship.EQUIVALENT);
    final ConceptMapping repeated = mapping("I21", "Repeated", ConceptMapRelationship.EQUIVALENT);
    final ConceptMapping other = mapping("I22", null, ConceptMapRelationship.EQUIVALENT);

    final ConceptMapContent content =
        ConceptMapContent.fromMappings(URL, null, List.of(first, repeated, other), 2);

    assertEquals(URL, content.getUrl());
    assertNull(content.getVersion());
    assertEquals(List.of(first, other), content.getMappings());
    assertEquals("First", content.getMappings().get(0).getTargetDisplay());
    final ConceptMapLimitExceededException e =
        assertThrows(
            ConceptMapLimitExceededException.class,
            () -> ConceptMapContent.fromMappings(URL, null, List.of(first, repeated, other), 1));
    assertEquals(1, e.getLimit());
  }

  @Test
  void rejectsResourceWithoutUrl() {
    final ConceptMap conceptMap = new ConceptMap();

    assertThrows(
        IllegalArgumentException.class, () -> ConceptMapContent.fromResource(conceptMap, NO_LIMIT));
  }

  @Test
  void carriesUrlAndVersionFromResource() {
    final ConceptMap conceptMap = singleMapping("I21", ConceptMapEquivalence.EQUIVALENT);

    final ConceptMapContent content = ConceptMapContent.fromResource(conceptMap, NO_LIMIT);

    assertEquals(URL, content.getUrl());
    assertEquals("2026", content.getVersion());
  }

  @Test
  void versionIsNullWhereResourceHasNone() {
    final ConceptMap conceptMap = singleMapping("I21", ConceptMapEquivalence.EQUIVALENT);
    conceptMap.setVersionElement(null);

    assertNull(ConceptMapContent.fromResource(conceptMap, NO_LIMIT).getVersion());
  }
}
