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

package au.csiro.pathling.terminology.local;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.terminology.conceptmap.ConceptMapContent;
import au.csiro.pathling.terminology.conceptmap.ConceptMapContentException;
import au.csiro.pathling.terminology.conceptmap.ConceptMapVersionException;
import au.csiro.pathling.terminology.conceptmap.ConceptMapping;
import au.csiro.pathling.test.NoNetworkExtension;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Service-level tests for reading explicit concept maps in local mode: pinned and unpinned version
 * selection over imported ConceptMap resources, agreement with {@link
 * ConceptMapContent#fromResource}, unknown URLs, undeterminable versions and unrepresentable
 * content.
 *
 * @author John Grimes
 */
@ExtendWith(NoNetworkExtension.class)
class LocalTerminologyServiceConceptMapTest {

  private static final int NO_LIMIT = Integer.MAX_VALUE;

  private static TerminologyService service;

  @BeforeAll
  static void setUp() {
    service = ConceptMapTerminologyFixture.service();
  }

  @Test
  void pinnedVersionsYieldTheirOwnRows() {
    final ConceptMapContent content2025 =
        service
            .readConceptMap(ConceptMapTerminologyFixture.SCT_TO_ICD10, "2025", NO_LIMIT)
            .orElseThrow();
    final ConceptMapContent content2026 =
        service
            .readConceptMap(ConceptMapTerminologyFixture.SCT_TO_ICD10, "2026", NO_LIMIT)
            .orElseThrow();

    assertEquals("2025", content2025.getVersion());
    assertEquals("2026", content2026.getVersion());
    assertEquals(
        List.of(ConceptMapTerminologyFixture.EDITED_2025_TARGET),
        targetCodesOf(content2025, "73211009"));
    assertEquals(List.of("E14"), targetCodesOf(content2026, "73211009"));
  }

  @Test
  void unpinnedSelectsTheLatestVersion() {
    final ConceptMapContent content =
        service
            .readConceptMap(ConceptMapTerminologyFixture.SCT_TO_ICD10, null, NO_LIMIT)
            .orElseThrow();

    assertEquals("2026", content.getVersion());
    assertEquals(List.of("E14"), targetCodesOf(content, "73211009"));
  }

  @Test
  void contentEqualsConversionOfTheImportedResource() {
    assertEquals(
        ConceptMapContent.fromResource(ConceptMapTerminologyFixture.conceptMap2026(), NO_LIMIT),
        service
            .readConceptMap(ConceptMapTerminologyFixture.SCT_TO_ICD10, null, NO_LIMIT)
            .orElseThrow());
    assertEquals(
        ConceptMapContent.fromResource(ConceptMapTerminologyFixture.conceptMap2025(), NO_LIMIT),
        service
            .readConceptMap(ConceptMapTerminologyFixture.SCT_TO_ICD10, "2025", NO_LIMIT)
            .orElseThrow());
  }

  @Test
  void returnsEmptyForUnknownUrl() {
    assertTrue(
        service.readConceptMap("http://example.org/ConceptMap/missing", null, NO_LIMIT).isEmpty());
  }

  @Test
  void returnsEmptyForUnknownPinnedVersion() {
    assertTrue(
        service
            .readConceptMap(ConceptMapTerminologyFixture.SCT_TO_ICD10, "2024", NO_LIMIT)
            .isEmpty());
  }

  @Test
  void rejectsUndeterminableVersionOrder() {
    final ConceptMapVersionException e =
        assertThrows(
            ConceptMapVersionException.class,
            () -> service.readConceptMap(ConceptMapTerminologyFixture.AMBIGUOUS, null, NO_LIMIT));

    assertTrue(e.getMessage().contains(ConceptMapTerminologyFixture.AMBIGUOUS), e.getMessage());
  }

  @Test
  void rejectsMapWithDependsOn() {
    final ConceptMapContentException e =
        assertThrows(
            ConceptMapContentException.class,
            () -> service.readConceptMap(ConceptMapTerminologyFixture.DEPENDS_ON, null, NO_LIMIT));

    assertTrue(e.getMessage().contains("dependsOn"), e.getMessage());
  }

  private static List<String> targetCodesOf(
      final ConceptMapContent content, final String sourceCode) {
    return content.getMappings().stream()
        .filter(mapping -> sourceCode.equals(mapping.getSourceCode()))
        .map(ConceptMapping::getTargetCode)
        .toList();
  }
}
