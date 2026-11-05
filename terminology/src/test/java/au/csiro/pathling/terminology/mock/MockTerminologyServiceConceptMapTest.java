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

package au.csiro.pathling.terminology.mock;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.terminology.conceptmap.ConceptMapContent;
import au.csiro.pathling.terminology.conceptmap.ConceptMapRelationship;
import au.csiro.pathling.terminology.conceptmap.ConceptMapping;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Tests for reading a concept map through the {@link MockTerminologyService}.
 *
 * @author John Grimes
 */
class MockTerminologyServiceConceptMapTest {

  private static final String CONCEPT_MAP = "http://snomed.info/sct?fhir_cm=100";
  private static final int NO_LIMIT = Integer.MAX_VALUE;

  private final MockTerminologyService service = new MockTerminologyService();

  @Test
  void returnsForwardMappingsAsRowsWithConvertedRelationships() {
    final ConceptMapContent content =
        service.readConceptMap(CONCEPT_MAP, null, NO_LIMIT).orElseThrow();

    assertEquals(CONCEPT_MAP, content.getUrl());
    assertNull(content.getVersion());
    assertEquals(
        List.of(
            new ConceptMapping(
                MockTerminologyService.SNOMED_URI,
                null,
                "368529001",
                null,
                MockTerminologyService.SNOMED_URI,
                null,
                "368529002",
                null,
                ConceptMapRelationship.EQUIVALENT),
            new ConceptMapping(
                MockTerminologyService.SNOMED_URI,
                null,
                "368529001",
                null,
                MockTerminologyService.LOINC_URI,
                null,
                "55916-3",
                null,
                ConceptMapRelationship.RELATED_TO)),
        content.getMappings());
  }

  @Test
  void returnsEmptyForUnknownConceptMap() {
    assertTrue(
        service.readConceptMap("http://example.org/ConceptMap/unknown", null, NO_LIMIT).isEmpty());
  }
}
