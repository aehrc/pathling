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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.terminology.expand.ExpansionLimitExceededException;
import au.csiro.pathling.terminology.expand.ValueSetExpansion;
import au.csiro.pathling.terminology.expand.ValueSetExpansionException;
import au.csiro.pathling.terminology.expand.ValueSetMember;
import java.util.List;
import org.hl7.fhir.r4.model.ValueSet;
import org.junit.jupiter.api.Test;

/**
 * Tests for value set expansion through the {@link MockTerminologyService}.
 *
 * @author John Grimes
 */
class MockTerminologyServiceExpandTest {

  private static final String LOINC_VALUE_SET = "http://loinc.org/vs/LP14885-5";
  private static final int NO_LIMIT = Integer.MAX_VALUE;

  private final MockTerminologyService service = new MockTerminologyService();

  @Test
  void returnsConfiguredCodingsAsMembers() {
    final ValueSetExpansion expansion =
        service.expand(LOINC_VALUE_SET, null, NO_LIMIT).orElseThrow();

    assertEquals(LOINC_VALUE_SET, expansion.getUrl());
    assertEquals(
        List.of(
            new ValueSetMember(
                MockTerminologyService.LOINC_URI,
                null,
                MockTerminologyService.BETA_2_GLOBULIN_CODE,
                null,
                null)),
        expansion.getMembers());
  }

  @Test
  void returnsEmptyForUnknownValueSet() {
    assertTrue(service.expand("http://example.org/ValueSet/unknown", null, NO_LIMIT).isEmpty());
  }

  @Test
  void rejectsMembershipOverTheLimit() {
    assertThrows(
        ExpansionLimitExceededException.class, () -> service.expand(LOINC_VALUE_SET, null, 0));
  }

  @Test
  void flattensSuppliedExpansion() {
    final ValueSet supplied = new ValueSet();
    supplied.setUrl("http://example.org/ValueSet/supplied");
    supplied
        .getExpansion()
        .addContains()
        .setAbstract(true)
        .addContains()
        .setSystem(MockTerminologyService.SNOMED_URI)
        .setCode("368529001")
        .setDisplay("Left");

    final ValueSetExpansion expansion = service.expand(supplied, NO_LIMIT);

    assertEquals("http://example.org/ValueSet/supplied", expansion.getUrl());
    assertEquals(
        List.of(
            new ValueSetMember(MockTerminologyService.SNOMED_URI, null, "368529001", "Left", null)),
        expansion.getMembers());
  }

  @Test
  void rejectsSuppliedComposeOnly() {
    final ValueSet supplied = new ValueSet();
    supplied.setUrl("http://example.org/ValueSet/supplied");
    supplied.getCompose().addInclude().setSystem(MockTerminologyService.SNOMED_URI);

    assertThrows(ValueSetExpansionException.class, () -> service.expand(supplied, NO_LIMIT));
  }
}
