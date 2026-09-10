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

import au.csiro.pathling.config.LocalTerminologyConfiguration;
import au.csiro.pathling.config.TerminologyConfiguration;
import au.csiro.pathling.config.TerminologyMode;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.test.NoNetworkExtension;
import au.csiro.pathling.test.Rf2Mini;
import java.util.Map;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.codesystems.ConceptSubsumptionOutcome;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Service-level tests for {@code subsumes} in local mode over the rf2-mini store: the four
 * subsumption outcomes computed from the stored is-a hierarchy, the cross-system short-circuit,
 * equal-coding equivalence, version-qualified codings, unknown codes, and the guarantee that no
 * network request is made.
 *
 * @author John Grimes
 */
@ExtendWith(NoNetworkExtension.class)
class LocalTerminologyServiceSubsumesTest {

  private static TerminologyService service;

  @BeforeAll
  static void setUp() {
    final TerminologyConfiguration configuration =
        TerminologyConfiguration.builder()
            .mode(TerminologyMode.LOCAL)
            .local(
                LocalTerminologyConfiguration.builder()
                    .storagePath(LocalTerminologyFixture.storagePath())
                    .build())
            .build();
    service = new LocalTerminologyService(configuration, Map.of());
  }

  private static Coding snomed(final String code) {
    return new Coding().setSystem(Rf2Mini.SNOMED_URI).setCode(code);
  }

  @Test
  void ancestorSubsumesDescendant() {
    // DIABETES is a direct parent of TYPE2_DIABETES.
    assertEquals(
        ConceptSubsumptionOutcome.SUBSUMES,
        service.subsumes(snomed(Rf2Mini.DIABETES), snomed(Rf2Mini.TYPE2_DIABETES)));
  }

  @Test
  void ancestorSubsumesTransitiveDescendant() {
    // DISORDER -> DIABETES -> TYPE2_DIABETES -> TYPE2_WITH_COMPLICATION spans several levels.
    assertEquals(
        ConceptSubsumptionOutcome.SUBSUMES,
        service.subsumes(snomed(Rf2Mini.DISORDER), snomed(Rf2Mini.TYPE2_WITH_COMPLICATION)));
  }

  @Test
  void descendantIsSubsumedByAncestor() {
    assertEquals(
        ConceptSubsumptionOutcome.SUBSUMEDBY,
        service.subsumes(snomed(Rf2Mini.TYPE2_DIABETES), snomed(Rf2Mini.DIABETES)));
  }

  @Test
  void equalCodingsAreEquivalent() {
    assertEquals(
        ConceptSubsumptionOutcome.EQUIVALENT,
        service.subsumes(snomed(Rf2Mini.DIABETES), snomed(Rf2Mini.DIABETES)));
  }

  @Test
  void unrelatedCodingsAreNotSubsumed() {
    // DIABETES and HYPERTENSION are siblings under DISORDER; neither subsumes the other.
    assertEquals(
        ConceptSubsumptionOutcome.NOTSUBSUMED,
        service.subsumes(snomed(Rf2Mini.DIABETES), snomed(Rf2Mini.HYPERTENSION)));
  }

  @Test
  void differentSystemsAreNotSubsumed() {
    final Coding loinc = new Coding().setSystem("http://loinc.org").setCode("1234-5");
    assertEquals(
        ConceptSubsumptionOutcome.NOTSUBSUMED, service.subsumes(snomed(Rf2Mini.DIABETES), loinc));
  }

  @Test
  void unknownCodeIsNotSubsumed() {
    assertEquals(
        ConceptSubsumptionOutcome.NOTSUBSUMED,
        service.subsumes(snomed(Rf2Mini.DIABETES), snomed("999999999")));
  }

  @Test
  void versionQualifiedCodingsResolveToTheRequestedEdition() {
    final Coding ancestor = snomed(Rf2Mini.DIABETES).setVersion(Rf2Mini.VERSION_20230601);
    final Coding descendant = snomed(Rf2Mini.TYPE2_DIABETES).setVersion(Rf2Mini.VERSION_20230601);
    assertEquals(ConceptSubsumptionOutcome.SUBSUMES, service.subsumes(ancestor, descendant));
  }

  @Test
  void nullSystemOrCodeIsNotSubsumed() {
    assertEquals(
        ConceptSubsumptionOutcome.NOTSUBSUMED,
        service.subsumes(new Coding().setCode(Rf2Mini.DIABETES), snomed(Rf2Mini.TYPE2_DIABETES)));
    assertEquals(
        ConceptSubsumptionOutcome.NOTSUBSUMED,
        service.subsumes(snomed(Rf2Mini.DIABETES), new Coding().setSystem(Rf2Mini.SNOMED_URI)));
  }
}
