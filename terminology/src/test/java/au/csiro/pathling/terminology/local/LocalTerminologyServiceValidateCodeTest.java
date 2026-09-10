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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import au.csiro.pathling.config.LocalTerminologyConfiguration;
import au.csiro.pathling.config.TerminologyConfiguration;
import au.csiro.pathling.config.TerminologyMode;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.test.NoNetworkExtension;
import au.csiro.pathling.test.Rf2Mini;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.hl7.fhir.r4.model.Coding;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Service-level tests for {@code member_of} in local mode over the rf2-mini store: SNOMED implicit
 * value set forms (all-concepts, reference set, is-a, ECL), the unknown-content fallback, null and
 * cross-system codings, and the guarantee that no network request is made.
 *
 * @author John Grimes
 */
@ExtendWith(NoNetworkExtension.class)
class LocalTerminologyServiceValidateCodeTest {

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

  private static String eclValueSet(final String ecl) {
    return Rf2Mini.SNOMED_URI + "?fhir_vs=ecl/" + URLEncoder.encode(ecl, StandardCharsets.UTF_8);
  }

  @Test
  void validatesEclMembership() {
    final String valueSet = eclValueSet("<< " + Rf2Mini.DIABETES);
    assertTrue(service.validateCode(valueSet, snomed(Rf2Mini.TYPE1_DIABETES)));
    assertTrue(service.validateCode(valueSet, snomed(Rf2Mini.DIABETES)));
    assertFalse(service.validateCode(valueSet, snomed(Rf2Mini.HYPERTENSION)));
  }

  @Test
  void validatesIsaImplicitValueSet() {
    final String valueSet = Rf2Mini.SNOMED_URI + "?fhir_vs=isa/" + Rf2Mini.DIABETES;
    assertTrue(service.validateCode(valueSet, snomed(Rf2Mini.TYPE2_WITH_COMPLICATION)));
    assertFalse(service.validateCode(valueSet, snomed(Rf2Mini.HYPERTENSION)));
  }

  @Test
  void validatesRefsetImplicitValueSet() {
    final String valueSet = Rf2Mini.SNOMED_URI + "?fhir_vs=refset/" + Rf2Mini.SIMPLE_REFSET;
    assertTrue(service.validateCode(valueSet, snomed(Rf2Mini.TYPE1_DIABETES)));
    assertFalse(service.validateCode(valueSet, snomed(Rf2Mini.DIABETES)));
  }

  @Test
  void validatesAllConceptsAndExcludesInactive() {
    final String valueSet = Rf2Mini.SNOMED_URI + "?fhir_vs";
    assertTrue(service.validateCode(valueSet, snomed(Rf2Mini.DIABETES)));
    // The inactive concept is not a member of the all-concepts implicit value set.
    assertFalse(service.validateCode(valueSet, snomed(Rf2Mini.DIABETES_INACTIVE)));
  }

  @Test
  void returnsFalseForUnknownCode() {
    assertFalse(service.validateCode(eclValueSet("<< " + Rf2Mini.DIABETES), snomed("999999999")));
  }

  @Test
  void returnsFalseForUnknownValueSet() {
    assertFalse(service.validateCode("http://loinc.org/vs", snomed(Rf2Mini.DIABETES)));
  }

  @Test
  void returnsFalseForCodingFromAnotherSystem() {
    final Coding loinc = new Coding().setSystem("http://loinc.org").setCode("1234-5");
    assertFalse(service.validateCode(eclValueSet("<< " + Rf2Mini.DIABETES), loinc));
  }

  @Test
  void returnsFalseForNullSystemOrCode() {
    assertFalse(service.validateCode(Rf2Mini.SNOMED_URI + "?fhir_vs", new Coding().setCode("x")));
    assertFalse(
        service.validateCode(
            Rf2Mini.SNOMED_URI + "?fhir_vs", new Coding().setSystem(Rf2Mini.SNOMED_URI)));
  }

  @Test
  void repeatedValidationUsesTheCachedExpansion() {
    final String valueSet = eclValueSet("<< " + Rf2Mini.DIABETES);
    // A second call over the same value set must return the same result from the cached expansion.
    assertTrue(service.validateCode(valueSet, snomed(Rf2Mini.TYPE2_DIABETES)));
    assertTrue(service.validateCode(valueSet, snomed(Rf2Mini.TYPE2_DIABETES)));
  }
}
