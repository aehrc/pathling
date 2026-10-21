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

import static au.csiro.pathling.terminology.mock.MockTerminologyService.BETA_2_GLOBULIN_CODE;
import static au.csiro.pathling.terminology.mock.MockTerminologyService.LOINC_URI;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.hl7.fhir.r4.model.Coding;
import org.junit.jupiter.api.Test;

/**
 * Tests for value set membership through the {@link MockTerminologyService}, which stands in for a
 * terminology server in the Python and R test suites.
 *
 * @author John Grimes
 */
class MockTerminologyServiceValidateCodeTest {

  private static final String LOINC_VALUE_SET = "http://loinc.org/vs/LP14885-5";
  private static final String UNKNOWN_VALUE_SET = "http://example.org/ValueSet/unknown";

  private final MockTerminologyService service = new MockTerminologyService();

  @Test
  void validatesCodingAgainstKnownValueSet() {
    assertEquals(
        true,
        service.validateCode(LOINC_VALUE_SET, new Coding(LOINC_URI, BETA_2_GLOBULIN_CODE, null)));
    assertEquals(
        false, service.validateCode(LOINC_VALUE_SET, new Coding(LOINC_URI, "1234-5", null)));
  }

  @Test
  void validatesCodeByInferringSystem() {
    assertEquals(true, service.validateCodeInferringSystem(LOINC_VALUE_SET, BETA_2_GLOBULIN_CODE));
    assertEquals(false, service.validateCodeInferringSystem(LOINC_VALUE_SET, "1234-5"));
  }

  @Test
  void membershipIsUnknownForUnknownValueSet() {
    // As with a terminology server, an unknown value set leaves the membership unknown.
    assertNull(
        service.validateCode(UNKNOWN_VALUE_SET, new Coding(LOINC_URI, BETA_2_GLOBULIN_CODE, null)));
    assertNull(service.validateCodeInferringSystem(UNKNOWN_VALUE_SET, BETA_2_GLOBULIN_CODE));
  }
}
