/*
 * Copyright © 2018-2025 Commonwealth Scientific and Industrial Research
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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

import au.csiro.pathling.fhirpath.encoding.ImmutableCoding;
import au.csiro.pathling.terminology.validatecode.ValidateCodeParameters;
import org.hl7.fhir.r4.model.Coding;
import org.junit.jupiter.api.Test;

public class ParameterComparabilityTest {

  @Test
  void validateCode() {
    final ValidateCodeParameters parameters1 = new ValidateCodeParameters(
        "http://snomed.info/sct?fhir_vs",
        ImmutableCoding.of("http://snomed.info/sct", "1032491000168100",
            "Atomoxetine (GxP) 100 mg capsule, 28"));

    // These parameters have a different display in the coding.
    final ValidateCodeParameters parameters2 = new ValidateCodeParameters(
        "http://snomed.info/sct?fhir_vs",
        ImmutableCoding.of("http://snomed.info/sct", "1032491000168100",
            "Atomoxetine"));

    // These parameters have a different version in the coding.
    final Coding coding3 = new Coding("http://snomed.info/sct", "1032491000168100",
        "Atomoxetine (GxP) 100 mg capsule, 28");
    coding3.setVersion("http://snomed.info/sct/32506021000036107/version/20220930");
    final ValidateCodeParameters parameters3 = new ValidateCodeParameters(
        "http://snomed.info/sct?fhir_vs",
        ImmutableCoding.of(coding3));

    // These parameters have a different user selected in the coding.
    final Coding coding4 = new Coding("http://snomed.info/sct", "1032491000168100",
        "Atomoxetine (GxP) 100 mg capsule, 28");
    coding3.setUserSelected(true);
    final ValidateCodeParameters parameters4 = new ValidateCodeParameters(
        "http://snomed.info/sct?fhir_vs",
        ImmutableCoding.of(coding4));

    // These parameters have a different value set URL.
    final ValidateCodeParameters parameters5 = new ValidateCodeParameters(
        "http://aehrc.csiro.au/some-other-value-set",
        ImmutableCoding.of("http://snomed.info/sct", "1032491000168100",
            "Atomoxetine (GxP) 100 mg capsule, 28"));

    assertEquals(parameters1, parameters2);
    assertEquals(parameters1.hashCode(), parameters2.hashCode());

    assertNotEquals(parameters1, parameters3);
    assertNotEquals(parameters1.hashCode(), parameters3.hashCode());

    assertEquals(parameters1, parameters4);
    assertEquals(parameters1.hashCode(), parameters4.hashCode());

    assertNotEquals(parameters1, parameters5);
    assertNotEquals(parameters1.hashCode(), parameters5.hashCode());
  }

}
