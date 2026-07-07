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

import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.test.FhirFixtures;
import au.csiro.pathling.test.NoNetworkExtension;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import org.hl7.fhir.r4.model.Coding;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

/**
 * Tests {@code member_of} evaluation of VCL implicit value sets over a FHIR CodeSystem: hierarchy
 * navigation from the CodeSystem's nesting, filters over declared scalar properties, regular
 * expression matching, and property existence, over the animal-species fixture.
 *
 * @author John Grimes
 */
@ExtendWith(NoNetworkExtension.class)
class VclCodeSystemEvaluationTest {

  private static TerminologyService service;

  @BeforeAll
  static void setUp() {
    service = FhirTerminologyFixture.service();
  }

  private static Coding species(final String code) {
    return new Coding().setSystem(FhirFixtures.ANIMAL_SPECIES).setCode(code);
  }

  private static String vcl(final String expression) {
    return "http://fhir.org/VCL?v1=" + URLEncoder.encode(expression, StandardCharsets.UTF_8);
  }

  private static String scoped(final String body) {
    return "(" + FhirFixtures.ANIMAL_SPECIES + ")" + body;
  }

  @Test
  void hierarchyFilterFromNesting() {
    final String valueSet = vcl(scoped("concept << mammal"));
    assertTrue(service.validateCode(valueSet, species(FhirFixtures.MAMMAL)));
    assertTrue(service.validateCode(valueSet, species(FhirFixtures.DOG)));
    assertFalse(service.validateCode(valueSet, species(FhirFixtures.SPARROW)));
  }

  @Test
  void codePropertyFilter() {
    final String valueSet = vcl(scoped("habitat = land"));
    assertTrue(service.validateCode(valueSet, species(FhirFixtures.DOG)));
    assertTrue(service.validateCode(valueSet, species(FhirFixtures.SPARROW)));
    assertFalse(service.validateCode(valueSet, species(FhirFixtures.WHALE)));
  }

  @Test
  void integerPropertyFilter() {
    final String valueSet = vcl(scoped("legs = 4"));
    assertTrue(service.validateCode(valueSet, species(FhirFixtures.DOG)));
    assertFalse(service.validateCode(valueSet, species(FhirFixtures.SPARROW)));
  }

  @Test
  void regexPropertyFilter() {
    final String valueSet = vcl(scoped("habitat / \"wat.*\""));
    assertTrue(service.validateCode(valueSet, species(FhirFixtures.WHALE)));
    assertFalse(service.validateCode(valueSet, species(FhirFixtures.DOG)));
  }

  @Test
  void propertyExistsFilter() {
    // The endangered property is declared only on dog and whale.
    final String valueSet = vcl(scoped("endangered ? true"));
    assertTrue(service.validateCode(valueSet, species(FhirFixtures.WHALE)));
    assertTrue(service.validateCode(valueSet, species(FhirFixtures.DOG)));
    assertFalse(service.validateCode(valueSet, species(FhirFixtures.CAT)));
  }
}
