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

package au.csiro.pathling.terminology;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * Tests for the implicit value set and concept map URL grammar of {@link ImplicitTerminologyUrls}.
 *
 * @author John Grimes
 */
class ImplicitTerminologyUrlsTest {

  @ParameterizedTest
  @ValueSource(
      strings = {
        "http://snomed.info/sct?fhir_vs",
        "http://snomed.info/sct?fhir_vs=isa/73211009",
        "http://snomed.info/sct?fhir_vs=refset/723264001",
        "http://snomed.info/sct?fhir_vs=ecl/%3C%3C73211009",
        "http://snomed.info/sct/900000000000207008/version/20230601?fhir_vs",
        "http://snomed.info/sct/900000000000207008/version/20230601?fhir_vs=isa/73211009",
        "http://snomed.info/xsct/900000000000207008/version/20230601?fhir_vs=isa/73211009",
        "http://fhir.org/VCL?v1=%28http%3A%2F%2Fsnomed.info%2Fsct%29inactive%3Dtrue"
      })
  void implicitValueSets(final String url) {
    assertTrue(ImplicitTerminologyUrls.isImplicitValueSet(url), url);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "http://example.org/ValueSet/cardiovascular-disease",
        "http://example.org/ValueSet/x?edition=au",
        "http://snomed.info/sct",
        "http://snomed.info/sct?fhir_cm=900000000000526001",
        "http://snomed.info/sct/900000000000207008/version/20230601?fhir_cm=900000000000526001",
        "http://example.org/sct?fhir_vs=isa/73211009"
      })
  void notImplicitValueSets(final String url) {
    assertFalse(ImplicitTerminologyUrls.isImplicitValueSet(url), url);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "http://snomed.info/sct?fhir_cm=900000000000526001",
        "http://snomed.info/sct/900000000000207008/version/20230601?fhir_cm=900000000000526001",
        "http://snomed.info/xsct/900000000000207008/version/20230601?fhir_cm=900000000000527005"
      })
  void implicitConceptMaps(final String url) {
    assertTrue(ImplicitTerminologyUrls.isImplicitConceptMap(url), url);
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "http://example.org/ConceptMap/sct-to-icd10",
        "http://example.org/sct?fhir_cm=900000000000526001",
        "http://snomed.info/sct?fhir_vs=isa/73211009",
        "http://snomed.info/sct",
        "http://fhir.org/VCL?v1=x"
      })
  void notImplicitConceptMaps(final String url) {
    assertFalse(ImplicitTerminologyUrls.isImplicitConceptMap(url), url);
  }
}
