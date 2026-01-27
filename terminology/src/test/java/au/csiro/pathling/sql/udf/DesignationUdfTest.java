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

package au.csiro.pathling.sql.udf;

import static au.csiro.pathling.fhirpath.encoding.CodingSchema.encode;
import static au.csiro.pathling.test.helpers.FhirMatchers.deepEq;
import static au.csiro.pathling.test.helpers.TerminologyServiceHelpers.setupLookup;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import au.csiro.pathling.test.AbstractTerminologyTestBase;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DesignationUdfTest extends AbstractTerminologyTestBase {

  private static final String[] EMPTY = new String[0];

  private DesignationUdf designationUdf;
  private TerminologyService terminologyService;

  @BeforeEach
  void setUp() {
    terminologyService = mock(TerminologyService.class);
    final TerminologyServiceFactory terminologyServiceFactory =
        mock(TerminologyServiceFactory.class);
    when(terminologyServiceFactory.build()).thenReturn(terminologyService);
    designationUdf = new DesignationUdf(terminologyServiceFactory);
  }

  @Test
  void testReturnsNullWhenNullCoding() {
    assertNull(designationUdf.call(null, encode(CODING_D), null));
    verifyNoMoreInteractions(terminologyService);
  }

  @Test
  void testResultEmptyWhenInvalidUse() {
    assertArrayEquals(EMPTY, designationUdf.call(encode(CODING_A), encode(INVALID_CODING_0), null));
    assertArrayEquals(EMPTY, designationUdf.call(encode(CODING_B), encode(INVALID_CODING_1), "en"));
    assertArrayEquals(EMPTY, designationUdf.call(encode(CODING_C), encode(INVALID_CODING_2), "fr"));
    verifyNoMoreInteractions(terminologyService);
  }

  @Test
  void testResultEmptyIfNoDesignations() {
    assertArrayEquals(EMPTY, designationUdf.call(encode(CODING_A), encode(CODING_D), null));
    assertArrayEquals(
        EMPTY, designationUdf.call(encode(CODING_BB_VERSION1), encode(CODING_E), "en"));

    verify(terminologyService).lookup(deepEq(CODING_A), eq("designation"));
    verify(terminologyService).lookup(deepEq(CODING_BB_VERSION1), eq("designation"));
    verifyNoMoreInteractions(terminologyService);
  }

  @Test
  void testReturnsCorrectDesignationsWithUseAndLanguage() {
    setupLookup(terminologyService)
        .withDesignation(CODING_A, CODING_C, null, "A_C_??")
        .withDesignation(CODING_A, CODING_C, "en", "A_C_en")
        .withDesignation(CODING_A, CODING_D, "en", "A_D_en")
        .withDesignation(CODING_BB_VERSION1, CODING_E, "en", "BB1_E_en.0", "BB1_E_en.1")
        .withDesignation(CODING_BB_VERSION1, CODING_E, "fr", "BB1_E_fr.0", "BB1_E_fr.1")
        .withDesignation(CODING_BB_VERSION1, null, "fr", "BB1_?_fr")
        .done();

    assertArrayEquals(
        new String[] {"A_C_en"}, designationUdf.call(encode(CODING_A), encode(CODING_C), "en"));

    assertArrayEquals(
        new String[] {"BB1_E_fr.0", "BB1_E_fr.1"},
        designationUdf.call(encode(CODING_BB_VERSION1), encode(CODING_E), "fr"));
  }

  @Test
  void testReturnsCorrectDesignationsWithNoUseOrNoLanguage() {
    setupLookup(terminologyService)
        .withDesignation(CODING_AA_VERSION1, null, "en", "AA1_?_en")
        .withDesignation(CODING_AA_VERSION1, CODING_D, "en", "AA1_D_en")
        .withDesignation(CODING_AA_VERSION1, CODING_E, "en", "AA1_E_en")
        .withDesignation(CODING_AA_VERSION1, CODING_E, "fr", "AA1_E_fr")
        .withDesignation(CODING_AA_VERSION1, CODING_E, null, "AA1_E_??")
        .withDesignation(CODING_AA_VERSION1, null, null, "AA1_?_??")
        .done();

    assertArrayEquals(
        new String[] {"AA1_E_en", "AA1_E_fr", "AA1_E_??"},
        designationUdf.call(encode(CODING_AA_VERSION1), encode(CODING_E), null));
    assertArrayEquals(
        new String[] {"AA1_?_en", "AA1_D_en", "AA1_E_en"},
        designationUdf.call(encode(CODING_AA_VERSION1), null, "en"));
    assertArrayEquals(
        new String[] {"AA1_?_en", "AA1_D_en", "AA1_E_en", "AA1_E_fr", "AA1_E_??", "AA1_?_??"},
        designationUdf.call(encode(CODING_AA_VERSION1), null, null));
  }
}
