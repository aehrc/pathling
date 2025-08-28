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
import static au.csiro.pathling.test.helpers.TerminologyHelpers.CD_SNOMED_284551006;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import au.csiro.pathling.test.AbstractTerminologyTestBase;
import au.csiro.pathling.test.helpers.TerminologyServiceHelpers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@SuppressWarnings("ConstantConditions")
class MemberOfUdfTest extends AbstractTerminologyTestBase {

  private static final String VALUE_SET_URL_A = "uuid:vsA";
  private static final String VALUE_SET_URL_AB = "uuid:vsAB";

  private MemberOfUdf memberUdf;
  private TerminologyService terminologyService;

  @BeforeEach
  void setUp() {
    terminologyService = mock(TerminologyService.class);
    final TerminologyServiceFactory terminologyServiceFactory = mock(
        TerminologyServiceFactory.class);
    when(terminologyServiceFactory.build()).thenReturn(terminologyService);
    memberUdf = new MemberOfUdf(terminologyServiceFactory);

    TerminologyServiceHelpers.setupValidate(terminologyService)
        .withValueSet(VALUE_SET_URL_A, CODING_A)
        .withValueSet(VALUE_SET_URL_AB, CODING_A, CODING_B);
  }

  @Test
  void testNullCodings() {
    assertNull(memberUdf.call(null, "uuid:url"));
  }

  @Test
  void testNullValueSetUrl() {
    assertNull(memberUdf.call(encode(CD_SNOMED_284551006), null));
  }

  @Test
  void testInvalidAndNullCodings() {
    assertFalse(
        memberUdf.call(encodeMany(INVALID_CODING_0, INVALID_CODING_1, INVALID_CODING_2, null),
            VALUE_SET_URL_A));
    verifyNoMoreInteractions(terminologyService);
  }

  @Test
  void testInvalidCoding() {
    assertFalse(memberUdf.call(encode(INVALID_CODING_0), VALUE_SET_URL_A));
    verifyNoMoreInteractions(terminologyService);
  }

  @Test
  void testCodingBelongsToValueSet() {
    assertTrue(memberUdf.call(encode(CODING_A), VALUE_SET_URL_A));
    assertTrue(memberUdf.call(encode(CODING_A), VALUE_SET_URL_AB));
    assertTrue(memberUdf.call(encode(CODING_B), VALUE_SET_URL_AB));

    assertFalse(memberUdf.call(encode(CODING_B), VALUE_SET_URL_A));
    assertFalse(memberUdf.call(encode(CODING_C), VALUE_SET_URL_A));
    assertFalse(memberUdf.call(encode(CODING_C), VALUE_SET_URL_AB));
  }

  @Test
  void testCodingsBelongsToValueSet() {
    // positive cases
    assertTrue(memberUdf.call(encodeMany(CODING_C, CODING_A), VALUE_SET_URL_A));
    assertTrue(memberUdf.call(encodeMany(null, INVALID_CODING_0, CODING_B), VALUE_SET_URL_AB));
    assertTrue(memberUdf.call(encodeMany(CODING_A, CODING_B), VALUE_SET_URL_AB));
    // negative cases
    assertFalse(memberUdf.call(encodeMany(), VALUE_SET_URL_A));
    assertFalse(memberUdf.call(encodeMany(CODING_C, CODING_B), VALUE_SET_URL_A));
    assertFalse(memberUdf.call(encodeMany(null, INVALID_CODING_1, CODING_C), VALUE_SET_URL_AB));
  }

  @Test
  void testEarlyExitWhenMatchingCodingFound() {
    assertTrue(memberUdf.call(encodeMany(CODING_A, CODING_B), VALUE_SET_URL_AB));
    verify(terminologyService).validateCode(eq(VALUE_SET_URL_AB), deepEq(CODING_A));
    verifyNoMoreInteractions(terminologyService);
  }
}
