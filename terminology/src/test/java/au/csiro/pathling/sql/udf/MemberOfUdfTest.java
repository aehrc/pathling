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

package au.csiro.pathling.sql.udf;

import static au.csiro.pathling.fhirpath.encoding.CodingSchema.encode;
import static au.csiro.pathling.test.helpers.FhirMatchers.deepEq;
import static au.csiro.pathling.test.helpers.TerminologyHelpers.CODING_LACERATION_OF_FOOT;
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
  private static final String VALUE_SET_URL_UNRESOLVABLE = "uuid:vsUnresolvable";
  private static final String VALUE_SET_URL_PARTIAL = "uuid:vsPartial";

  private MemberOfUdf memberUdf;
  private TerminologyService terminologyService;

  @BeforeEach
  void setUp() {
    terminologyService = mock(TerminologyService.class);
    final TerminologyServiceFactory terminologyServiceFactory =
        mock(TerminologyServiceFactory.class);
    when(terminologyServiceFactory.build()).thenReturn(terminologyService);
    memberUdf = new MemberOfUdf(terminologyServiceFactory);

    TerminologyServiceHelpers.setupValidate(terminologyService)
        .withValueSet(VALUE_SET_URL_A, CODING_A)
        .withValueSet(VALUE_SET_URL_AB, CODING_A, CODING_B)
        .withCodes(VALUE_SET_URL_A, CODE_A)
        .withUnresolvableValueSet(VALUE_SET_URL_UNRESOLVABLE);
    // A value set in which the membership of CODING_C cannot be determined, while CODING_B is a
    // member and every other coding is not.
    when(terminologyService.validateCode(eq(VALUE_SET_URL_PARTIAL), deepEq(CODING_C)))
        .thenReturn(null);
    when(terminologyService.validateCode(eq(VALUE_SET_URL_PARTIAL), deepEq(CODING_B)))
        .thenReturn(true);
  }

  @Test
  void testNullCodings() {
    assertNull(memberUdf.call(null, "uuid:url"));
  }

  @Test
  void testNullValueSetUrl() {
    assertNull(memberUdf.call(encode(CODING_LACERATION_OF_FOOT), null));
    assertNull(memberUdf.call(CODE_A, null));
    verifyNoMoreInteractions(terminologyService);
  }

  @Test
  void testInvalidAndNullCodings() {
    assertFalse(
        memberUdf.call(
            encodeMany(INVALID_CODING_0, INVALID_CODING_1, INVALID_CODING_2, null),
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

  @Test
  void testUnknownWhenValueSetUnresolvable() {
    // Membership in a value set that cannot be resolved is unknown, for one coding or many.
    assertNull(memberUdf.call(encode(CODING_A), VALUE_SET_URL_UNRESOLVABLE));
    assertNull(memberUdf.call(encodeMany(CODING_A, CODING_B), VALUE_SET_URL_UNRESOLVABLE));
  }

  @Test
  void testCodingsCombineAsThreeValuedOr() {
    // A known member makes the concept a member, whatever the other codings say.
    assertTrue(memberUdf.call(encodeMany(CODING_C, CODING_B), VALUE_SET_URL_PARTIAL));
    // Without a known member, an unknown coding makes the concept unknown rather than false.
    assertNull(memberUdf.call(encodeMany(CODING_A, CODING_C), VALUE_SET_URL_PARTIAL));
    // Only when every coding is known not to be a member is the concept not a member.
    assertFalse(memberUdf.call(encodeMany(CODING_A, CODING_D), VALUE_SET_URL_PARTIAL));
  }

  @Test
  void testCodeValidatedByInferringSystem() {
    // A string is a bare code, validated by inferring its system from the value set.
    assertTrue(memberUdf.call(CODE_A, VALUE_SET_URL_A));
    assertFalse(memberUdf.call(CODE_B, VALUE_SET_URL_A));
    assertNull(memberUdf.call(CODE_A, VALUE_SET_URL_UNRESOLVABLE));
    verify(terminologyService).validateCodeInferringSystem(VALUE_SET_URL_A, CODE_A);
  }
}
