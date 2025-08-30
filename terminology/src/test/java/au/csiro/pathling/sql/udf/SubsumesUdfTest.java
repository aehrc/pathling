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
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import au.csiro.pathling.test.AbstractTerminologyTestBase;
import au.csiro.pathling.test.helpers.TerminologyServiceHelpers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

@SuppressWarnings("ConstantConditions")
class SubsumesUdfTest extends AbstractTerminologyTestBase {

  private SubsumesUdf subsumesUdf;
  private TerminologyService terminologyService;

  @BeforeEach
  void setUp() {
    terminologyService = mock(TerminologyService.class);
    final TerminologyServiceFactory terminologyServiceFactory = mock(
        TerminologyServiceFactory.class);
    when(terminologyServiceFactory.build()).thenReturn(terminologyService);

    TerminologyServiceHelpers.setupSubsumes(terminologyService)
        .withSubsumes(CODING_AA, CODING_AB)
        .withSubsumes(CODING_BA, CODING_BB);
    subsumesUdf = new SubsumesUdf(terminologyServiceFactory);
  }

  @Test
  void testNullCodings() {
    assertNull(subsumesUdf.call(null, null, null));
    assertNull(subsumesUdf.call(encode(CODING_A), null, true));
    assertNull(subsumesUdf.call(null, encode(CODING_B), false));
    verifyNoMoreInteractions(terminologyService);
  }

  @Test
  void testInvalidCodings() {
    assertFalse(subsumesUdf.call(encode(INVALID_CODING_0), encode(INVALID_CODING_1), null));
    assertFalse(subsumesUdf.call(encode(INVALID_CODING_1), encode(INVALID_CODING_2), false));
    assertFalse(subsumesUdf.call(encode(INVALID_CODING_2), encode(INVALID_CODING_0), true));
    verifyNoMoreInteractions(terminologyService);
  }

  @Test
  void testNullAndInvalidCodings() {
    assertFalse(subsumesUdf.call(encodeMany(null, INVALID_CODING_0),
        encodeMany(null, INVALID_CODING_1, INVALID_CODING_2), false));
    assertFalse(subsumesUdf.call(encodeMany(null, INVALID_CODING_1, INVALID_CODING_2),
        encodeMany(null, INVALID_CODING_0), true));
    verifyNoMoreInteractions(terminologyService);
  }


  @Test
  void testSubsumesCoding() {
    // self subsumption
    assertTrue(subsumesUdf.call(encode(CODING_A), encode(CODING_A), null));
    assertTrue(subsumesUdf.call(encode(CODING_B), encode(CODING_B), false));
    assertTrue(subsumesUdf.call(encode(CODING_C), encode(CODING_C), true));

    // positive cases 
    assertTrue(subsumesUdf.call(encode(CODING_AA), encode(CODING_AB), null));
    assertTrue(subsumesUdf.call(encode(CODING_BA), encode(CODING_BB), false));
    assertTrue(subsumesUdf.call(encode(CODING_BB), encode(CODING_BA), true));

    // negative cases
    assertFalse(subsumesUdf.call(encode(CODING_BB), encode(CODING_BA), null));
    assertFalse(subsumesUdf.call(encode(CODING_AB), encode(CODING_AA), false));
    assertFalse(subsumesUdf.call(encode(CODING_AA), encode(CODING_AB), true));
    assertFalse(subsumesUdf.call(encode(CODING_C), encode(CODING_B), null));
    assertFalse(subsumesUdf.call(encode(CODING_A), encode(CODING_C), true));
  }

  @Test
  void testSubsumesCodings() {
    // positive cases 
    assertTrue(
        subsumesUdf.call(encodeMany(null, INVALID_CODING_0, CODING_AA, CODING_D), encode(CODING_AB),
            null));
    assertTrue(
        subsumesUdf.call(encodeMany(CODING_AB, CODING_BA), encodeMany(CODING_AA, CODING_BB),
            false));
    assertTrue(
        subsumesUdf.call(encodeMany(CODING_AB, CODING_BA), encodeMany(CODING_AA, CODING_BB), true));
    assertTrue(
        subsumesUdf.call(encodeMany(null, INVALID_CODING_1, CODING_C, CODING_BA), encode(CODING_BB),
            false));

    // NegativeCases
    assertFalse(subsumesUdf.call(encodeMany(), encode(CODING_B), null));
    assertFalse(subsumesUdf.call(encode(CODING_C), encodeMany(), true));
    assertFalse(subsumesUdf.call(encodeMany(), encodeMany(), true));

    assertFalse(
        subsumesUdf.call(encode(CODING_AB), encodeMany(CODING_AA, CODING_C, CODING_D), null));
    assertFalse(
        subsumesUdf.call(encodeMany(CODING_BB, CODING_AA, CODING_AB), encode(CODING_BA), false));
    assertFalse(
        subsumesUdf.call(encodeMany(CODING_AA, CODING_BA), encodeMany(CODING_AB, CODING_BB), true));
  }

}
