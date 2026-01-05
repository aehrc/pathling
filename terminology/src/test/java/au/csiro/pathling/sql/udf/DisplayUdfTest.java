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
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import au.csiro.pathling.test.AbstractTerminologyTestBase;
import au.csiro.pathling.test.helpers.TerminologyServiceHelpers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DisplayUdfTest extends AbstractTerminologyTestBase {

  private static final String DISPLAY_NAME_A = "Test Display Name A";
  private static final String DISPLAY_NAME_B = "Test Display Name B";

  private DisplayUdf displayUdf;
  private TerminologyService terminologyService;

  @BeforeEach
  void setUp() {
    terminologyService = mock(TerminologyService.class);
    final TerminologyServiceFactory terminologyServiceFactory =
        mock(TerminologyServiceFactory.class);
    when(terminologyServiceFactory.build()).thenReturn(terminologyService);
    displayUdf = new DisplayUdf(terminologyServiceFactory);
  }

  @Test
  void testNullCoding() {
    assertNull(displayUdf.call(null, null));
    verifyNoMoreInteractions(terminologyService);
  }

  @Test
  void testInvalidCodings() {
    assertNull(displayUdf.call(encode(INVALID_CODING_0), null));
    assertNull(displayUdf.call(encode(INVALID_CODING_1), null));
    assertNull(displayUdf.call(encode(INVALID_CODING_2), null));
    verifyNoMoreInteractions(terminologyService);
  }

  @Test
  void testGetsDisplayName() {
    TerminologyServiceHelpers.setupLookup(terminologyService)
        .withDisplay(CODING_A, DISPLAY_NAME_A, null)
        .withDisplay(CODING_BB_VERSION1, DISPLAY_NAME_B, "xx-XX");

    assertEquals(DISPLAY_NAME_A, displayUdf.call(encode(CODING_A), null));
    assertEquals(DISPLAY_NAME_B, displayUdf.call(encode(CODING_BB_VERSION1), "xx-XX"));

    // null when display property it not present
    assertNull(displayUdf.call(encode(CODING_C), null));
  }
}
