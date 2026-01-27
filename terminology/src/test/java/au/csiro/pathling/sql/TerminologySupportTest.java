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

package au.csiro.pathling.sql;

import static org.hl7.fhir.r4.model.codesystems.ConceptMapEquivalence.EQUIVALENT;
import static org.hl7.fhir.r4.model.codesystems.ConceptMapEquivalence.INEXACT;
import static org.hl7.fhir.r4.model.codesystems.ConceptMapEquivalence.RELATEDTO;
import static org.hl7.fhir.r4.model.codesystems.ConceptMapEquivalence.WIDER;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import au.csiro.pathling.errors.InvalidUserInputError;
import com.google.common.collect.ImmutableSet;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;

class TerminologySupportTest {

  @Test
  void convertsNullAndCsvCodesEnumSet() {
    assertNull(TerminologySupport.parseCsvEquivalences(null));
    assertEquals(Collections.emptySet(), TerminologySupport.parseCsvEquivalences(""));
    assertEquals(Collections.emptySet(), TerminologySupport.parseCsvEquivalences("  "));
  }

  @Test
  void parseValidCodesToEnumSetIgnoringBlanks() {
    assertEquals(
        ImmutableSet.of(WIDER, INEXACT),
        TerminologySupport.parseCsvEquivalences("wider, inexact, , wider "));
  }

  @Test
  void throwInvalidUserInputWhenParsingInvalidCodes() {
    final InvalidUserInputError ex =
        assertThrows(
            InvalidUserInputError.class,
            () -> TerminologySupport.parseCsvEquivalences("wider, invalid"));
    assertEquals("Unknown ConceptMapEquivalence code 'invalid'", ex.getMessage());
  }

  @Test
  void convertsNullAndEmptyCodesToEnumSet() {
    assertNull(TerminologySupport.equivalenceCodesToEnum(null));
    assertEquals(
        Collections.emptySet(), TerminologySupport.equivalenceCodesToEnum(Collections.emptyList()));
  }

  @Test
  void convertsValidCodesToEnumSet() {
    assertEquals(
        ImmutableSet.of(EQUIVALENT, RELATEDTO),
        TerminologySupport.equivalenceCodesToEnum(
            List.of("equivalent", "relatedto", "equivalent")));
  }

  @Test
  void throwInvalidUserInputWhenConvertingInvalidCode() {
    final List<String> invalid = List.of("invalid");
    final InvalidUserInputError ex =
        assertThrows(
            InvalidUserInputError.class, () -> TerminologySupport.equivalenceCodesToEnum(invalid));

    assertEquals("Unknown ConceptMapEquivalence code 'invalid'", ex.getMessage());
  }
}
