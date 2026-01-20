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
import static org.hl7.fhir.r4.model.codesystems.ConceptMapEquivalence.EQUIVALENT;
import static org.hl7.fhir.r4.model.codesystems.ConceptMapEquivalence.NARROWER;
import static org.hl7.fhir.r4.model.codesystems.ConceptMapEquivalence.RELATEDTO;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static scala.jdk.javaapi.CollectionConverters.asScala;

import au.csiro.pathling.errors.InvalidUserInputError;
import au.csiro.pathling.terminology.TerminologyService;
import au.csiro.pathling.terminology.TerminologyService.Translation;
import au.csiro.pathling.terminology.TerminologyServiceFactory;
import au.csiro.pathling.test.AbstractTerminologyTestBase;
import au.csiro.pathling.test.helpers.TerminologyServiceHelpers;
import jakarta.annotation.Nonnull;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.spark.sql.Row;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import scala.collection.mutable.ArraySeq;
import scala.reflect.ClassTag;

class TranslateOfUdfTest extends AbstractTerminologyTestBase {

  private static final String CONCEPT_MAP_A = "uuid:caA";
  private static final String CONCEPT_MAP_B = "uuid:caB";

  private static final Row[] NO_TRANSLATIONS = new Row[] {};

  private TranslateUdf translateUdf;
  private TerminologyService terminologyService;

  private static <T> ArraySeq<T> newWrappedArray(@Nonnull final List<T> args) {
    return ArraySeq.from(asScala(args), ClassTag.apply(Object.class));
  }

  @BeforeEach
  void setUp() {
    terminologyService = mock(TerminologyService.class);
    final TerminologyServiceFactory terminologyServiceFactory =
        mock(TerminologyServiceFactory.class);
    when(terminologyServiceFactory.build()).thenReturn(terminologyService);
    translateUdf = new TranslateUdf(terminologyServiceFactory);
  }

  @Test
  void testNullCodings() {
    assertNull(translateUdf.call(null, CONCEPT_MAP_A, true, null, null));
  }

  @Test
  void testTranslatesCodingWithDefaults() {

    TerminologyServiceHelpers.setupTranslate(terminologyService)
        .withTranslations(
            CODING_AA,
            CONCEPT_MAP_A,
            Translation.of(EQUIVALENT, CODING_BB),
            Translation.of(RELATEDTO, CODING_AB));

    assertArrayEquals(
        asArray(CODING_BB), translateUdf.call(encode(CODING_AA), CONCEPT_MAP_A, false, null, null));
  }

  @Test
  void testTranslatesCodingsUniqueResults() {

    TerminologyServiceHelpers.setupTranslate(terminologyService)
        .withTranslations(
            CODING_AA_VERSION1,
            CONCEPT_MAP_B,
            true,
            SYSTEM_B,
            Translation.of(EQUIVALENT, CODING_AA),
            Translation.of(NARROWER, CODING_BB),
            Translation.of(RELATEDTO, CODING_AB))
        .withTranslations(
            CODING_AB_VERSION1,
            CONCEPT_MAP_B,
            true,
            SYSTEM_B,
            Translation.of(EQUIVALENT, CODING_AB),
            Translation.of(NARROWER, CODING_BB),
            Translation.of(RELATEDTO, CODING_BA));

    assertArrayEquals(
        asArray(CODING_BB, CODING_AB, CODING_BA),
        translateUdf.call(
            encodeMany(
                null,
                INVALID_CODING_1,
                INVALID_CODING_0,
                INVALID_CODING_2,
                CODING_AA_VERSION1,
                CODING_AB_VERSION1),
            CONCEPT_MAP_B,
            true,
            newWrappedArray(List.of("narrower", "relatedto")),
            SYSTEM_B));
  }

  @Test
  void testInvalidAndNullCodings() {
    assertArrayEquals(
        NO_TRANSLATIONS,
        translateUdf.call(
            encodeMany(INVALID_CODING_0, INVALID_CODING_1, INVALID_CODING_2, null),
            "uuid:url",
            true,
            null,
            null));
    verifyNoMoreInteractions(terminologyService);
  }

  @Test
  void testThrowsInputErrorWhenInvalidEquivalence() {
    final Row coding = encode(CODING_AA);
    final ArraySeq<String> invalid = newWrappedArray(List.of("invalid"));
    final InvalidUserInputError ex =
        assertThrows(
            InvalidUserInputError.class,
            () -> translateUdf.call(coding, CONCEPT_MAP_B, true, invalid, null));
    assertEquals("Unknown ConceptMapEquivalence code 'invalid'", ex.getMessage());
    verifyNoMoreInteractions(terminologyService);
  }

  @Test
  void testToleratesNullAndBlankEquivalencesAndEmptyEquivalencesList() {

    TerminologyServiceHelpers.setupTranslate(terminologyService)
        .withTranslations(
            CODING_AA,
            CONCEPT_MAP_A,
            Translation.of(EQUIVALENT, CODING_BB),
            Translation.of(RELATEDTO, CODING_AB));

    final List<String> args = new ArrayList<>();
    args.add("");
    args.add(null);
    assertArrayEquals(
        NO_TRANSLATIONS,
        translateUdf.call(encode(CODING_AA), CONCEPT_MAP_A, false, newWrappedArray(args), null));

    assertArrayEquals(
        NO_TRANSLATIONS,
        translateUdf.call(
            encode(CODING_AA),
            CONCEPT_MAP_A,
            false,
            newWrappedArray(Collections.emptyList()),
            null));
    verifyNoMoreInteractions(terminologyService);
  }
}
