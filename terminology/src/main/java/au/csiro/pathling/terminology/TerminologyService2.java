/*
 * Copyright 2022 Commonwealth Scientific and Industrial Research
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

import lombok.Value;
import org.hl7.fhir.r4.model.Coding;
import org.hl7.fhir.r4.model.codesystems.ConceptMapEquivalence;
import org.hl7.fhir.r4.model.codesystems.ConceptSubsumptionOutcome;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.util.List;

/**
 * Abstraction layer for the terminology related operations.
 *
 * @author Piotr Szul
 */
public interface TerminologyService2 {

  /**
   * Represent a single translation of a code.
   */
  @Value(staticConstructor = "of")
  class Translation {

    @Nonnull
    ConceptMapEquivalence equivalence;

    @Nonnull
    Coding concept;
  }

  /**
   * Validates that a coded value is in the code system. Abstracts the FHIR <a
   * href="https://www.hl7.org/fhir/R4/operation-codesystem-validate-code.html">CodeSystem/$validate-code</a>
   * operation.
   *
   * @param codeSystemUrl the URL of the code system.
   * @param coding the coding to test.
   * @return true if the coding is valid.
   */
  boolean validateCode(@Nonnull String codeSystemUrl, @Nonnull Coding coding);

  /**
   * Translates a code from one value set to another, based on the existing concept map. Abstracts
   * the FHIR <a href="https://www.hl7.org/fhir/R4/operation-conceptmap-translate.html">ConceptMap/$translate</a>
   * operation.
   *
   * @param coding the code to translate.
   * @param conceptMapUrl the url of the concept map to use for translation.
   * @param reverse if this is true, then the operation should return all the codes that might be
   * mapped to this code.
   * @param target identifies the value set in which a translation is sought. If null all known
   * translations are returned.
   * @return the list of translations.
   */
  @Nonnull
  List<Translation> translate(@Nonnull Coding coding,
      @Nonnull String conceptMapUrl,
      boolean reverse,
      @Nullable String target);

  /**
   * Tests the subsumption relationship between two codings given the semantics of subsumption in
   * the underlying code system. Abstracts the <a href="https://www.hl7.org/fhir/R4/codesystem-operation-subsumes.html">CodeSystem/$subsumes</a>
   * operation.
   *
   * @param codingA the left code to be tested.
   * @param codingB the right code to be tested.
   * @return {@link ConceptSubsumptionOutcome} representing the relation between codingA (left code)
   * and codingB (right code).
   */
  @Nonnull
  ConceptSubsumptionOutcome subsumes(@Nonnull Coding codingA, @Nonnull Coding codingB);
}