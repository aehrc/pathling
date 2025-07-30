/*
 * Copyright 2023 Commonwealth Scientific and Industrial Research
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

import static java.util.Objects.isNull;
import static java.util.function.Function.identity;

import au.csiro.pathling.sql.udf.TranslateUdf;
import au.csiro.pathling.utilities.Strings;
import jakarta.annotation.Nullable;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;
import org.hl7.fhir.r4.model.codesystems.ConceptMapEquivalence;

/**
 * Supporting functions for {@link Terminology} interface.
 */
public interface TerminologySupport {

  /**
   * Parses a string with the comma separated list of concept map equivalence codes to the
   * collection of unique {@link ConceptMapEquivalence} enum values. Throws
   * {@link au.csiro.pathling.errors.InvalidUserInputError} if any of the codes cannot be
   * converted.
   *
   * @param equivalencesCsv a collection of equivalence codes.
   * @return the set of unique enum values.
   */
  @Nullable
  static Set<ConceptMapEquivalence> parseCsvEquivalences(
      @Nullable final String equivalencesCsv) {
    return isNull(equivalencesCsv)
           ? null
           : equivalenceCodesToEnum(Strings.parseCsvList(equivalencesCsv, identity()));
  }

  /**
   * Converts a collection of concept map equivalence codes to the collection of unique
   * {@link ConceptMapEquivalence} enum values. Throws
   * {@link au.csiro.pathling.errors.InvalidUserInputError} if any of the codes cannot be
   * converted.
   *
   * @param equivalenceCodes a collection of equivalence codes.
   * @return the set of unique enum values.
   */
  @Nullable
  static Set<ConceptMapEquivalence> equivalenceCodesToEnum(
      @Nullable final Collection<String> equivalenceCodes) {
    return isNull(equivalenceCodes)
           ? null
           : equivalenceCodes.stream()
               .map(TranslateUdf::checkValidEquivalenceCode)
               .map(ConceptMapEquivalence::fromCode)
               .collect(Collectors.toUnmodifiableSet());
  }
}
