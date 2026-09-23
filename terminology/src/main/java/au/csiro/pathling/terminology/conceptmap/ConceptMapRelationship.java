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

package au.csiro.pathling.terminology.conceptmap;

import jakarta.annotation.Nonnull;
import org.hl7.fhir.r4.model.Enumerations.ConceptMapEquivalence;

/**
 * The R5 ConceptMapRelationship codes that a concept map relation carries, and the conversion of an
 * R4 equivalence to one of them.
 *
 * @author John Grimes
 */
public final class ConceptMapRelationship {

  /** The source and target are equivalent. */
  public static final String EQUIVALENT = "equivalent";

  /** The source is narrower than the target. */
  public static final String SOURCE_IS_NARROWER_THAN_TARGET = "source-is-narrower-than-target";

  /** The source is broader than the target. */
  public static final String SOURCE_IS_BROADER_THAN_TARGET = "source-is-broader-than-target";

  /** The source and target are related in some other way. */
  public static final String RELATED_TO = "related-to";

  /** The source and target are not related. */
  public static final String NOT_RELATED_TO = "not-related-to";

  private ConceptMapRelationship() {
    // Constants only.
  }

  /**
   * Converts an R4 equivalence to its R5 relationship, following the official R4 to R5 conversion
   * of ConceptMap.
   *
   * @param equivalence the R4 equivalence
   * @return the relationship code
   * @throws IllegalArgumentException for {@code unmatched}, which yields a no-mapping row rather
   *     than a relationship, and for {@code null}
   */
  @Nonnull
  public static String of(@Nonnull final ConceptMapEquivalence equivalence) {
    return switch (equivalence) {
      case EQUAL, EQUIVALENT -> EQUIVALENT;
      case WIDER, SUBSUMES -> SOURCE_IS_NARROWER_THAN_TARGET;
      case NARROWER, SPECIALIZES -> SOURCE_IS_BROADER_THAN_TARGET;
      case RELATEDTO, INEXACT -> RELATED_TO;
      case DISJOINT -> NOT_RELATED_TO;
      case UNMATCHED, NULL ->
          throw new IllegalArgumentException(
              "No relationship corresponds to the equivalence " + equivalence.toCode());
    };
  }
}
