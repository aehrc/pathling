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

package au.csiro.pathling.terminology.local.index;

import jakarta.annotation.Nonnull;
import java.util.Comparator;

/**
 * The order in which a concept's descriptions are held, fixed by their content so that no result
 * derived from them depends on the order the store's rows were read.
 *
 * <p>Language comes first, which keeps a concept's languages together rather than interleaving
 * them, as a person scanning a designation list would expect. Type and then term settle the order
 * within a language. Every key comes from the content, so two stores built from the same release
 * hold each concept's descriptions in the same order.
 *
 * @author John Grimes
 */
public final class DescriptionOrder {

  /** Orders descriptions by language, then type, then term, with nulls last within each key. */
  private static final Comparator<Description> BY_LANGUAGE_TYPE_AND_TERM =
      Comparator.comparing(
              Description::getLanguage, Comparator.nullsLast(Comparator.naturalOrder()))
          .thenComparing(Description::getTypeCode, Comparator.nullsLast(Comparator.naturalOrder()))
          .thenComparing(Description::getTerm, Comparator.nullsLast(Comparator.naturalOrder()));

  private DescriptionOrder() {
    // Utility class.
  }

  /**
   * Returns the comparator that orders descriptions by language, then type, then term.
   *
   * @return the comparator, which sorts nulls last within each key
   */
  @Nonnull
  public static Comparator<Description> byLanguageTypeAndTerm() {
    return BY_LANGUAGE_TYPE_AND_TERM;
  }
}
