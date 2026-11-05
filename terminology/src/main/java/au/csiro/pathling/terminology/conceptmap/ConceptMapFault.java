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

/**
 * The reasons that the content of an R4 ConceptMap cannot be represented as the rows of a concept
 * map relation. {@link ConceptMapContent#fromResource} reports them as it converts a resource, and
 * the local terminology store records the first of them at import, so that both report the same
 * reason for the same map.
 *
 * @author John Grimes
 */
public final class ConceptMapFault {

  private ConceptMapFault() {
    // Static helper.
  }

  /**
   * Returns the reason given for a group that names no source system.
   *
   * @return the reason
   */
  @Nonnull
  public static String groupWithoutSource() {
    return "a group has no source system";
  }

  /**
   * Returns the reason given for an element that carries no code.
   *
   * @return the reason
   */
  @Nonnull
  public static String elementWithoutCode() {
    return "an element has no code";
  }

  /**
   * Returns the reason given for a target that depends on other elements.
   *
   * @param sourceCode the code of the element the target belongs to
   * @return the reason
   */
  @Nonnull
  public static String dependsOn(@Nonnull final String sourceCode) {
    return "the mapping for source code '" + sourceCode + "' depends on other elements (dependsOn)";
  }

  /**
   * Returns the reason given for a target that carries products.
   *
   * @param sourceCode the code of the element the target belongs to
   * @return the reason
   */
  @Nonnull
  public static String product(@Nonnull final String sourceCode) {
    return "the mapping for source code '" + sourceCode + "' has products";
  }

  /**
   * Returns the reason given for a target that carries no equivalence.
   *
   * @param sourceCode the code of the element the target belongs to
   * @return the reason
   */
  @Nonnull
  public static String noEquivalence(@Nonnull final String sourceCode) {
    return "the mapping for source code '" + sourceCode + "' has no equivalence";
  }

  /**
   * Returns the reason given for a target that maps to something but carries no code.
   *
   * @param sourceCode the code of the element the target belongs to
   * @return the reason
   */
  @Nonnull
  public static String noTargetCode(@Nonnull final String sourceCode) {
    return "the mapping for source code '" + sourceCode + "' has no target code";
  }
}
