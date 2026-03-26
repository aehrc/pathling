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

package au.csiro.pathling.fhirpath.function.provider;

import au.csiro.pathling.fhirpath.EvaluationContext;
import au.csiro.pathling.fhirpath.TypeSpecifier;
import au.csiro.pathling.fhirpath.annotations.SqlOnFhirConformance;
import au.csiro.pathling.fhirpath.annotations.SqlOnFhirConformance.Profile;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.function.CollectionTransform;
import au.csiro.pathling.fhirpath.function.FhirPathFunction;
import jakarta.annotation.Nonnull;

/**
 * Contains functions for filtering and projecting elements in a collection.
 *
 * @author Piotr Szul
 * @author John Grimes
 * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#filtering-and-projection">FHIRPath
 *     Specification - Filtering and projection</a>
 */
public class FilteringAndProjectionFunctions {

  private FilteringAndProjectionFunctions() {}

  /**
   * Returns a collection containing only those elements in the input collection for which the
   * stated criteria expression evaluates to {@code true}. Elements for which the expression
   * evaluates to {@code false} or empty are not included in the result.
   *
   * <p>If the input collection is empty, the result is empty.
   *
   * <p>If the result of evaluating the condition is other than a single boolean value, the
   * evaluation will end and signal an error to the calling environment, consistent with singleton
   * evaluation of collections behavior.
   *
   * @param input The input collection
   * @param expression The criteria expression
   * @return A collection containing only those elements for which the criteria expression evaluates
   *     to {@code true}
   * @see <a
   *     href="https://build.fhir.org/ig/HL7/FHIRPath/#wherecriteria--expression--collection">where</a>
   */
  @FhirPathFunction
  @SqlOnFhirConformance(Profile.SHARABLE)
  @Nonnull
  public static Collection where(
      @Nonnull final Collection input, @Nonnull final CollectionTransform expression) {
    return input.filter(expression.requireBooleanSingleton().toColumnTransformation(input));
  }

  /**
   * Returns a collection containing the result of evaluating the projection expression for each
   * item in the input collection. The results are flattened: if evaluation produces multiple items,
   * all are added to the output; if evaluation produces empty, nothing is added.
   *
   * <p>If the input collection is empty, the result is empty.
   *
   * @param input The input collection
   * @param expression The projection expression
   * @return A collection containing the projected results
   * @see <a
   *     href="https://build.fhir.org/ig/HL7/FHIRPath/#selectprojection--expression--collection">select</a>
   */
  @FhirPathFunction
  @Nonnull
  public static Collection select(
      @Nonnull final Collection input, @Nonnull final CollectionTransform expression) {
    return input.project(expression);
  }

  /**
   * Returns a collection that contains all items in the input collection that are of the given type
   * or a subclass thereof. If the input collection is empty, the result is empty. The type argument
   * is an identifier that must resolve to the name of a type in a model. For implementations with
   * compile-time typing, this requires special-case handling when processing the argument to treat
   * it as type specifier rather than an identifier expression.
   *
   * @param input The input collection
   * @param typeSpecifier The type specifier
   * @return A collection containing only those elements that are of the specified type
   * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#oftype">ofType</a>
   */
  @FhirPathFunction
  @SqlOnFhirConformance(Profile.SHARABLE)
  @Nonnull
  public static Collection ofType(
      @Nonnull final Collection input, @Nonnull final TypeSpecifier typeSpecifier) {
    return input.filterByType(typeSpecifier);
  }

  /**
   * Recursively evaluates a projection expression on each item in the input collection, adding
   * results to the output only if they are not already present as determined by the equals ({@code
   * =}) operator. The projection is then re-evaluated on the new results, repeating until no new
   * items are produced or the maximum same-type recursion depth is reached.
   *
   * <p>This is the standard FHIRPath {@code repeat()} function. Unlike {@code repeatAll()}, it
   * performs equality-based deduplication of results.
   *
   * <p>If the input collection is empty, the result is empty.
   *
   * @param input The input collection
   * @param expression The projection expression to apply recursively
   * @param context The evaluation context providing configuration
   * @return A collection containing deduplicated recursively collected results
   * @see <a
   *     href="https://build.fhir.org/ig/HL7/FHIRPath/#repeatprojection--expression--collection">repeat</a>
   */
  @FhirPathFunction
  @Nonnull
  public static Collection repeat(
      @Nonnull final Collection input,
      @Nonnull final CollectionTransform expression,
      @Nonnull final EvaluationContext context) {
    final int maxDepth = context.getConfiguration().getMaxUnboundTraversalDepth();
    return input.repeat(expression, maxDepth);
  }

  /**
   * Recursively evaluates a projection expression on each item in the input collection, adding all
   * results to the output without deduplication. The projection is then re-evaluated on the new
   * results, repeating until no new items are produced or the maximum same-type recursion depth is
   * reached.
   *
   * <p>This is the STU FHIRPath {@code repeatAll()} function. Unlike {@code repeat()}, it does not
   * perform equality-based deduplication of results.
   *
   * <p>If the input collection is empty, the result is empty.
   *
   * <p>For Extension traversal, same-type depth exhaustion silently stops and returns results
   * collected up to that point. For all other same-type traversals, depth exhaustion raises an
   * error indicating infinite recursive traversal. The depth limit is controlled by {@code
   * maxUnboundTraversalDepth} in the FHIRPath configuration.
   *
   * @param input The input collection
   * @param expression The projection expression to apply recursively
   * @param context The evaluation context providing configuration
   * @return A collection containing all recursively collected results
   * @see <a
   *     href="https://build.fhir.org/ig/HL7/FHIRPath/#repeatallprojection--expression--collection">repeatAll</a>
   */
  @FhirPathFunction
  @SqlOnFhirConformance(Profile.EXPERIMENTAL)
  @Nonnull
  public static Collection repeatAll(
      @Nonnull final Collection input,
      @Nonnull final CollectionTransform expression,
      @Nonnull final EvaluationContext context) {
    final int maxDepth = context.getConfiguration().getMaxUnboundTraversalDepth();
    return input.repeatAll(expression, maxDepth, false);
  }
}
