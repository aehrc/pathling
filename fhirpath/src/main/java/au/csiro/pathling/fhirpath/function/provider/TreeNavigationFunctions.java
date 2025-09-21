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

package au.csiro.pathling.fhirpath.function.provider;

import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.function.CollectionTransform;
import au.csiro.pathling.fhirpath.function.FhirPathFunction;
import jakarta.annotation.Nonnull;

/**
 * Contains functions for tree navigation within FHIR resources.
 *
 * @author John Grimes
 * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#tree-navigation">FHIRPath
 * Specification - Tree navigation</a>
 */
public class TreeNavigationFunctions {

  private TreeNavigationFunctions() {
  }

  /**
   * Returns a collection that contains all items in the input collection, and recursively adds all
   * items that are reachable by repeatedly applying the projection expression to the input
   * collection. The projection is applied to the input collection and all intermediate results.
   * Unlike repeat(), this function does not check for duplicates and will include all items found
   * during the traversal.
   * <p>
   * This function is useful for traversing hierarchical data structures where duplicate checking
   * would be expensive and unnecessary. The order of items in the result is undefined.
   * <p>
   * If the input collection is empty, the result is empty.
   * <p>
   * Implementations should detect and prevent infinite loops, typically by limiting the depth of
   * traversal or detecting cycles.
   *
   * @param input The input collection
   * @param expression The projection expression to apply recursively
   * @return A collection containing all items reachable through repeated application of the
   * expression
   * @see <a
   * href="https://build.fhir.org/ig/HL7/FHIRPath/#repeatallprojection-expression--collection">repeatAll</a>
   */
  @FhirPathFunction
  @Nonnull
  public static Collection repeatAll(@Nonnull final Collection input,
      @Nonnull final CollectionTransform expression) {
    return input.repeatAll(expression.toColumnTransformation(input));
  }

}
