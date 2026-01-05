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

package au.csiro.pathling.projection;

import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import jakarta.annotation.Nonnull;
import org.apache.spark.sql.Column;

/**
 * Represents a selection that unnests a nested data structure, with either inner or outer join
 * semantics.
 *
 * <p>This selection evaluates a FHIRPath expression to get a collection, then applies a projection
 * clause to each element of that collection. The results are flattened into a single array. When
 * multiple projections are needed, wrap them in a {@link GroupingSelection} first.
 *
 * @param path the FHIRPath expression that identifies the collection to unnest
 * @param component the projection clause to apply to each element (use GroupingSelection for
 *     multiple)
 * @param joinOuter whether to use outer join semantics (i.e., return a row even if the unnesting
 *     collection is empty)
 * @author John Grimes
 * @author Piotr Szul
 */
public record UnnestingSelection(
    @Nonnull FhirPath path, @Nonnull ProjectionClause component, boolean joinOuter)
    implements UnarySelection {

  @Nonnull
  @Override
  public ProjectionResult evaluate(@Nonnull final ProjectionContext context) {
    // Evaluate the path to get the collection that will serve as the basis for unnesting.
    final Collection unnestingCollection = context.evalExpression(path);
    final ProjectionContext unnestingContext = context.withInputContext(unnestingCollection);
    final Column columnResult = component.evaluateElementWise(unnestingContext);
    return component
        .evaluate(unnestingContext.asStubContext())
        .withResultColumn(columnResult)
        .orNull(joinOuter);
  }

  /**
   * Returns the FHIRPath expression representation of this unnesting selection.
   *
   * @return the expression string containing forEach or forEachOrNull with path
   */
  @Nonnull
  @Override
  public String toExpression() {
    return (joinOuter ? "forEachOrNull" : "forEach") + ": " + path.toExpression();
  }
}
