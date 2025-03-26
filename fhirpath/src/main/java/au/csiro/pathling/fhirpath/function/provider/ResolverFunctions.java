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

package au.csiro.pathling.fhirpath.function.provider;

import au.csiro.pathling.fhirpath.EvaluationContext;
import au.csiro.pathling.fhirpath.FhirPath;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.ReferenceCollection;
import au.csiro.pathling.fhirpath.collection.ResourceCollection;
import au.csiro.pathling.fhirpath.execution.DataRoot.ResourceRoot;
import au.csiro.pathling.fhirpath.execution.DataRoot.ReverseResolveRoot;
import au.csiro.pathling.fhirpath.function.FhirPathFunction;
import jakarta.annotation.Nonnull;
import lombok.extern.slf4j.Slf4j;

/**
 * Functions for resolving references between FHIR resources in FHIRPath expressions.
 * <p>
 * This class provides implementations of the FHIRPath functions that traverse relationships between
 * resources:
 * <ul>
 *   <li>{@link #resolve} - Follows references from one resource to another</li>
 *   <li>{@link #reverseResolve} - Finds resources that reference a particular resource</li>
 * </ul>
 * <p>
 * These functions are essential for navigating the graph of relationships between FHIR resources
 * and enable complex queries that span multiple resource types.
 *
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#resolve">Pathling
 * documentation - resolve</a>
 * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#reverseresolve">Pathling
 * documentation - reverseResolve</a>
 */
@Slf4j
public class ResolverFunctions {

  /**
   * Resolves a reverse join from a parent resource to child resources that reference it.
   * <p>
   * This function implements the FHIRPath reverseResolve() function, which finds resources that
   * reference the parent resource. For example, in Patient.reverseResolve(Condition.subject), this
   * function finds all Condition resources that reference the Patient through their subject field.
   * <p>
   * The implementation:
   * <ol>
   *   <li>Creates a ReverseResolveRoot from the input resource type and subject path</li>
   *   <li>Delegates to the evaluation context to perform the actual reverse join resolution</li>
   * </ol>
   *
   * @param input The parent resource collection being referenced
   * @param subjectPath The FHIRPath expression in the child resource that references the parent
   * @param evaluationContext The context for evaluating the expression
   * @return A ResourceCollection containing the child resources that reference the parent
   * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#reverseresolve">Pathling
   * documentation - reverseResolve</a>
   */
  @FhirPathFunction
  @Nonnull
  public static ResourceCollection reverseResolve(@Nonnull final ResourceCollection input,
      @Nonnull final FhirPath subjectPath, @Nonnull EvaluationContext evaluationContext) {
    // subject path should be Resource + traversal path
    final ReverseResolveRoot root = ReverseResolveRoot.fromChildPath(
        ResourceRoot.of(input.getResourceType()),
        subjectPath);
    log.debug("Reverse resolve root: {}", root);
    return evaluationContext.resolveReverseJoin(input, root.getForeignResourceType().toCode(),
        root.getForeignKeyPath());
  }

  /**
   * Resolves references from one resource to another.
   * <p>
   * This function implements the FHIRPath resolve() function, which follows references from one
   * resource to another. For example, in Patient.managingOrganization.resolve(), this function
   * resolves the Organization resources referenced by the managingOrganization field.
   * <p>
   * The implementation delegates to the evaluation context to perform the actual join resolution.
   * The result can be a collection of resources of different types if the reference is polymorphic.
   * In such cases, the ofType() function can be used to filter for specific resource types.
   *
   * @param input The collection of references to resolve
   * @param evaluationContext The context for evaluating the expression
   * @return A Collection containing the resolved resources
   * @see <a href="https://pathling.csiro.au/docs/fhirpath/functions.html#resolve">Pathling
   * documentation - resolve</a>
   */
  @FhirPathFunction
  @Nonnull
  public static Collection resolve(@Nonnull final ReferenceCollection input,
      @Nonnull EvaluationContext evaluationContext) {
    return evaluationContext.resolveJoin(input);
  }
}
