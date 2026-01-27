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

import static java.util.Objects.nonNull;

import au.csiro.pathling.fhirpath.annotations.SqlOnFhirConformance;
import au.csiro.pathling.fhirpath.annotations.SqlOnFhirConformance.Profile;
import au.csiro.pathling.fhirpath.collection.BooleanCollection;
import au.csiro.pathling.fhirpath.collection.Collection;
import au.csiro.pathling.fhirpath.collection.IntegerCollection;
import au.csiro.pathling.fhirpath.function.CollectionTransform;
import au.csiro.pathling.fhirpath.function.FhirPathFunction;
import jakarta.annotation.Nonnull;
import jakarta.annotation.Nullable;

/**
 * Contains functions for evaluating the existence of elements in a collection.
 *
 * @author Piotr Szul
 * @author John Grimes
 * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#existence">FHIRPath Specification -
 *     Existence</a>
 */
@SuppressWarnings("unused")
public class ExistenceFunctions {

  private ExistenceFunctions() {}

  /**
   * Returns {@code true} if the input collection has any elements (optionally filtered by the
   * criteria), and {@code false} otherwise. This is the opposite of {@code empty()}, and as such is
   * a shorthand for {@code empty().not()}. If the input collection is empty, the result is {@code
   * false}.
   *
   * <p>Using the optional criteria can be considered a shorthand for {@code
   * where(criteria).exists()}.
   *
   * @param input The input collection
   * @param criteria The criteria to apply to the input collection
   * @return A {@link BooleanCollection} containing the result
   * @see <a
   *     href="https://build.fhir.org/ig/HL7/FHIRPath/#existscriteria--expression--boolean">FHIRPath
   *     Specification - exists</a>
   */
  @FhirPathFunction
  @SqlOnFhirConformance(Profile.SHARABLE)
  @Nonnull
  public static BooleanCollection exists(
      @Nonnull final Collection input, @Nullable final CollectionTransform criteria) {
    return BooleanLogicFunctions.not(
        empty(nonNull(criteria) ? FilteringAndProjectionFunctions.where(input, criteria) : input));
  }

  /**
   * Returns {@code true} if the input collection is empty and {@code false} otherwise.
   *
   * @param input The input collection
   * @return A {@link BooleanCollection} containing the result
   * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#empty--boolean">FHIRPath Specification -
   *     empty</a>
   */
  @FhirPathFunction
  @SqlOnFhirConformance(Profile.SHARABLE)
  @Nonnull
  public static BooleanCollection empty(@Nonnull final Collection input) {
    return BooleanCollection.build(input.getColumn().isEmpty());
  }

  /**
   * Returns the integer count of the number of items in the input collection. Returns 0 when the
   * input collection is empty.
   *
   * @param input The input collection
   * @return An {@link IntegerCollection} containing the count
   * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#count--integer">FHIRPath Specification -
   *     count</a>
   */
  @FhirPathFunction
  @SqlOnFhirConformance(Profile.SHARABLE)
  @Nonnull
  public static IntegerCollection count(@Nonnull final Collection input) {
    return IntegerCollection.build(input.getColumn().count());
  }
}
