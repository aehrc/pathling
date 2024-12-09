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

package au.csiro.pathling.fhirpath.operator;

import au.csiro.pathling.fhirpath.collection.BooleanCollection;
import au.csiro.pathling.fhirpath.collection.Collection;
import jakarta.annotation.Nonnull;
import org.apache.spark.sql.functions;

/**
 * Provides the functionality of the collection operators within FHIRPath.
 *
 * @author John Grimes
 * @author Piotr Szul
 * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#collections-1">FHIRPath specification -
 * Collections</a>
 */
public class CollectionOperations {

  /**
   * If the left operand is a collection with a single item, this operator returns {@code true} if
   * the item is in the right operand using equality semantics. If the left-hand side of the
   * operator is empty, the result is empty, if the right-hand side is empty, the result is
   * {@code false}. If the left operand has multiple items, an exception is thrown.
   *
   * @param element The element to check for membership
   * @param collection The collection to check membership within
   * @return A {@link BooleanCollection} representing the result of the operation
   * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#in-membership">FHIRPath specification -
   * in (membership)</a>
   */
  @FhirPathOperator
  @Nonnull
  public static BooleanCollection in(@Nonnull final Collection element,
      @Nonnull final Collection collection) {
    return contains(collection, element);
  }

  /**
   * If the right operand is a collection with a single item, this operator returns {@code true} if
   * the item is in the left operand using equality semantics. If the right-hand side of the
   * operator is empty, the result is empty, if the left-hand side is empty, the result is
   * {@code @code false}. This is the converse operation of {@link #in(Collection, Collection)}.
   *
   * @param collection The collection to check membership within
   * @param element The element to check for membership
   * @return A {@link BooleanCollection} representing the result of the operation
   * @see <a href="https://build.fhir.org/ig/HL7/FHIRPath/#contains-containership">FHIRPath
   * specification - contains (containership)</a>
   */
  @FhirPathOperator
  @Nonnull
  public static BooleanCollection contains(@Nonnull final Collection collection,
      @Nonnull final Collection element) {
    return BooleanCollection.build(collection.getColumn().vectorize(
            c -> functions.array_contains(c, element.asSingular().getColumnValue()),
            c -> c.equalTo(element.asSingular().getColumnValue()))
        .orElse(false)
    );
  }

}
