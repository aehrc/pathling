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
import au.csiro.pathling.fhirpath.collection.IntegerCollection;
import au.csiro.pathling.fhirpath.validation.FhirpathBinaryOperator;
import javax.annotation.Nonnull;
import org.apache.spark.sql.functions;

public class BinaryOperators {

  @FhirpathBinaryOperator
  public static BooleanCollection contains(@Nonnull final Collection collection,
      @Nonnull final Collection element) {
    return BooleanCollection.build(collection.getColumn().vectorize(
        c -> functions.array_contains(c, element.asSingular().getColumnValue()),
        c -> c.equalTo(element.asSingular().getColumnValue()))
    );
  }

  @FhirpathBinaryOperator
  public static BooleanCollection in(@Nonnull final Collection element,
      @Nonnull final Collection collection) {
    return contains(collection, element);
  }

  /**
   * Returns a singleton collection containing the element at the specified index.
   *
   * @param subject The collection to index
   * @param index The index to use
   * @return A singleton collection containing the element at the specified index
   * @see <a href="https://hl7.org/fhirpath/#index-integer-collection">Indexer operation</a>
   */
  @FhirpathBinaryOperator
  @Nonnull
  public static Collection index(@Nonnull final Collection subject,
      @Nonnull final IntegerCollection index) {
    return subject.map(
        rep -> rep.vectorize(
            // If the subject is non-singular, use the `element_at` function to extract the element 
            // at the specified index. 
            c -> functions.element_at(c, index.getColumnValue().plus(1)),
            // If the subject is singular, return the subject itself. 
            c -> c
        ));
  }

}