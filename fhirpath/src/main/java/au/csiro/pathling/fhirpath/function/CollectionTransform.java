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

package au.csiro.pathling.fhirpath.function;

import au.csiro.pathling.fhirpath.collection.BooleanCollection;
import au.csiro.pathling.fhirpath.collection.Collection;
import jakarta.annotation.Nonnull;
import java.util.function.Function;

/**
 * Represents a transformation from one {@link Collection} to another.
 *
 * @author Piotr Szul
 */
@FunctionalInterface
public interface CollectionTransform extends Function<Collection, Collection> {

  /**
   * @return the input collection if it returns a {@link BooleanCollection}, otherwise throws an
   * exception
   */
  default CollectionTransform requireBoolean() {
    return input -> {
      final Collection result = apply(input);
      if (result instanceof BooleanCollection) {
        return result;
      } else {
        throw new RuntimeException("Expected boolean result");
      }
    };
  }

  /**
   * @param input the input collection
   * @return a function that applies the transformation and returns the column representation
   */
  default ColumnTransform toColumnTransformation(
      @Nonnull final Collection input) {
    // The type of the element Collection needs to be the same as the input.
    return c -> apply(input.copyWith(c)).getColumn();
  }

}