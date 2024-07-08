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

package au.csiro.pathling.fhirpath;

import au.csiro.pathling.fhirpath.FhirPath.Composite;
import jakarta.annotation.Nonnull;
import lombok.Value;

public interface FhirPathVisitor<T> {

  @Value(staticConstructor = "of")
  class Contextual<T> {

    @Nonnull
    T value;

    @Nonnull
    FhirPathVisitor<T> context;
  }


  @Nonnull
  T visitPath(@Nonnull final FhirPath path);

  @Nonnull
  T nullValue();

  @Nonnull
  T combiner(@Nonnull final T left, @Nonnull final T right);

  @Nonnull
  default T visitComposite(@Nonnull final Composite path) {
    return path.asStream()
        .reduce(Contextual.of(nullValue(), this),
            (result, child) -> Contextual.of(combiner(result.value, child.accept(result.context)),
                result.context.enterContext(child)),
            (result1, result2) -> result1).value;
  }

  /**
   * New context is created by path traversal
   */
  @Nonnull
  default FhirPathVisitor<T> enterContext(@Nonnull final FhirPath context) {
    return this;
  }
}
