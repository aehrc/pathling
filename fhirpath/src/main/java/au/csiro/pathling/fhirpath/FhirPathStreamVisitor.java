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

import jakarta.annotation.Nonnull;
import java.util.stream.Stream;

public interface FhirPathStreamVisitor<E> extends FhirPathVisitor<Stream<E>> {

  @Nonnull
  @Override
  default Stream<E> nullValue() {
    return Stream.empty();
  }

  @Nonnull
  @Override
  default Stream<E> combiner(@Nonnull final Stream<E> left,
      @Nonnull final Stream<E> right) {
    return Stream.concat(left, right);
  }

  @Nonnull
  default Stream<E> visitChildren(@Nonnull final FhirPath path) {
    return path.children().flatMap(x -> x.accept(this));
  }
}
