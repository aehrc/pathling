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

package au.csiro.pathling.export;

import lombok.Value;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

@Value
public class Either<R, L> {

  @Nullable
  L left;

  @Nullable
  R right;


  @Nonnull
  R getRight() {
    if (right == null) {
      throw new IllegalStateException("Right value is empty");
    }
    return right;
  }

  @Nonnull
  L getLeft() {
    if (left == null) {
      throw new IllegalStateException("Left value is empty");
    }
    return left;
  }
  
  @Nonnull
  public static <L, R> Either<R, L> left(@Nonnull final L left) {
    return new Either<>(left, null);
  }

  @Nonnull
  public static <L, R> Either<R, L> right(@Nonnull final R right) {
    return new Either<>(null, right);
  }

  public boolean isEmpty() {
    return right == null;
  }
}
