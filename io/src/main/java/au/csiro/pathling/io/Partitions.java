/*
 * Copyright © 2018-2026 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.io;

import jakarta.annotation.Nonnull;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/** Helpers for the functions that Spark runs over a whole partition of rows. */
final class Partitions {

  private Partitions() {}

  /**
   * Returns the rows of a partition as a stream, in order, which is read lazily as the iterator is.
   *
   * @param rows the rows of the partition
   * @param <T> the type of the rows
   * @return the stream
   */
  @Nonnull
  static <T> Stream<T> stream(@Nonnull final Iterator<T> rows) {
    return StreamSupport.stream(
        Spliterators.spliteratorUnknownSize(rows, Spliterator.ORDERED), false);
  }
}
