/*
 * Copyright 2022 Commonwealth Scientific and Industrial Research
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

package au.csiro.pathling.sql;

import java.io.Serializable;
import java.util.Iterator;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Captures a contract for a mapping operation that is allowed to preview all of its input objects
 * and create a state object, that is then subsequently passed to the actual mapping operation.
 *
 * @param <I> input type of the mapper
 * @param <R> result type of the mapper
 * @param <S> state type of the mapper
 */
public interface MapperWithPreview<I, R, S> extends Serializable {

  /**
   * The preview operation that gives access to all input objects that will be later passed to the
   * mapping function and can use them to create a state object, which is also passed to the mapping
   * function.
   *
   * @param inputIterator the iterator over all objects to be mapped.
   * @return the state object that should be passed to the mapping function together with each input
   * object.
   */
  @Nonnull
  S preview(@Nonnull Iterator<I> inputIterator);

  /**
   * The mapping operations.
   *
   * @param input the object to map
   * @param state the state created by `preview` operation
   * @return the result of mapping the input with the state
   */
  @Nullable
  R call(@Nullable I input, @Nonnull S state);

}
