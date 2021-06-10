/*
 * Copyright © 2018-2021, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.sql;

import java.io.Serializable;
import java.util.Iterator;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * Captures a contract for a mapping operation that is allowed to preview all of its input objects
 * and create a state object, that is then subsequently passed to the to actual mapping operation.
 *
 * @param <I> input type of the mapper
 * @param <R> result type of the mapper
 * @param <S> state type of the mapper
 */
public interface MapperWithPreview<I, R, S> extends Serializable {

  /**
   * The preview operation that gives access to all of input object that will be later passed to the
   * mapping function and can use them to create a state object, which is also passed to the mapping
   * function.
   *
   * @param inputIterator the iterator over all objects to be mapped.
   * @return the state object that should be passed to the mapping function together with each input
   * object.
   * @throws Exception when an error occurs during processing
   */
  @Nonnull
  S preview(@Nonnull Iterator<I> inputIterator) throws Exception;

  /**
   * The mapping operations.
   *
   * @param input the object to map
   * @param state the state created by `preview` operation
   * @return the result of mapping the input with the state
   * @throws Exception when an error occurs during processing
   */
  @Nullable
  R call(@Nullable I input, @Nonnull S state) throws Exception;
}

