/*
 * Copyright © 2018-2022, Commonwealth Scientific and Industrial Research
 * Organisation (CSIRO) ABN 41 687 119 230. Licensed under the CSIRO Open Source
 * Software Licence Agreement.
 */

package au.csiro.pathling.utilities;


import jakarta.annotation.Nonnull;
import java.util.Iterator;
import java.util.function.BinaryOperator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * Utility class containing some stream helper functions.
 *
 * @author John Grimes
 */
public abstract class Streams {

  /**
   * Creates a stream from the iterator.
   *
   * @param iterator an iterator
   * @param <T> the type (of elements)
   * @return the stream for given iterator
   */
  @Nonnull
  public static <T> Stream<T> streamOf(@Nonnull final Iterator<T> iterator) {
    final Iterable<T> iterable = () -> iterator;
    return StreamSupport
        .stream(iterable.spliterator(), false);
  }


  /**
   * Creates a combiner for Stream.reduce() that throws an exception if the combiner is called.
   *
   * @param <T> the type (of elements)
   * @return combiner for Stream.reduce() that throws an exception if the combiner is called
   */
  @Nonnull
  public static <T> BinaryOperator<T> unsupportedCombiner() {
    return (a, b) -> {
      throw new UnsupportedOperationException("Combiner should not be called");
    };
  }

}
