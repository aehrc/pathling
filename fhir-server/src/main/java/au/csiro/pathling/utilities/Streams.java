package au.csiro.pathling.utilities;


import java.util.Iterator;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import javax.annotation.Nonnull;

/**
 * Utility class containing some stream helper functions.
 *
 * @author John Grimes
 */
public final class Streams {

  private Streams() {
  }

  /**
   * Creates a stream from the iterator.
   *
   * @param iterator an iterator
   * @param <T> the type (of elements)
   * @return the stream for given iterator
   */
  @Nonnull
  public static <T> Stream<T> streamOf(@Nonnull Iterator<T> iterator) {
    final Iterable<T> iterable = () -> iterator;
    return StreamSupport
        .stream(iterable.spliterator(), false);
  }
}
