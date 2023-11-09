package au.csiro.pathling.fhirpath.column;

import au.csiro.pathling.fhirpath.collection.Collection;
import lombok.Value;
import javax.annotation.Nonnull;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Piotr Szul
 */
public interface ColumnPath {

  ColumnPath NULL = new Null();

  @Nonnull
  default ColumnPath andThen(@Nonnull final ColumnPath after) {
    return nullPath().equals(after)
           ? this
           : new Composite(
               Stream.concat(asStream(), after.asStream())
                   .collect(Collectors.toUnmodifiableList()));
  }

  @Nonnull
  default Stream<ColumnPath> asStream() {
    return Stream.of(this);
  }

  @Nonnull
  static ColumnPath nullPath() {
    return NULL;
  }


  default boolean isNull() {
    return NULL.equals(this);
  }


  @Value
  class Null implements ColumnPath {

    @Nonnull
    @Override
    public Stream<ColumnPath> asStream() {
      return Stream.empty();
    }

    @Nonnull
    @Override
    public ColumnPath andThen(@Nonnull final ColumnPath after) {
      return after;
    }

  }

  @Value
  class Composite implements ColumnPath {

    // TODO: add the precondition - a composite should have at least two elements.
    // Or otherwise it should not be a composite but either null or a primitive.
    @Nonnull
    List<ColumnPath> elements;

    @Nonnull
    @Override
    public Stream<ColumnPath> asStream() {
      return elements.stream();
    }
  }

}
