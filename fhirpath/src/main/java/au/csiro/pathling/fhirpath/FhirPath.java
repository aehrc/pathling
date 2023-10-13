package au.csiro.pathling.fhirpath;

import au.csiro.pathling.fhirpath.collection.Collection;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import javax.annotation.Nonnull;

import lombok.Value;

/**
 * A description of how to take one {@link Collection} and transform it into another.
 *
 * @param <I> The input type of {@link Collection}
 * @author John Grimes
 */
@FunctionalInterface
public interface FhirPath<I extends Collection> {

  FhirPath<?> NULL = new This<>();

  I apply(@Nonnull final I input, @Nonnull final EvaluationContext context);

  default FhirPath<I> first() {
    return this;
  }

  default FhirPath<I> suffix() {
    return nullPath();
  }

  default FhirPath<I> last() {
    return this;
  }

  default FhirPath<I> prefix() {
    return nullPath();
  }

  default FhirPath<I> andThen(@Nonnull final FhirPath<I> after) {
    return nullPath().equals(after)
           ? this
           : new Composite<>(
               Stream.concat(asStream(), after.asStream())
                   .collect(Collectors.toUnmodifiableList()));
  }

  default Stream<FhirPath<I>> asStream() {
    return Stream.of(this);
  }

  static <I extends Collection> FhirPath<I> nullPath() {
    //noinspection unchecked
    return (FhirPath<I>) NULL;
  }

  default boolean isNull() {
    return NULL.equals(this);
  }


  @Nonnull
  default String toExpression() {
    return toString();
  }

  @Value
  class This<I extends Collection> implements FhirPath<I> {

    @Override
    public I apply(@Nonnull final I input,
        @Nonnull final EvaluationContext context) {
      return input;
    }

    @Override
    public Stream<FhirPath<I>> asStream() {
      return Stream.empty();
    }

    @Override
    public FhirPath<I> andThen(@Nonnull final FhirPath<I> after) {
      return after;
    }

    @Nonnull
    @Override
    public String toExpression() {
      return "$this";
    }
  }

  @Value
  class Composite<I extends Collection> implements FhirPath<I> {


    // TODO: add the precondition - a composite should have at least two elements.
    // Or otherwise it should not be a composite but either null or a primitive.
    @Nonnull
    List<FhirPath<I>> elements;

    @Override
    public I apply(@Nonnull final I input, @Nonnull final EvaluationContext context) {
      return elements.stream()
          .reduce(input, (acc, element) -> element.apply(acc, context), (a, b) -> b);
    }

    @Override
    public Stream<FhirPath<I>> asStream() {
      return elements.stream();
    }

    @Override
    public FhirPath<I> first() {
      return elements.get(0);
    }

    @Override
    public FhirPath<I> suffix() {
      return elements.size() > 2
             ? new Composite<>(elements.subList(1, elements.size()))
             : elements.get(1);
    }

    @Override
    public FhirPath<I> last() {
      return elements.get(elements.size() - 1);
    }

    @Override
    public FhirPath<I> prefix() {
      return elements.size() > 2
             ? new Composite<>(elements.subList(0, elements.size() - 1))
             : elements.get(0);
    }

    @Nonnull
    @Override
    public String toExpression() {
      return elements.stream()
          .map(FhirPath::toExpression)
          .collect(Collectors.joining("."));
    }
  }

}
