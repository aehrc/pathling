package au.csiro.pathling.fhirpath;

import au.csiro.pathling.fhirpath.collection.Collection;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Value;

/**
 * A description of how to take one {@link Collection} and transform it into another.
 *
 * @author John Grimes
 */
@FunctionalInterface
public interface FhirPath {

  FhirPath NULL = new This();

  Collection apply(@Nonnull final Collection input, @Nonnull final EvaluationContext context);

  default FhirPath first() {
    return this;
  }

  default FhirPath suffix() {
    return nullPath();
  }

  default FhirPath last() {
    return this;
  }

  default FhirPath prefix() {
    return nullPath();
  }

  default FhirPath andThen(@Nonnull final FhirPath after) {
    return nullPath().equals(after)
           ? this
           : new Composite(
               Stream.concat(asStream(), after.asStream())
                   .collect(Collectors.toUnmodifiableList()));
  }

  default Stream<FhirPath> asStream() {
    return Stream.of(this);
  }


  default Stream<FhirPath> children() {
    return Stream.empty();
  }

  static FhirPath nullPath() {
    return NULL;
  }

  default boolean isNull() {
    return NULL.equals(this);
  }


  @Nonnull
  default String toExpression() {
    return toString();
  }


  @Nonnull
  default <T> T accept(@Nonnull final FhirPathVisitor<T> visitor) {
    return visitor.visitPath(this);
  }

  @Value
  class This implements FhirPath {

    @Override
    public Collection apply(@Nonnull final Collection input,
        @Nonnull final EvaluationContext context) {
      return input;
    }

    @Override
    public Stream<FhirPath> asStream() {
      return Stream.empty();
    }

    @Override
    public FhirPath andThen(@Nonnull final FhirPath after) {
      return after;
    }

    @Nonnull
    @Override
    public String toExpression() {
      return "$this";
    }
  }

  @Value
  class Composite implements FhirPath {


    // TODO: add the precondition - a composite should have at least two elements.
    // Or otherwise it should not be a composite but either null or a primitive.
    @Nonnull
    List<FhirPath> elements;

    @Override
    public Collection apply(@Nonnull final Collection input,
        @Nonnull final EvaluationContext context) {
      return elements.stream()
          .reduce(input, (acc, element) -> element.apply(acc, context), (a, b) -> b);
    }

    @Override
    public Stream<FhirPath> asStream() {
      return elements.stream();
    }

    @Override
    public FhirPath first() {
      return elements.get(0);
    }

    @Override
    public FhirPath suffix() {
      return elements.size() > 2
             ? new Composite(elements.subList(1, elements.size()))
             : elements.get(1);
    }

    @Override
    public FhirPath last() {
      return elements.get(elements.size() - 1);
    }

    @Override
    public FhirPath prefix() {
      return elements.size() > 2
             ? new Composite(elements.subList(0, elements.size() - 1))
             : elements.get(0);
    }

    @Nonnull
    @Override
    public String toExpression() {
      return elements.stream()
          .map(FhirPath::toExpression)
          .collect(Collectors.joining("."));
    }


    @Override
    @Nonnull
    public <T> T accept(@Nonnull final FhirPathVisitor<T> visitor) {
      return visitor.visitComposite(this);
    }
  }

}
