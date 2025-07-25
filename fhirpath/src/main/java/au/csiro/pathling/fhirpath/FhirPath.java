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

  /**
   * Get the first element of the path.
   *
   * @return the first element of the path
   */
  default FhirPath head() {
    return this;
  }


  /**
   * Get the prefix of the path. In most cases it's same as head() but some FhirPaths (e.g. binary
   * operator) may return a different value. In any case `path.prefix().andThen(path.suffix())
   * should produce the same result as the original `path`.
   *
   * @return the prefix element of the path
   */
  default FhirPath prefix() {
    return head();
  }


  default FhirPath andThen(@Nonnull final FhirPath after) {
    return nullPath().equals(after)
           ? this
           : new Composite(
               Stream.concat(asStream(), after.asStream())
                   .toList());
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

  /**
   * Converts the FHIRPath expression that can be uses a term in a FHIRPath expression.
   *
   * @return the FHIRPath expression
   */
  @Nonnull
  default String toTermExpression() {
    return toExpression();
  }

  @Nonnull
  static FhirPath of(@Nonnull final List<FhirPath> elements) {
    return switch (elements.size()) {
      case 0 -> nullPath();
      case 1 -> elements.get(0);
      default -> new Composite(elements);
    };
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
    public FhirPath head() {
      return elements.get(0);
    }

    @Nonnull
    @Override
    public String toExpression() {
      return elements.stream()
          .map(FhirPath::toTermExpression)
          .collect(Collectors.joining("."));
    }
  }
}
