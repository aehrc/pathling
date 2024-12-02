package au.csiro.pathling.fhirpath;

import au.csiro.pathling.fhirpath.collection.Collection;
import jakarta.annotation.Nonnull;
import java.util.List;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Value;
import org.apache.commons.collections4.IterableUtils;
import org.apache.commons.lang3.tuple.Pair;

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
  default FhirPath withNewChildren(@Nonnull final List<FhirPath> children) {
    if (children.isEmpty()) {
      return this;
    } else {
      throw new IllegalArgumentException("Non-empty child list passed to zero arg FhirPath");
    }
  }
  

  @Nonnull
  default String toExpression() {
    return toString();
  }

  @Nonnull
  default <T> T accept(@Nonnull final FhirPathVisitor<T> visitor) {
    return visitor.visitPath(this);
  }

  @Nonnull
  static FhirPath of(@Nonnull final List<FhirPath> elements) {
    return switch (elements.size()) {
      case 0 -> nullPath();
      case 1 -> elements.get(0);
      default -> new Composite(elements);
    };
  }

  /**
   * Split the path into two parts, the left part is the longest prefix that does not satisfy the
   * predicate, and the right part is the rest of the path where the first element satisfies the
   * predicate or  {@link #nullPath()} if the path is empty.
   *
   * @param predicate the predicate to split the path
   * @return a pair of the left and right parts of the path
   */
  @Nonnull
  default Pair<FhirPath, FhirPath> splitRight(@Nonnull final Predicate<FhirPath> predicate) {
    return predicate.test(this)
           ? Pair.of(nullPath(), this)
           : Pair.of(this, nullPath());
  }


  /**
   * Split the path into two parts, the left part is the longest prefix that does not satisfy the
   * predicate and the first element that foes, and the right part is the rest of the path
   * {@link #nullPath()} if the path is empty.
   *
   * @param predicate the predicate to split the path
   * @return a pair of the left and right parts of the path
   */
  @Nonnull
  default Pair<FhirPath, FhirPath> splitLeft(@Nonnull final Predicate<FhirPath> predicate) {
    return predicate.test(this)
           ? Pair.of(this, nullPath())
           : Pair.of(nullPath(), this);
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

    @Override
    @Nonnull
    public Pair<FhirPath, FhirPath> splitRight(@Nonnull final Predicate<FhirPath> predicate) {
      // find the first element that satisfies the predicate
      final int splitIndex = IterableUtils.indexOf(elements, predicate::test);
      if (splitIndex == -1) {
        return Pair.of(this, nullPath());
      } else if (splitIndex == 0) {
        return Pair.of(nullPath(), this);
      } else {
        return Pair.of(FhirPath.of(elements.subList(0, splitIndex)),
            FhirPath.of(elements.subList(splitIndex, elements.size())));
      }
    }


    @Override
    @Nonnull
    public Pair<FhirPath, FhirPath> splitLeft(@Nonnull final Predicate<FhirPath> predicate) {
      // find the first element that satisfies the predicate
      final int splitIndex = IterableUtils.indexOf(elements, predicate::test);
      if (splitIndex == -1) {
        return Pair.of(nullPath(), this);
      } else if (splitIndex == 0) {
        return Pair.of(first(), suffix());
      } else {
        return Pair.of(FhirPath.of(elements.subList(0, splitIndex + 1)),
            FhirPath.of(elements.subList(splitIndex + 1, elements.size())));
      }
    }
  }
}
