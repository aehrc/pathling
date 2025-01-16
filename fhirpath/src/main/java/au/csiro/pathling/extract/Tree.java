package au.csiro.pathling.extract;

import jakarta.annotation.Nonnull;
import java.util.Collections;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.Value;

public interface Tree<T> {

  @Nonnull
  T getValue();

  @Nonnull
  List<Tree<T>> getChildren();

  @Nonnull
  default Stream<Tree<T>> streamChildren() {
    return getChildren().stream();
  }

  @Nonnull
  String toTreeString(final int level,
      @Nonnull final Function<T, String> formatter);

  @Nonnull
  default String toTreeString() {
    return toTreeString(0, Object::toString);
  }

  @Nonnull
  default String toTreeString(@Nonnull final Function<T, String> formatter) {
    return toTreeString(0, formatter);
  }

  @Nonnull
  default <R> Tree<R> map(@Nonnull final Function<T, R> mapper) {
    return Tree.node(mapper.apply(getValue()),
        streamChildren().map(child -> child.map(mapper)).collect(Collectors.toList()));
  }

  /**
   * Map the value of this node only.
   * @param mapper The mapping function.
   * @return A new tree with the value of this node mapped.
   */
  @Nonnull
  default Tree<T> mapValue(@Nonnull final Function<T, T> mapper) {
    return Tree.node(mapper.apply(getValue()),
        streamChildren().toList());
  }


  @Value(staticConstructor = "of")
  class Node<T> implements Tree<T> {

    T value;
    List<Tree<T>> children;

    @Override
    @Nonnull
    public String toTreeString(final int level, @Nonnull final Function<T, String> formatter) {
      return "  ".repeat(level) + formatter.apply(value) + "\n" +
          streamChildren()
              .map(child -> child.toTreeString(level + 1, formatter))
              .collect(Collectors.joining("\n"));
    }
  }

  @Value(staticConstructor = "of")
  class Leaf<T> implements Tree<T> {

    T value;

    @Override
    @Nonnull
    public List<Tree<T>> getChildren() {
      return Collections.emptyList();
    }

    @Override
    @Nonnull
    public String toTreeString(final int level, @Nonnull final Function<T, String> formatter) {
      return "  ".repeat(level) + formatter.apply(value);
    }
  }

  @Nonnull
  static <T> Tree<T> node(@Nonnull final T value, @Nonnull final List<Tree<T>> children) {
    return children.isEmpty()
           ? Leaf.of(value)
           : Node.of(value, children);
  }

  @SafeVarargs
  @Nonnull
  static <T> Tree<T> node(@Nonnull final T value, @Nonnull final Tree<T>... children) {
    return node(value, List.of(children));
  }
}
