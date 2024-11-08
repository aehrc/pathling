package au.csiro.pathling.fhirpath.collection.rendering;

import java.util.Map;
import java.util.Optional;
import java.util.function.UnaryOperator;
import org.apache.spark.sql.Column;
import org.jetbrains.annotations.NotNull;

/**
 * This is used for rendering a collection of complex types that is represented in the dataset as a
 * set of columns, rather than a struct. The most prominent example of this is a FHIR resource that
 * is represented in a dataset using a column for each top-level element.
 * <p>
 * This rendering will always have an empty column, but you can use traverse to access specific
 * fields. It needs to be initialised with a map of column names to columns.
 *
 * @param columns A map of column names to columns
 */
public record RootRendering(@NotNull Map<String, Column> columns) implements Rendering {

  @Override
  public @NotNull Optional<Column> getColumn() {
    return Optional.empty();
  }

  @Override
  public @NotNull Optional<Rendering> traverse(@NotNull final String name) {
    return Optional.ofNullable(columns.get(name))
        .map(SingleColumnRendering::new);
  }

  @Override
  public @NotNull Rendering singleton() {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override
  public @NotNull Rendering collection() {
    throw new UnsupportedOperationException("Operation not supported");
  }

  @Override
  public @NotNull Rendering map(@NotNull final UnaryOperator<Column> mapper) {
    throw new UnsupportedOperationException("Operation not supported");
  }

}
