package au.csiro.pathling.fhirpath.collection.rendering;

import java.util.Optional;
import java.util.function.UnaryOperator;
import org.apache.spark.sql.Column;
import org.jetbrains.annotations.NotNull;

public interface Rendering {

  /**
   * Get the column representing the rendering. Not all collections can be represented by a single
   * column, so this method may return empty.
   *
   * @return the column representing the rendering, if it exists
   */
  @NotNull Optional<Column> getColumn();

  /**
   * Get a field from the rendering. Not all renderings can access fields, so this method may return
   * empty.
   *
   * @param name the name of the field to get
   * @return a new rendering representing the field, if it exists
   */
  @NotNull Optional<Rendering> traverse(@NotNull String name);

  /**
   * Assert that the rendering is a singleton, and return it.
   *
   * @return the singleton rendering
   */
  @NotNull Rendering singleton();

  /**
   * Assert that the rendering is a collection, and return it.
   *
   * @return the collection rendering
   */
  @NotNull Rendering collection();

  /**
   * Map to a new rendering using the provided mapper.
   *
   * @param mapper the mapper to use
   * @return the new rendering
   */
  @NotNull Rendering map(@NotNull UnaryOperator<Column> mapper);

}
