package au.csiro.pathling.fhirpath;

import static java.util.stream.Collectors.toList;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.Nonnull;
import lombok.Getter;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

/**
 * Stores information about traversal of nested elements and resources within a FHIRPath expression.
 * This allows us to reuse exploded and joined values, and to avoid unnecessary joins. It is also
 * used to retrieve the necessary columns required to order a path when applying operations that are
 * sensitive to element ordering, such as {@code first()}.
 *
 * @author John Grimes
 */
public class Nesting {

  @Nonnull
  private final Map<NestingKey, NonLiteralPath> nesting;

  /**
   * Indicates whether the root level of the dataset has been rolled up due to aggregation. This is
   * a trigger to reconstitute the root level when traversing back into the root.
   */
  @Getter
  private boolean rootErased = false;

  public Nesting() {
    this.nesting = new LinkedHashMap<>();
  }

  @Nonnull
  public NonLiteralPath updateOrRetrieve(@Nonnull final NestingKey key,
      @Nonnull final String expression, @Nonnull final Dataset<Row> dataset,
      final boolean singular, @Nonnull final Optional<Column> thisColumn,
      @Nonnull final Function<? super NestingKey, NonLiteralPath> updater) {
    final boolean hit = nesting.containsKey(key);
    final NonLiteralPath result = nesting.computeIfAbsent(key, updater);
    if (hit) {
      // If the path has been retrieved from the cache, we need to copy it and substitute the 
      // supplied values for expression, dataset, singular and this column.
      return result.copy(expression, dataset, result.getIdColumn(), result.getValueColumn(),
          result.getOrderingColumn(), singular, thisColumn);
    } else {
      // If the path has been computed fresh, we don't need to update anything.
      return result;
    }
  }

  /**
   * @return whether the traversed nesting levels are all orderable, which tells us whether ordering
   * can be determined in this context
   */
  public boolean isOrderable() {
    return nesting.values().stream()
        .allMatch(path -> path.getOrderingColumn().isPresent());
  }

  /**
   * @return all value columns within nesting paths that have been traversed
   */
  @Nonnull
  public List<Column> getColumns() {
    return nesting.values().stream()
        .map(FhirPath::getValueColumn)
        .collect(toList());
  }

  /**
   * @return all ordering columns within nesting paths that have been traversed
   */
  @Nonnull
  public List<Column> getOrderingColumns() {
    return nesting.values().stream()
        .map(FhirPath::getOrderingColumn)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(toList());
  }

  /**
   * @return whether the nesting stack is empty
   */
  public boolean isEmpty() {
    return nesting.isEmpty();
  }

  /**
   * Clears the nesting stack.
   */
  public void clear() {
    nesting.clear();
    rootErased = true;
  }

}
