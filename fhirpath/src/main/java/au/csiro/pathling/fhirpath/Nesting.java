package au.csiro.pathling.fhirpath;

import static java.util.stream.Collectors.toList;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class Nesting {

  @Nonnull
  private final Map<NestingKey, NonLiteralPath> nesting;

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
      return result.copy(expression, dataset, result.getIdColumn(), result.getValueColumn(),
          result.getOrderingColumn(), singular, thisColumn);
    } else {
      return result;
    }
  }

  public boolean isOrderable() {
    return nesting.values().stream()
        .allMatch(path -> path.getOrderingColumn().isPresent());
  }

  @Nonnull
  public List<Column> getOrderingColumns() {
    return nesting.values().stream()
        .map(FhirPath::getOrderingColumn)
        .filter(Optional::isPresent)
        .map(Optional::get)
        .collect(toList());
  }

  public void removeLast() {
    @Nullable NestingKey last = null;
    for (final NestingKey key : nesting.keySet()) {
      last = key;
    }
    nesting.remove(last);
  }

}
