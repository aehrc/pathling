package spike;

import static org.apache.spark.sql.functions.*;

import java.util.Arrays;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Row-level guard: a filter that raises on the first NOT NULL violation in a row, applied to the
 * still-nullable input. Unlike the per-column check in {@link Conform}, a filter is not removed by
 * column pruning, so it runs even when a later query does not read the checked column (count(),
 * select of other columns), and the optimiser cannot fold it because the input is still nullable.
 */
public final class Guard {

  private Guard() {}

  public static Dataset<Row> apply(Dataset<Row> df, StructType target) {
    final Column msg =
        coalesce(
            Arrays.stream(target.fields())
                .map(f -> violation(df.col(f.name()), f.name(), f.dataType(), f.nullable()))
                .toArray(Column[]::new));
    return df.filter(when(msg.isNotNull(), raise_error(msg)).otherwise(lit(true)));
  }

  /** The message for the first violation at or under c, or null. */
  static Column violation(Column c, String path, DataType t, boolean nullable) {
    final Column own =
        nullable
            ? lit(null).cast("string")
            : when(c.isNull(), lit("NOT NULL field '" + path + "' is null"));
    final Column inner;
    if (t instanceof StructType st) {
      inner =
          coalesce(
              Arrays.stream(st.fields())
                  .map(
                      (StructField f) ->
                          violation(
                              c.getField(f.name()),
                              path + "." + f.name(),
                              f.dataType(),
                              f.nullable()))
                  .toArray(Column[]::new));
    } else if (t instanceof ArrayType at) {
      inner = first(c, x -> violation(x, path + "[]", at.elementType(), at.containsNull()));
    } else if (t instanceof MapType mt) {
      inner =
          first(
              map_values(c),
              x -> violation(x, path + "{}", mt.valueType(), mt.valueContainsNull()));
    } else {
      return own;
    }
    return coalesce(own, when(c.isNotNull(), inner));
  }

  private static Column first(Column arr, scala.Function1<Column, Column> f) {
    return get(filter(transform(arr, f), m -> m.isNotNull()), lit(0));
  }
}
