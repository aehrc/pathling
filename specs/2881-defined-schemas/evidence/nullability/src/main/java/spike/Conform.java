package spike;

import static org.apache.spark.sql.functions.*;

import java.math.BigDecimal;
import java.util.Arrays;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.ArrayType;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.DecimalType;
import org.apache.spark.sql.types.MapType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

/**
 * Public-API-only conformance of a dataset's nullability to a target schema, at every depth.
 *
 * <p>NOT NULL target: {@code coalesce(when(c.isNull, raise_error(msg)).otherwise(v), sentinel)}.
 * The raise fires first for a null, so the sentinel is never returned; it exists only so that
 * Catalyst infers {@code nullable = false} for the coalesce (it has a non-nullable child).
 *
 * <p>Nullable target: {@code when(c.isNotNull, v)} with no otherwise, which Catalyst always infers
 * as nullable.
 *
 * <p>Structs are rebuilt with {@code struct(...)} (non-nullable), arrays with {@code transform}
 * (containsNull = lambda's nullability), maps with {@code transform_values} (valueContainsNull =
 * lambda's nullability). Checks on a nested field are inside the parent's {@code when(isNotNull)}
 * so they run only where the parent is present.
 */
public final class Conform {

  private Conform() {}

  public static Dataset<Row> apply(Dataset<Row> df, StructType target) {
    final Column[] cols =
        Arrays.stream(target.fields())
            .map(f -> conform(df.col(f.name()), f.name(), f.dataType(), f.nullable()).as(f.name()))
            .toArray(Column[]::new);
    return df.select(cols);
  }

  static Column conform(Column c, String path, DataType t, boolean nullable) {
    final Column v = rebuild(c, path, t);
    if (nullable) {
      return when(c.isNotNull(), v);
    }
    return coalesce(
        when(c.isNull(), raise_error(lit("NOT NULL field '" + path + "' is null"))).otherwise(v),
        sentinel(t));
  }

  static Column rebuild(Column c, String path, DataType t) {
    if (t instanceof StructType st) {
      return struct(
          Arrays.stream(st.fields())
              .map(
                  (StructField f) ->
                      conform(
                              c.getField(f.name()),
                              path + "." + f.name(),
                              f.dataType(),
                              f.nullable())
                          .as(f.name()))
              .toArray(Column[]::new));
    }
    if (t instanceof ArrayType at) {
      return transform(c, x -> conform(x, path + "[]", at.elementType(), at.containsNull()));
    }
    if (t instanceof MapType mt) {
      return transform_values(
          c, (k, x) -> conform(x, path + "{}", mt.valueType(), mt.valueContainsNull()));
    }
    return c;
  }

  /** A non-null value of the type; never returned, only there to make the coalesce non-null. */
  static Column sentinel(DataType t) {
    if (t instanceof StructType st) {
      return struct(
          Arrays.stream(st.fields())
              .map(f -> sentinelOrNull(f).as(f.name()))
              .toArray(Column[]::new));
    }
    if (t instanceof ArrayType) return array().cast(t);
    if (t instanceof MapType) return map().cast(t);
    if (t.equals(DataTypes.StringType)) return lit("");
    if (t.equals(DataTypes.IntegerType)) return lit(0);
    if (t.equals(DataTypes.LongType)) return lit(0L);
    if (t.equals(DataTypes.BooleanType)) return lit(false);
    if (t.equals(DataTypes.DoubleType)) return lit(0.0d);
    if (t.equals(DataTypes.BinaryType)) return lit(new byte[0]);
    if (t.equals(DataTypes.DateType)) return lit(java.sql.Date.valueOf("1970-01-01"));
    if (t.equals(DataTypes.TimestampType)) return lit(new java.sql.Timestamp(0));
    if (t instanceof DecimalType) return lit(BigDecimal.ZERO).cast(t);
    throw new IllegalArgumentException("no sentinel for " + t);
  }

  // Sentinel struct fields keep the target nullability, so the sentinel's type merges cleanly.
  static Column sentinelOrNull(StructField f) {
    return f.nullable() ? lit(null).cast(f.dataType()) : sentinel(f.dataType());
  }

  public static Column top(String name, DataType t) {
    return conform(col(name), name, t, false).as(name);
  }
}
