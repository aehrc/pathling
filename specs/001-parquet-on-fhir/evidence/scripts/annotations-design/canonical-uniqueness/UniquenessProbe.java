import au.csiro.pathling.schema.annotation.kinds.CanonicalQuantity;
import au.csiro.pathling.schema.annotation.kinds.QuantityCanonicalKind;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

/**
 * Checks whether equal quantities get byte-equal __canonical_exact annotations, today and with the
 * planned trailing-zero strip on the canonical path. Uses Pathling's own canonicaliser
 * (QuantityCanonicalKind.canonicalOf and toExactRow). Each group lists quantities of one magnitude.
 * Usage: UniquenessProbe <output dir>.
 */
public class UniquenessProbe {

  static final String UCUM = QuantityCanonicalKind.UCUM_SYSTEM;

  static final String[][][] GROUPS = {
    {{"0.195", "g"}, {"0.1950", "g"}, {"0.19500", "g"}},
    {{"37.0", "Cel"}, {"37", "Cel"}, {"37.00", "Cel"}},
    {{"1", "m"}, {"100", "cm"}, {"1000", "mm"}, {"1.0", "m"}},
    {{"1", "kg"}, {"1000", "g"}, {"1000.0", "g"}},
    {{"100", "g"}, {"100.0", "g"}, {"0.1", "kg"}},
    {{"1", "mg"}, {"0.001", "g"}, {"1000", "ug"}},
    {{"37", "Cel"}, {"310.15", "K"}, {"310.150", "K"}},
    {{"0", "Cel"}, {"273.15", "K"}},
    {{"98.6", "[degF]"}, {"37", "Cel"}, {"310.15", "K"}},
    {{"1", "[in_i]"}, {"2.54", "cm"}, {"0.0254", "m"}},
    {{"1", "h"}, {"60", "min"}, {"3600", "s"}},
    {{"1", "L"}, {"1000", "mL"}, {"0.001", "m3"}},
    {{"5.0", "mmol/L"}, {"5", "mmol/L"}, {"0.005", "mol/L"}},
  };

  static Row today(final BigDecimal v, final String code) {
    return QuantityCanonicalKind.toExactRow(QuantityCanonicalKind.canonicalOf(v, UCUM, code));
  }

  /** The planned fix: strip trailing zeros on the canonical value only, then the same encoding. */
  static Row stripped(final BigDecimal v, final String code) {
    final CanonicalQuantity c = QuantityCanonicalKind.canonicalOf(v, UCUM, code);
    return c == null
        ? null
        : QuantityCanonicalKind.toExactRow(
            new CanonicalQuantity(c.getValue().stripTrailingZeros(), c.getCode()));
  }

  static String show(final Row r) {
    if (r == null) {
      return "null";
    }
    final Row v = r.getStruct(0);
    return "(" + v.get(0) + ", " + v.get(1) + ", " + r.get(1) + ")";
  }

  public static void main(final String[] args) {
    final SparkSession spark =
        SparkSession.builder().master("local[1]").config("spark.ui.enabled", "false").getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");
    System.out.println("RESULT spark " + spark.version());
    final List<Row> rows = new ArrayList<>();
    int g = 0;
    for (final String[][] group : GROUPS) {
      g++;
      final Set<String> t = new LinkedHashSet<>();
      final Set<String> s = new LinkedHashSet<>();
      for (final String[] q : group) {
        final BigDecimal v = new BigDecimal(q[0]);
        final Row rt = today(v, q[1]);
        final Row rs = stripped(v, q[1]);
        t.add(show(rt));
        s.add(show(rs));
        System.out.println(
            "RESULT g" + g + " | " + q[0] + " '" + q[1] + "' | today " + show(rt) + " | stripped "
                + show(rs));
        rows.add(RowFactory.create("g" + g, q[0] + " " + q[1], rt, rs));
      }
      System.out.println(
          "RESULT g" + g + " distinct stored forms: today " + t.size() + ", stripped " + s.size()
              + " (of " + group.length + " equal quantities)");
    }
    // The same through Parquet and SQL: how many distinct stored values each group has.
    final StructType schema =
        new StructType()
            .add("grp", DataTypes.StringType)
            .add("q", DataTypes.StringType)
            .add("today", QuantityCanonicalKind.CANONICAL_EXACT_TYPE)
            .add("stripped", QuantityCanonicalKind.CANONICAL_EXACT_TYPE);
    final String dir = args[0] + "/canonical.parquet";
    spark.createDataFrame(rows, schema).coalesce(1).write().mode("overwrite").parquet(dir);
    spark.read().parquet(dir).createOrReplaceTempView("c");
    final Dataset<Row> sql =
        spark.sql(
            "SELECT grp, count(*) AS n, count(DISTINCT today) AS today_distinct, "
                + "count(DISTINCT stripped) AS stripped_distinct FROM c GROUP BY grp ORDER BY grp");
    for (final Row r : sql.collectAsList()) {
      System.out.println(
          "RESULT sql " + r.get(0) + " | rows " + r.get(1) + " | distinct today " + r.get(2)
              + " | distinct stripped " + r.get(3));
    }
    spark.stop();
  }
}
