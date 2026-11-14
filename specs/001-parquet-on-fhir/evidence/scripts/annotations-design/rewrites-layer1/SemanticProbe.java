import au.csiro.pathling.library.PathlingContext;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Compares the engine's value of name.exists(c) and name.all(c) per patient with candidate SQL
 * forms of them, over the edge data set (null name, null elements, an element with only an
 * extension, criteria that evaluate to null). Usage: SemanticProbe edgeDir. Prints one SEM line
 * per criteria, function and candidate: the number of patients whose value differs from the
 * engine's, with up to three examples.
 */
public class SemanticProbe {

  static String viewFor(final String path) {
    return "{\"resourceType\":\"ViewDefinition\",\"resource\":\"Patient\",\"status\":\"active\","
        + "\"select\":[{\"column\":[{\"path\":\"id\",\"name\":\"id\"},"
        + "{\"path\":\"" + path + "\",\"name\":\"v\"}]}]}";
  }

  static Map<String, String> engine(final PathlingContext ptc, final String dir, final String path) {
    final Dataset<Row> df = ptc.read().parquet(dir).view("Patient").json(viewFor(path)).execute();
    final Map<String, String> m = new HashMap<>();
    for (final Row r : df.collectAsList()) {
      m.put(r.getString(0), String.valueOf(r.get(1)));
    }
    return m;
  }

  static void compare(final SparkSession spark, final Map<String, String> eng, final String fn,
      final String crit, final String tag, final String sql) {
    final List<Row> rows = spark.sql("select id, " + sql + " as v from t").collectAsList();
    int diffs = 0;
    final StringBuilder ex = new StringBuilder();
    for (final Row r : rows) {
      final String got = String.valueOf(r.get(1));
      final String want = eng.get(r.getString(0));
      if (!got.equals(want)) {
        diffs++;
        if (diffs <= 3) {
          ex.append(" [").append(r.getString(0)).append(": engine=").append(want).append(" sql=")
              .append(got).append("]");
        }
      }
    }
    System.out.println("SEM\t" + fn + "\t" + crit + "\t" + tag + "\tdiffering=" + diffs + "/"
        + rows.size() + ex);
  }

  public static void main(final String[] args) {
    final SparkSession spark = SparkSession.builder().master("local[2]")
        .config("spark.ui.enabled", "false").config("spark.sql.adaptive.enabled", "false")
        .getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");
    final PathlingContext ptc = PathlingContext.create(spark);
    spark.read().parquet(args[0] + "/Patient.parquet").createOrReplaceTempView("t");
    final String[][] crits = {
      {"use = 'nickname'", "x.use = 'nickname'"},
      {"use != 'nickname'", "x.use != 'nickname'"},
      {"use.empty()", "x.use is null"},
      {"family.exists()", "x.family is not null"},
    };
    for (final String[] c : crits) {
      final Map<String, String> ex = engine(ptc, args[0], "name.exists(" + c[0] + ")");
      final String b = c[1];
      compare(spark, ex, "exists", c[0], "pure: coalesce(array_max(transform(name, x -> c)), false)",
          "coalesce(array_max(transform(name, x -> " + b + ")), false)");
      compare(spark, ex, "exists", c[0], "native: exists(name, x -> c)", "exists(name, x -> " + b + ")");
      compare(spark, ex, "exists", c[0], "native, coalesced: coalesce(exists(name, x -> c), false)",
          "coalesce(exists(name, x -> " + b + "), false)");
      compare(spark, ex, "exists", c[0],
          "final: coalesce(array_max(transform(filter(name, x -> x is not null), x -> coalesce(c, false))), false)",
          "coalesce(array_max(transform(filter(name, x -> x is not null), x -> coalesce(" + b
              + ", false))), false)");
      final Map<String, String> al = engine(ptc, args[0], "name.all(" + c[0] + ")");
      compare(spark, al, "all", c[0], "pure: coalesce(array_min(transform(name, x -> c)), true)",
          "coalesce(array_min(transform(name, x -> " + b + ")), true)");
      compare(spark, al, "all", c[0], "native: forall(name, x -> c)", "forall(name, x -> " + b + ")");
      compare(spark, al, "all", c[0], "native, coalesced: coalesce(forall(name, x -> c), true)",
          "coalesce(forall(name, x -> " + b + "), true)");
      compare(spark, al, "all", c[0],
          "final: coalesce(array_min(transform(filter(name, x -> x is not null), x -> coalesce(c, false))), true)",
          "coalesce(array_min(transform(filter(name, x -> x is not null), x -> coalesce(" + b
              + ", false))), true)");
    }
    spark.stop();
  }
}
