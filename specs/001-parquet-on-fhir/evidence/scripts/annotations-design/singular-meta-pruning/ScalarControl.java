import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

/**
 * Asks which columns Spark reads when a struct(value, meta) is built and its value is taken, with
 * and without the array and Generate wrapper the view adds. Usage: ScalarControl dataDir.
 */
public class ScalarControl {
  public static void main(final String[] a) {
    final SparkSession spark =
        SparkSession.builder()
            .master("local[2]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.maxMetadataStringLength", "100000")
            .config("spark.sql.adaptive.enabled", "false")
            .getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");
    final Dataset<Row> raw = spark.read().parquet(a[0] + "/Patient.parquet");
    final String s = "named_struct('value', birthDate, 'meta', _birthDate)";
    final String[][] cases = {
      {"plain select s.value", "select " + s + ".value AS r from t"},
      {"array of s.value", "select array(" + s + ".value) AS r from t"},
      {"inline(array(struct(array(s.value))))",
          "select inline(array(struct(array(" + s + ".value) AS r))) from t"},
      {"inline(array(struct(transform(filter(array(s), v -> v is not null), e -> e.value))))",
          "select inline(array(struct(transform(filter(array(" + s
              + "), v -> v IS NOT NULL), e -> e.value) AS r))) from t"},
    };
    raw.createOrReplaceTempView("t");
    for (final String[] c : cases) {
      final Dataset<Row> df = spark.sql(c[1]);
      final String plan = df.queryExecution().executedPlan().toString();
      final int i = plan.lastIndexOf("ReadSchema:");
      System.out.println(c[0] + "\n   " + plan.substring(i).split("\n")[0]);
    }
    spark.stop();
  }
}
