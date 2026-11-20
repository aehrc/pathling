import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 * Workarounds for the Generate keeping the whole _x group. Prints the ReadSchema and the bytes read
 * for each shape. Usage: Workarounds dataDir (holding Patient.parquet).
 */
public class Workarounds {
  static final AtomicLong BYTES = new AtomicLong();

  static final String RAW = "named_struct('value', birthDate, 'meta', _birthDate)";
  static final String EXTRACTS =
      "named_struct('value', birthDate, 'meta', named_struct('id', _birthDate.id, 'extension',"
          + " _birthDate.extension, '__start', _birthDate.__start, '__end', _birthDate.__end))";
  static final String EXTRACTS_NO_EXT =
      "named_struct('value', birthDate, 'meta', named_struct('id', _birthDate.id,"
          + " '__start', _birthDate.__start, '__end', _birthDate.__end))";

  /** The output-only model, over the struct s. */
  static String out(final String s) {
    return "transform(filter(array(" + s + "), v -> v IS NOT NULL), e -> e.value)";
  }

  /** The where($this > @1990) model, over the struct s. */
  static String where(final String s) {
    return "transform(filter(array(CASE WHEN element_at(transform(array("
        + s
        + "), e -> coalesce(e.meta.__start > TIMESTAMP'1990-01-01 00:00:00', false)), 1)"
        + " THEN "
        + s
        + " END), v -> v IS NOT NULL), e -> e.value)";
  }

  static String inline(final String expr, final String from) {
    return "select inline(array(struct(" + expr + " AS r))) from " + from;
  }

  static String readSchema(final Dataset<Row> df) {
    final String plan = df.queryExecution().executedPlan().toString();
    final int i = plan.lastIndexOf("ReadSchema: ");
    final String last = plan.substring(i + 12).split("\n")[0].trim();
    return last
        .replace("struct<", "{")
        .replace("array<{url,valueString}>", "ARR")
        .replace(">", "}")
        .replace("__start:timestamp,__end:timestamp", "__start,__end")
        .replace(":string", "")
        .replace("extension", "extension");
  }

  static void run(final SparkSession spark, final String name, final String sql) throws Exception {
    final Dataset<Row> df = spark.sql(sql);
    final String schema = readSchema(df);
    BYTES.set(0);
    df.write().format("noop").mode("overwrite").save();
    long last = -1;
    for (int i = 0; i < 20; i++) {
      Thread.sleep(300);
      final long now = BYTES.get();
      if (now == last && now > 0) {
        break;
      }
      last = now;
    }
    System.out.printf("| %s | `%s` | %.1f |%n", name, schema, BYTES.get() / 1048576.0);
  }

  public static void main(final String[] a) throws Exception {
    final SparkSession spark =
        SparkSession.builder()
            .master("local[2]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.adaptive.enabled", "false")
            .getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");
    spark
        .sparkContext()
        .addSparkListener(
            new SparkListener() {
              @Override
              public void onTaskEnd(final SparkListenerTaskEnd e) {
                if (e.taskMetrics() != null) {
                  BYTES.addAndGet(e.taskMetrics().inputMetrics().bytesRead());
                }
              }
            });
    spark.read().parquet(a[0] + "/Patient.parquet").createOrReplaceTempView("t");

    final Map<String, String> cases = new LinkedHashMap<>();
    cases.put("control: select s.value", "select " + RAW + ".value AS r from t");
    cases.put("base: inline, array(s.value)", inline("array(" + RAW + ".value)", "t"));
    cases.put("base: inline, output-only model", inline(out(RAW), "t"));
    cases.put("base: inline, where model", inline(where(RAW), "t"));
    cases.put(
        "2a: Project below, `select s.value as r`, then inline(array(r))",
        "select inline(array(struct(array(r) AS r))) from (select " + RAW + ".value AS r from t)");
    cases.put(
        "2a: Project below holds s, model above",
        "select inline(array(struct(" + out("s") + " AS r))) from (select " + RAW + " AS s from t)");
    cases.put("2b: extracts, inline, output-only", inline(out(EXTRACTS), "t"));
    cases.put("2b: extracts, inline, where", inline(where(EXTRACTS), "t"));
    cases.put("2b: extracts without extension, inline, output-only", inline(out(EXTRACTS_NO_EXT), "t"));
    cases.put("2b: extracts without extension, inline, where", inline(where(EXTRACTS_NO_EXT), "t"));
    cases.put("2c: explode, array(s.value)", "select explode(array(" + RAW + ".value)) AS r from t");
    cases.put("2c: explode, raw output-only model", "select explode(" + out(RAW) + ") AS r from t");
    cases.put("2c: explode, raw where model", "select explode(" + where(RAW) + ") AS r from t");
    cases.put("2c: posexplode, raw output-only model", "select posexplode(" + out(RAW) + ") from t");
    cases.put("2c: explode, extracts output-only", "select explode(" + out(EXTRACTS) + ") AS r from t");
    cases.put("2c: explode, extracts where", "select explode(" + where(EXTRACTS) + ") AS r from t");
    cases.put(
        "2c: explode, extracts without extension, where",
        "select explode(" + where(EXTRACTS_NO_EXT) + ") AS r from t");
    cases.put(
        "no generator: raw output-only model", "select " + out(RAW) + " AS r from t");
    cases.put("no generator: raw where model", "select " + where(RAW) + " AS r from t");
    for (final Map.Entry<String, String> c : cases.entrySet()) {
      run(spark, c.getKey(), c.getValue());
    }
    spark.stop();
  }
}
