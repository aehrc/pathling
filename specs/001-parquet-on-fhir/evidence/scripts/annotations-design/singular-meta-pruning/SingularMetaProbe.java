import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerTaskEnd;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

/**
 * Asks whether design A (a FHIR primitive is struct(value, meta), meta being the stored _x group
 * with its extensions) makes Spark read a singular primitive's _x where today's engine reads only
 * the annotation fields. Usage: SingularMetaProbe dataDir (a directory holding Patient.parquet).
 *
 * <p>Three arms per case. "engine" runs the FHIRPath through the library API. "A-raw" and
 * "A-system" are hand-built SQL models of the same expression, mirroring the shape of the
 * engine's plan: array(CASE WHEN x IS NOT NULL THEN element END) filtered for nulls, union as
 * concat plus a dedupe aggregate, where as filter over array(CASE WHEN cond THEN element END).
 * In A-raw the element is struct(value=x, meta=_x). In A-system it is built from that struct and
 * converted to struct(value, ann=struct(__start, __end)) before the operation.
 */
public class SingularMetaProbe {

  static final AtomicLong BYTES = new AtomicLong();

  // ---- Model: the primitive element and its accessors, per arm. ----

  /** The arm decides the element shape and where the range fields live. */
  static String fhir(final String x) {
    return "named_struct('value', " + x + ", 'meta', _" + x + ")";
  }

  static String elem(final String arm, final String x) {
    final String f = fhir(x);
    if (arm.equals("A-raw")) {
      return "CASE WHEN " + x + " IS NOT NULL THEN " + f + " END";
    }
    return "CASE WHEN "
        + x
        + " IS NOT NULL THEN named_struct('value', "
        + f
        + ".value, 'ann', named_struct('__start', "
        + f
        + ".meta.__start, '__end', "
        + f
        + ".meta.__end)) END";
  }

  static String start(final String arm, final String e) {
    return arm.equals("A-raw") ? e + ".meta.__start" : e + ".ann.__start";
  }

  static String end(final String arm, final String e) {
    return arm.equals("A-raw") ? e + ".meta.__end" : e + ".ann.__end";
  }

  static String single(final String arm, final String x) {
    return "filter(array(" + elem(arm, x) + "), v -> v IS NOT NULL)";
  }

  static String dedupe(final String arm, final String arr) {
    final String eq =
        "coalesce("
            + start(arm, "a")
            + " = "
            + start(arm, "y")
            + " AND "
            + end(arm, "a")
            + " = "
            + end(arm, "y")
            + ", false)";
    return "aggregate("
        + arr
        + ", filter("
        + arr
        + ", v -> false), (acc, y) -> CASE WHEN exists(acc, a -> "
        + eq
        + ") THEN acc ELSE concat(acc, array(y)) END)";
  }

  static String cmp(final String arm, final String e) {
    return "coalesce(" + start(arm, e) + " > TIMESTAMP'1990-01-01 00:00:00', false)";
  }

  static String values(final String arr) {
    return "transform(" + arr + ", e -> e.value)";
  }

  static String whereArr(final String arm, final String x, final String cond) {
    return "filter(array(CASE WHEN " + cond + " THEN " + elem(arm, x) + " END), v -> v IS NOT NULL)";
  }

  /** The model expression for each case, in the order of CASE_PATHS. */
  static String model(final String arm, final int c) {
    final String b = "birthDate";
    final String d = "deceasedDateTime";
    final String both = "concat(" + single(arm, b) + ", " + single(arm, d) + ")";
    switch (c) {
      case 0:
        return values(single(arm, b));
      case 1:
        return values(dedupe(arm, both));
      case 2:
        return values(both);
      case 3:
        return values("filter(array(element_at(" + dedupe(arm, both) + ", 1)), v -> v IS NOT NULL)");
      case 4:
        return values(
            dedupe(
                arm,
                "concat("
                    + whereArr(arm, b, "active")
                    + ", "
                    + whereArr(arm, d, "NOT active")
                    + ")"));
      case 5:
        return values(
            "filter(array(CASE WHEN "
                + b
                + " IS NOT NULL AND element_at(transform(array("
                + elem(arm, b)
                + "), e -> "
                + cmp(arm, "e")
                + "), 1) THEN "
                + elem(arm, b)
                + " END), v -> v IS NOT NULL)");
      case 6:
        return "array(size(filter("
            + dedupe(arm, both)
            + ", e -> "
            + cmp(arm, "e")
            + ")) > 0)";
      case 7:
        return values(
            "filter(array(CASE WHEN element_at(transform(array("
                + elem(arm, b)
                + "), e -> "
                + cmp(arm, "e")
                + "), 1) <=> true THEN "
                + elem(arm, b)
                + " END), v -> v IS NOT NULL)");
      default:
        throw new IllegalArgumentException();
    }
  }

  static final String[] CASE_PATHS = {
    "birthDate",
    "birthDate | deceasedDateTime",
    "birthDate.combine(deceasedDateTime)",
    "(birthDate | deceasedDateTime).first()",
    "where(active).birthDate | where(active.not()).deceasedDateTime",
    "birthDate.where($this > @1990)",
    "(birthDate | deceasedDateTime).where($this > @1990).exists()",
    "where(birthDate > @1990).birthDate"
  };

  // ---- Measurement. ----

  static String readSchema(final Dataset<Row> df) {
    final String plan = df.queryExecution().executedPlan().toString();
    final Matcher m =
        Pattern.compile("ReadSchema: (struct<.*?>)\\s*$", Pattern.MULTILINE).matcher(plan);
    String last = "<none>";
    while (m.find()) {
      last = m.group(1);
    }
    return last;
  }

  static String abbreviate(final String s) {
    return s.replace("struct<", "{")
        .replace(">", "}")
        .replace("__start:timestamp,__end:timestamp", "__start,__end")
        .replace(":string", "")
        .replace("extension:array{url,valueString}", "extension")
        .replace("id,extension,__start,__end", "WHOLE");
  }

  /** Runs the plan to a no-op sink and returns the bytes the scan read, from the task metrics. */
  static long bytesRead(final SparkSession spark, final Dataset<Row> df) throws Exception {
    BYTES.set(0);
    df.write().format("noop").mode("overwrite").save();
    // Listener events are delivered asynchronously, so wait for the count to settle.
    long last = -1;
    for (int i = 0; i < 20; i++) {
      Thread.sleep(300);
      final long now = BYTES.get();
      if (now == last && now > 0) {
        break;
      }
      last = now;
    }
    return BYTES.get();
  }

  static void report(
      final SparkSession spark,
      final String label,
      final String arm,
      final Dataset<Row> df,
      final List<String> out)
      throws Exception {
    final String schema = readSchema(df);
    final long bytes = bytesRead(spark, df);
    final Row check = df.agg(functions.sum(functions.hash(functions.col("r")))).first();
    final String line =
        String.format(
            "| `%s` | %s | `%s` | %.1f | %s |",
            label, arm, abbreviate(schema), bytes / 1048576.0, check.get(0));
    System.out.println(line);
    out.add(line);
  }

  /** Wraps a result expression the way a view does: a Generate inline over a one-row struct. */
  static Dataset<Row> wrap(final Dataset<Row> src, final String expr) {
    return src.select(functions.inline(functions.expr("array(struct(" + expr + " AS r))")));
  }

  public static void main(final String[] args) throws Exception {
    final SparkSession spark =
        SparkSession.builder()
            .master("local[2]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.maxMetadataStringLength", "100000")
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
    final PathlingContext ptc = PathlingContext.create(spark);
    final QueryableDataSource data = ptc.read().parquet(args[0]);
    final Dataset<Row> raw = spark.read().parquet(args[0] + "/Patient.parquet");
    System.out.println("rows=" + raw.count());
    final List<String> out = new ArrayList<>();

    // Baselines: what each stored column costs to read.
    final String[][] baselines = {
      {"baseline: birthDate", "birthDate"},
      {"baseline: _birthDate.__start,__end", "_birthDate.__start, _birthDate.__end"},
      {"baseline: whole _birthDate", "_birthDate"},
      {"baseline: birthDate, deceasedDateTime and both ranges",
          "birthDate, deceasedDateTime, _birthDate.__start, _birthDate.__end,"
              + " _deceasedDateTime.__start, _deceasedDateTime.__end"},
      {"baseline: both whole _x groups and values",
          "birthDate, _birthDate, deceasedDateTime, _deceasedDateTime"},
    };
    for (final String[] b : baselines) {
      report(spark, b[0], "scan", raw.selectExpr(b[1].split(",\\s*")).selectExpr("hash(*) AS r"), out);
    }

    // The scalar control for A-raw: the struct is built and read outside any lambda.
    report(
        spark,
        "scalar struct(x,_x).value",
        "A-raw",
        wrap(raw, "array(" + fhir("birthDate") + ".value)"),
        out);

    for (int c = 0; c < CASE_PATHS.length; c++) {
      final String path = CASE_PATHS[c];
      try {
        final String view =
            "{\"resourceType\":\"ViewDefinition\",\"resource\":\"Patient\",\"status\":\"active\","
                + "\"select\":[{\"column\":[{\"path\":\""
                + path
                + "\",\"name\":\"r\",\"collection\":true}]}]}";
        report(spark, path, "engine", data.view("Patient").json(view).execute(), out);
      } catch (final Exception e) {
        System.out.println("| `" + path + "` | engine | FAILED: " + e.getMessage() + " | | |");
      }
      for (final String arm : new String[] {"A-raw", "A-system"}) {
        report(spark, path, arm, wrap(raw, model(arm, c)), out);
      }
    }
    spark.stop();
  }
}
