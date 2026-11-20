/**
 * Measures the CPU cost per row of reading repeating primitives, for the annotations design. Takes
 * the data directory (holding {@code annotated/} and {@code plain/}, as WriteData writes them), the
 * arm, the variant and, optionally, a comma-separated list of cases.
 *
 * <p>Arms: {@code today} runs the engine through a ViewDefinition column; {@code bare} reads the
 * values only; {@code A} pairs each element as struct(value, meta) at traversal and computes an
 * annotation only in an accessor; {@code B} keeps parallel arrays and pairs them only at a lambda
 * boundary. {@code bare}, {@code A} and {@code B} are hand-built Spark columns.
 *
 * <p>Cases: g1 and d1 are output only (name.given and name.period.start); g2 and d2 are the same
 * after name.where(family.exists()); d3 is name.period.start.where($this > @2000).
 *
 * <p>Each configuration is run once to warm up and five times, timed, with a noop write.
 */
import au.csiro.pathling.encoders.ColumnFunctions;
import au.csiro.pathling.library.PathlingContext;
import au.csiro.pathling.library.io.source.QueryableDataSource;
import au.csiro.pathling.schema.date.FhirDateTime;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.UnaryOperator;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.api.java.UDF1;
import org.apache.spark.sql.expressions.UserDefinedFunction;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.types.DataTypes;

public class RepeatingCpuBench {

  static final AtomicLong START_CALLS = new AtomicLong();

  static final UserDefinedFunction DATE_START =
      functions
          .udf(
              (UDF1<String, Instant>)
                  text -> {
                    if (text == null) {
                      return null;
                    }
                    START_CALLS.incrementAndGet();
                    return FhirDateTime.parse(text).getLowerBoundary();
                  },
              DataTypes.TimestampType)
          .withName("date_start");

  static final Instant AFTER_2000 = FhirDateTime.parse("2000").getUpperBoundary();

  static Column f(final Column array, final UnaryOperator<Column> fn) {
    return functions.filter(array, fn::apply);
  }

  static Column t(final Column array, final UnaryOperator<Column> fn) {
    return functions.transform(array, fn::apply);
  }

  /** The parents, after name.where(family.exists()) where asked. */
  static Column parents(final Column name, final boolean where) {
    return where ? f(name, n -> n.getField("family").isNotNull()) : name;
  }

  /** The metadata group of the start of a name's period, as the engine reads it. */
  static Column startMeta(final Column n) {
    return ColumnFunctions.resolveOrNull(n.getField("period"), "_start", DataTypes.NullType);
  }

  /** A stored start where the schema carries it, and the computed one otherwise. */
  static Column start(final Column element) {
    return ColumnFunctions.resolveOrElse(
        element.getField("meta"), "__start", DATE_START.apply(element.getField("value")));
  }

  static Column bare(final String c, final Column name) {
    final Column p = parents(name, c.endsWith("2"));
    switch (c) {
      case "g1":
      case "g2":
        return t(p, n -> n.getField("given"));
      case "d1":
      case "d2":
        return t(p, n -> n.getField("period").getField("start"));
      case "d3":
        return f(t(p, n -> n.getField("period").getField("start")), v -> v.gt(functions.lit("2000")));
      default:
        throw new IllegalArgumentException(c);
    }
  }

  static Column modelB(final String c, final Column name) {
    final Column p = parents(name, c.endsWith("2"));
    switch (c) {
      case "g1":
      case "g2":
        return functions.flatten(
            t(f(p, n -> n.getField("given").isNotNull()), n -> n.getField("given")));
      case "d1":
      case "d2":
        return f(t(p, n -> n.getField("period").getField("start")), Column::isNotNull);
      case "d3":
        // The arrays are paired only here, where the comparison needs the metadata.
        final Column values = t(p, n -> n.getField("period").getField("start"));
        final Column metas = t(p, RepeatingCpuBench::startMeta);
        final Column zipped = functions.arrays_zip(values.alias("value"), metas.alias("meta"));
        return t(
            f(zipped, e -> e.getField("value").isNotNull().and(start(e).gt(functions.lit(AFTER_2000)))),
            e -> e.getField("value"));
      default:
        throw new IllegalArgumentException(c);
    }
  }

  static Column modelA(final String c, final Column name) {
    final Column p = parents(name, c.endsWith("2"));
    switch (c) {
      case "g1":
      case "g2":
        {
          final Column elements =
              functions.flatten(
                  t(
                      f(p, n -> n.getField("given").isNotNull()),
                      n ->
                          functions.arrays_zip(
                              n.getField("given").alias("value"),
                              functions.coalesce(n.getField("_given"), functions.array()).alias("meta"))));
          return elements.getField("value");
        }
      case "d1":
      case "d2":
        return pairedDates(p).getField("value");
      case "d3":
        return f(
                pairedDates(p),
                e -> start(e).gt(functions.lit(AFTER_2000)))
            .getField("value");
      default:
        throw new IllegalArgumentException(c);
    }
  }

  static Column pairedDates(final Column p) {
    return f(
        t(
            p,
            n ->
                functions
                    .struct(n.getField("period").getField("start").alias("value"), startMeta(n).alias("meta"))),
        e -> e.getField("value").isNotNull());
  }

  static String fhirPath(final String c) {
    switch (c) {
      case "g1":
        return "name.given";
      case "d1":
        return "name.period.start";
      case "g2":
        return "name.where(family.exists()).given";
      case "d2":
        return "name.where(family.exists()).period.start";
      case "d3":
        return "name.period.start.where($this > @2000)";
      default:
        throw new IllegalArgumentException(c);
    }
  }

  static String view(final String path) {
    return "{\"resourceType\":\"ViewDefinition\",\"resource\":\"Patient\",\"status\":\"active\","
        + "\"select\":[{\"column\":[{\"path\":\""
        + path
        + "\",\"name\":\"r\",\"collection\":true}]}]}";
  }

  public static void main(final String[] args) {
    final String dir = args[0];
    final String arm = args[1];
    final String variant = args[2];
    final List<String> cases =
        args.length > 3
            ? Arrays.asList(args[3].split(","))
            : List.of("g1", "d1", "g2", "d2", "d3");
    final SparkSession spark =
        SparkSession.builder()
            .master("local[4]")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.maxMetadataStringLength", "100000")
            .config("spark.sql.adaptive.enabled", "false")
            .getOrCreate();
    spark.sparkContext().setLogLevel("ERROR");
    final PathlingContext ptc = PathlingContext.create(spark);
    final String path = dir + "/" + variant;
    final Dataset<Row> raw = spark.read().parquet(path + "/Patient.parquet");
    final QueryableDataSource data = ptc.read().parquet(path);

    for (final String c : cases) {
      final Dataset<Row> df;
      switch (arm) {
        case "today":
          df = data.view("Patient").json(view(fhirPath(c))).execute();
          break;
        case "bare":
          df = raw.select(bare(c, functions.col("name")).alias("r"));
          break;
        case "A":
          df = raw.select(modelA(c, functions.col("name")).alias("r"));
          break;
        case "B":
          df = raw.select(modelB(c, functions.col("name")).alias("r"));
          break;
        default:
          throw new IllegalArgumentException(arm);
      }
      final String plan = df.queryExecution().optimizedPlan().toString();
      final boolean range = plan.contains("date_range");
      final boolean startUdf = plan.contains("date_start");
      final List<Double> times = new ArrayList<>();
      long calls = 0;
      for (int i = 0; i < 6; i++) {
        START_CALLS.set(0);
        final long t0 = System.nanoTime();
        df.write().format("noop").mode("overwrite").save();
        final double secs = (System.nanoTime() - t0) / 1e9;
        calls = START_CALLS.get();
        if (i > 0) {
          times.add(secs);
        }
      }
      Collections.sort(times);
      System.out.printf(
          "RESULT arm=%s variant=%s case=%s median=%.2f min=%.2f max=%.2f date_range_in_plan=%s"
              + " date_start_in_plan=%s date_start_calls_per_run=%d%n",
          arm, variant, c, times.get(2), times.get(0), times.get(4), range, startUdf, calls);
    }
    spark.stop();
  }
}
